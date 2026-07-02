import glob
import argparse
from datetime import timedelta
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def parse_args():
    parser = argparse.ArgumentParser(
        description="Cold storage CSV 데이터를 날짜 범위로 읽어 대시보드 이미지를 생성합니다."
    )
    parser.add_argument("start_date", nargs="?", help="조회 시작일 (YYYY-MM-DD)")
    parser.add_argument("end_date", nargs="?", help="조회 종료일 (YYYY-MM-DD)")
    return parser.parse_args()


def parse_date_range(start_date, end_date):
    if not start_date and not end_date:
        return None, None

    if not start_date or not end_date:
        raise ValueError("시작일과 종료일을 모두 입력해주세요. 예: python dashboard.py 2026-06-30 2026-07-01")

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    if start > end:
        raise ValueError("시작일은 종료일보다 늦을 수 없습니다.")

    return start, end


def find_csv_files(s3_path, start, end):
    """가상 S3 디렉토리에서 조회 범위에 해당하는 CSV 파일 목록을 찾습니다."""
    if start is None or end is None:
        return glob.glob(str(s3_path / "raw-data" / "liveklass" / "*" / "*" / "*.csv"))

    csv_files = []
    current = start.date()
    end_date = end.date()

    while current <= end_date:
        csv_files.extend(
            glob.glob(str(s3_path / "raw-data" / "liveklass" / "*" / current.isoformat() / "*.csv"))
        )
        current += timedelta(days=1)

    return csv_files


args = parse_args()

try:
    start, end = parse_date_range(args.start_date, args.end_date)
except ValueError as exc:
    print(f"[ERROR] {exc}")
    exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
S3_PATH = PROJECT_ROOT / "s3_data_lake"
csv_files = find_csv_files(S3_PATH, start, end)

if not csv_files:
    print("[ERROR] 시각화할 cold storage 데이터(CSV)가 존재하지 않습니다.")
    exit()

df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
for column in [
    "lecture_id",
    "referrer",
    "buffering_duration",
    "payment_status",
    "task_status",
]:
    if column not in df.columns:
        df[column] = pd.NA

df["creator_id"] = pd.to_numeric(df["creator_id"], errors="coerce")
df = df.dropna(subset=["creator_id"])
df["creator_id"] = df["creator_id"].astype(int)
df["lecture_id"] = pd.to_numeric(df["lecture_id"], errors="coerce")
df["buffering_duration"] = pd.to_numeric(df["buffering_duration"], errors="coerce")

if start is not None and end is not None:
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True).dt.tz_localize(None)
    end_exclusive = end + timedelta(days=1)
    df = df[(df["created_at"] >= start) & (df["created_at"] < end_exclusive)]

if df.empty:
    print("[ERROR] 입력한 날짜 범위에 해당하는 데이터가 없습니다.")
    exit()

sns.set_theme(style="darkgrid")
fig, axes = plt.subplots(2, 2, figsize=(18, 12))

cart_logs = df[df['event_type'] == 'cart_add'].copy()
payment_logs = df[df['event_type'] == 'payment'].copy()
cart_logs['analysis_id'] = cart_logs['creator_id']

cart_count = cart_logs.groupby('analysis_id').size().reset_index(name='cart_count')
success_payment_logs = payment_logs[payment_logs['payment_status'].fillna('SUCCESS') == 'SUCCESS']
payment_count_by_group = success_payment_logs.groupby('creator_id').size().reset_index(name='payment_count')
payment_count_by_group = payment_count_by_group.rename(columns={'creator_id': 'analysis_id'})
cart_data = cart_count.merge(payment_count_by_group, on='analysis_id', how='left').fillna({'payment_count': 0})
cart_data['dropout_rate'] = ((cart_data['cart_count'] - cart_data['payment_count']).clip(lower=0) / cart_data['cart_count']) * 100

sns.barplot(data=cart_data, x='analysis_id', y='dropout_rate', hue='analysis_id', ax=axes[0, 0], palette='Reds_r', legend=False)
axes[0, 0].set_title("Query 1. Cart-to-Payment Dropout Rate by Creator (%)", fontsize=12, weight='bold')
axes[0, 0].set_xlabel("Creator ID")
axes[0, 0].set_ylabel("Dropout Rate (%)")

page_view_logs = df[(df['event_type'] == 'page_view') & df['referrer'].notna()]
buffering_logs = df[df['event_type'].str.contains('buffering|video', case=False, na=False)].copy()
user_referrers = page_view_logs.drop_duplicates('user_id').set_index('user_id')['referrer']
buffering_logs['referrer'] = buffering_logs['user_id'].map(user_referrers)
buffering_logs = buffering_logs.dropna(subset=['referrer'])
channel_data = buffering_logs.groupby('referrer').size().reset_index(name='buffering_count')
if channel_data.empty:
    channel_data = pd.DataFrame({'referrer': ['no_matched_referrer'], 'buffering_count': [0]})

sns.barplot(data=channel_data, x='referrer', y='buffering_count', hue='referrer', ax=axes[0, 1], palette='viridis', legend=False)
axes[0, 1].set_title("Query 2. Video Buffering Incidents by Referrer", fontsize=12, weight='bold')
axes[0, 1].set_xlabel("Inbound Referrer")
axes[0, 1].set_ylabel("Incident Count")

success_payments = payment_logs[payment_logs['payment_status'] == 'SUCCESS']
payment_count = len(success_payments)
pending_task_count = len(success_payments[success_payments['task_status'] == 'PENDING'])
status_data = pd.DataFrame({
    'Metric': ['Success Payments', 'Pending Outbox Tasks'],
    'Volume': [payment_count, pending_task_count]
})
sns.barplot(data=status_data, x='Metric', y='Volume', hue='Metric', ax=axes[1, 0], palette='coolwarm', legend=False)
axes[1, 0].set_title("Query 3. Pending Outbox Tasks After Successful Payment", fontsize=12, weight='bold')
axes[1, 0].set_ylabel("Record Count")

creator_buffering = buffering_logs.dropna(subset=['buffering_duration'])
real_creator_stats = creator_buffering.groupby('creator_id')['buffering_duration'].mean().reset_index(name='avg_buffering_duration')
real_creator_stats = real_creator_stats.sort_values(by='avg_buffering_duration', ascending=False).head(5)
if real_creator_stats.empty:
    real_creator_stats = pd.DataFrame({'creator_id': [0], 'avg_buffering_duration': [0]})

creators = [f"Creator {int(cid)}" for cid in real_creator_stats['creator_id']]
durations = real_creator_stats['avg_buffering_duration'].tolist()

sns.barplot(x=durations, y=creators, hue=creators, ax=axes[1, 1], palette='flare', orient='h', legend=False)
axes[1, 1].set_title("Query 4. Avg Video Buffering Duration by Creator (Secs)", fontsize=12, weight='bold')
axes[1, 1].set_xlabel("Duration (Seconds)")

plt.tight_layout()
output_image = S3_PATH / "liveklass_production_dashboard.png"
plt.savefig(output_image, dpi=300)
print(f"SQL 집계 결과 대시보드 시각화 완료: {output_image}")
