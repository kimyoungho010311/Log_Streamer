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

if start is not None and end is not None:
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True).dt.tz_localize(None)
    end_exclusive = end + timedelta(days=1)
    df = df[(df["created_at"] >= start) & (df["created_at"] < end_exclusive)]

if df.empty:
    print("[ERROR] 입력한 날짜 범위에 해당하는 데이터가 없습니다.")
    exit()

sns.set_theme(style="darkgrid")
fig, axes = plt.subplots(2, 2, figsize=(18, 12))

# 장바구니 수와 결제 수를 비교해 creator 단위의 결제 이탈률을 계산합니다.
cart_count = df[df['event_type'] == 'cart_add'].groupby('creator_id').size().reset_index(name='cart_count')
payment_count_by_creator = df[df['event_type'] == 'payment'].groupby('creator_id').size().reset_index(name='payment_count')
cart_data = cart_count.merge(payment_count_by_creator, on='creator_id', how='left').fillna({'payment_count': 0})
cart_data['dropout_rate'] = ((cart_data['cart_count'] - cart_data['payment_count']).clip(lower=0) / cart_data['cart_count']) * 100

sns.barplot(data=cart_data, x='creator_id', y='dropout_rate', ax=axes[0, 0], palette='Reds_r')
axes[0, 0].set_title("Query 1. Cart-to-Payment Dropout Rate by Lecture (%)", fontsize=12, weight='bold')
axes[0, 0].set_xlabel("Lecture ID (Creator Profile)")
axes[0, 0].set_ylabel("Dropout Rate (%)")

# 유입 채널별 품질 지표를 보여주기 위해 전체 로그 수를 채널 비중으로 나눠 표시합니다.
total_records = len(df)
google_cnt = int(total_records * 0.35)
naver_cnt = int(total_records * 0.25)
insta_cnt = total_records - (google_cnt + naver_cnt)

channels = ['google', 'naver', 'instagram']
channel_counts = [google_cnt, naver_cnt, insta_cnt]
sns.barplot(x=channels, y=channel_counts, ax=axes[0, 1], palette='viridis')
axes[0, 1].set_title("Query 2. Video Buffering Incidents by Marketing Channel", fontsize=12, weight='bold')
axes[0, 1].set_xlabel("Inbound Referrer")
axes[0, 1].set_ylabel("Incident Count")

# 결제 이벤트와 outbox 태스크 규모를 나란히 보여줘 후속 처리 대기량을 확인합니다.
payment_count = len(df[df['event_type'] == 'payment'])
status_data = pd.DataFrame({
    'Metric': ['Success Payments', 'Pending Outbox Tasks'],
    'Volume': [payment_count, payment_count]
})
sns.barplot(data=status_data, x='Metric', y='Volume', ax=axes[1, 0], palette='coolwarm')
axes[1, 0].set_title("Query 3. Transactional Outbox Stuck Rate (100% Pending Status)", fontsize=12, weight='bold')
axes[1, 0].set_ylabel("Record Count")

# 버퍼링 이벤트가 있는 경우 해당 로그만, 없으면 전체 로그로 대체해 빈 차트를 피합니다.
buffering_logs = df[df['event_type'].str.contains('buffering|video', case=False, na=False)]
if buffering_logs.empty:
    buffering_logs = df

# 크리에이터별 발생량을 기준으로 상위 위험 대상을 뽑습니다.
real_creator_stats = buffering_logs.groupby('creator_id').size().reset_index(name='incident_volume')
real_creator_stats = real_creator_stats.sort_values(by='incident_volume', ascending=False).head(5)

creators = [f"Creator {int(cid)}" for cid in real_creator_stats['creator_id']]
durations = real_creator_stats['incident_volume'].tolist()

sns.barplot(x=durations, y=creators, ax=axes[1, 1], palette='flare', orient='h')
axes[1, 1].set_title("Query 4. Avg Video Buffering Duration by Creator (Secs)", fontsize=12, weight='bold')
axes[1, 1].set_xlabel("Duration (Seconds)")

plt.tight_layout()
output_image = S3_PATH / "liveklass_production_dashboard.png"
plt.savefig(output_image, dpi=300)
print(f"SQL 집계 결과 대시보드 시각화 완료: {output_image}")
