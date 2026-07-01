from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import csv
import logging

# 1. DAG 기본 설정
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 데이터 레이크의 최상위 루트 경로
S3_VIRTUAL_ROOT = os.getenv("S3_VIRTUAL_PATH", "/opt/airflow/s3_data_lake")

with DAG(
    dag_id='liveklass_log_management_pipeline',
    default_args=default_args,
    schedule_interval='1 * * * *',  # 실습용 1분 주기 유지
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'database', 'backup'],
) as dag:

    # ── TASK 1: PostgreSQL 데이터 추출 및 날짜별(YYYY-MM-DD) 가상 S3 백업 ──
    def extract_and_backup_to_s3(**context):
        logger = logging.getLogger("airflow.task")
        logger.info("BATCH: Initiating log extraction from PostgreSQL to YYYY-MM-DD Partitioned Virtual S3.")
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        target_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        select_query = """
            SELECT event_id, user_id, creator_id, event_type, created_at 
            FROM master_event_logs 
            WHERE created_at <= %s;
        """
        
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(select_query, (target_date,))
        rows = cursor.fetchall()
        
        if not rows:
            logger.warning("BATCH: No target log records found for the specified period. Terminating task.")
            context['ti'].xcom_push(key='has_data', value=False)
            return

        # ----------------------------------------------------------------------
        # [핵심 변경] event_type/YYYY-MM-DD/ 디렉토리 구조로 대통일
        # ----------------------------------------------------------------------
        processed_count = 0
        
        for row in rows:
            event_id, user_id, creator_id, event_type, created_at = row
            
            # 1. DB의 created_at 값에서 YYYY-MM-DD 포맷만 추출
            if isinstance(created_at, str):
                dt_obj = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            else:
                dt_obj = created_at
                
            date_str = dt_obj.strftime('%Y-%m-%d') # 예: '2026-06-30'
            
            # 2. 직관적인 디렉토리 경로 생성
            # 예시 경로: /opt/airflow/s3_data_lake/raw-data/liveklass/page_view/2026-06-30/
            partition_dir = os.path.join(
                S3_VIRTUAL_ROOT, 
                "raw-data", 
                "liveklass", 
                str(event_type).lower(),
                date_str  # 👈 YYYY-MM-DD 폴더가 바로 생성됨
            )
            
            # 3. 디렉토리가 없으면 자동 생성
            os.makedirs(partition_dir, exist_ok=True)
            
            # 4. 해당 날짜 폴더 안에 저장될 일일 로그 파일 정의
            # 그날의 데이터가 계속 누적되도록 파일명을 고정하거나 배치의 유니크함을 줍니다.
            # 여기서는 배치 실행 시간 분 단위까지 묶어 유니크하게 저장하되, 한 날짜 폴더에 다 모이게 만듭니다.
            file_name = f"log_daily_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            file_path = os.path.join(partition_dir, file_name)
            
            # 5. 파일이 이미 존재하면 헤더 생략, 없으면 헤더 작성 (Append 모드)
            file_exists = os.path.exists(file_path)
            
            with open(file_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['event_id', 'user_id', 'creator_id', 'event_type', 'created_at'])
                writer.writerow([event_id, user_id, creator_id, event_type, created_at])
                
            processed_count += 1
            
        logger.info(f"BATCH: Successfully sequenced {processed_count} records into YYYY-MM-DD folders.")
        
        context['ti'].xcom_push(key='has_data', value=True)
        context['ti'].xcom_push(key='target_date', value=target_date)
        context['ti'].xcom_push(key='processed_row_count', value=processed_count)

    # ── TASK 2: 데이터베이스 원본 데이터 삭제 (기존과 동일) ──
    def purge_database_records(**context):
        logger = logging.getLogger("airflow.task")
        has_data = context['ti'].xcom_pull(task_ids='extract_and_backup_to_s3', key='has_data')
        if not has_data:
            logger.info("BATCH: Purge step skipped. No data processed.")
            return

        target_date = context['ti'].xcom_pull(task_ids='extract_and_backup_to_s3', key='target_date')
        row_count = context['ti'].xcom_pull(task_ids='extract_and_backup_to_s3', key='processed_row_count')
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        purge_queries = [
            "DELETE FROM payment_tasks WHERE order_id IN (SELECT order_id FROM payment_details WHERE event_id IN (SELECT event_id FROM master_event_logs WHERE created_at <= %s));",
            "DELETE FROM page_view_details WHERE event_id IN (SELECT event_id FROM master_event_logs WHERE created_at <= %s);",
            "DELETE FROM cart_add_details WHERE event_id IN (SELECT event_id FROM master_event_logs WHERE created_at <= %s);",
            "DELETE FROM video_buffering_details WHERE event_id IN (SELECT event_id FROM master_event_logs WHERE created_at <= %s);",
            "DELETE FROM payment_details WHERE event_id IN (SELECT event_id FROM master_event_logs WHERE created_at <= %s);",
            "DELETE FROM master_event_logs WHERE created_at <= %s;"
        ]
        
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        try:
            for query in purge_queries:
                cursor.execute(query, (target_date,))
            conn.commit()
            logger.info(f"BATCH: Successfully purged {row_count} records from PostgreSQL.")
        except Exception as e:
            conn.rollback()
            logger.error(f"BATCH: Database purge failed. Transaction rolled back: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()

    # ── TASK 3: 최종 모니터링 리포트 ──
    def report_pipeline_metrics(**context):
        logger = logging.getLogger("airflow.task")
        has_data = context['ti'].xcom_pull(task_ids='extract_and_backup_to_s3', key='has_data')
        
        logger.info("------------------------------------------------------------")
        if has_data:
            count = context['ti'].xcom_pull(task_ids='extract_and_backup_to_s3', key='processed_row_count')
            logger.info(f"METRIC: Pipeline execution report - Target: {count} rows. Status: SUCCESS.")
        else:
            logger.info("METRIC: Pipeline execution report - Status: IDLE.")
        logger.info("------------------------------------------------------------")

    task_backup = PythonOperator(task_id='extract_and_backup_to_s3', python_callable=extract_and_backup_to_s3)
    task_purge  = PythonOperator(task_id='purge_database_records',   python_callable=purge_database_records)
    task_report = PythonOperator(task_id='report_pipeline_metrics',  python_callable=report_pipeline_metrics)

    task_backup >> task_purge >> task_report