from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import csv
import logging

default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 로컬 파일 시스템을 S3 데이터 레이크처럼 사용하기 위한 루트 경로입니다.
S3_VIRTUAL_ROOT = os.getenv("S3_VIRTUAL_PATH", "/opt/airflow/s3_data_lake")
COMMON_COLUMNS = ['event_id', 'user_id', 'creator_id', 'event_type', 'created_at']
EVENT_COLUMNS = {
    'page_view': COMMON_COLUMNS + ['lecture_id', 'referrer'],
    'cart_add': COMMON_COLUMNS + ['lecture_id'],
    'video_buffering': COMMON_COLUMNS + ['lecture_id', 'buffering_duration', 'user_agent'],
    'payment': COMMON_COLUMNS + ['order_id', 'amount', 'payment_status', 'error_code', 'task_status', 'retry_count'],
}

with DAG(
    dag_id='liveklass_log_management_pipeline',
    default_args=default_args,
    schedule_interval='1 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'database', 'backup'],
) as dag:

    def extract_and_backup_to_s3(**context):
        """PostgreSQL 원본 로그를 이벤트 타입과 날짜 기준 CSV 파티션으로 백업합니다."""
        logger = logging.getLogger("airflow.task")
        logger.info("BATCH: Initiating log extraction from PostgreSQL to YYYY-MM-DD Partitioned Virtual S3.")
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_main')
        target_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        select_query = """
            SELECT
                m.event_id,
                m.user_id,
                m.creator_id,
                m.event_type,
                m.created_at,
                COALESCE(pv.lecture_id, ca.lecture_id, vb.lecture_id) AS lecture_id,
                pv.referrer,
                vb.buffering_duration,
                vb.user_agent,
                pd.order_id,
                pd.amount,
                pd.status AS payment_status,
                pd.error_code,
                pt.status AS task_status,
                pt.retry_count
            FROM master_event_logs m
            LEFT JOIN page_view_details pv ON m.event_id = pv.event_id
            LEFT JOIN cart_add_details ca ON m.event_id = ca.event_id
            LEFT JOIN video_buffering_details vb ON m.event_id = vb.event_id
            LEFT JOIN payment_details pd ON m.event_id = pd.event_id
            LEFT JOIN payment_tasks pt ON pd.order_id = pt.order_id
            WHERE m.created_at <= %s;
        """
        
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(select_query, (target_date,))
        rows = cursor.fetchall()
        
        if not rows:
            logger.warning("BATCH: No target log records found for the specified period. Terminating task.")
            context['ti'].xcom_push(key='has_data', value=False)
            return

        processed_count = 0
        
        for row in rows:
            event_data = {
                'event_id': row[0],
                'user_id': row[1],
                'creator_id': row[2],
                'event_type': row[3],
                'created_at': row[4],
                'lecture_id': row[5],
                'referrer': row[6],
                'buffering_duration': row[7],
                'user_agent': row[8],
                'order_id': row[9],
                'amount': row[10],
                'payment_status': row[11],
                'error_code': row[12],
                'task_status': row[13],
                'retry_count': row[14],
            }
            event_type = event_data['event_type']
            created_at = event_data['created_at']
            
            if isinstance(created_at, str):
                dt_obj = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            else:
                dt_obj = created_at
                
            date_str = dt_obj.strftime('%Y-%m-%d')
            
            # event_type/date 구조로 저장해 특정 이벤트와 날짜만 따로 조회할 수 있게 합니다.
            partition_dir = os.path.join(
                S3_VIRTUAL_ROOT, 
                "raw-data", 
                "liveklass", 
                str(event_type).lower(),
                date_str
            )
            
            os.makedirs(partition_dir, exist_ok=True)
            
            # 한 날짜 파티션 안에서 배치 실행 단위별 파일을 구분합니다.
            file_name = f"log_daily_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            file_path = os.path.join(partition_dir, file_name)
            
            file_exists = os.path.exists(file_path)
            
            with open(file_path, mode='a', newline='', encoding='utf-8') as f:
                columns = EVENT_COLUMNS.get(str(event_type).lower(), COMMON_COLUMNS)
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(columns)
                writer.writerow([event_data[column] for column in columns])
                
            processed_count += 1
            
        logger.info(f"BATCH: Successfully sequenced {processed_count} records into YYYY-MM-DD folders.")
        
        context['ti'].xcom_push(key='has_data', value=True)
        context['ti'].xcom_push(key='target_date', value=target_date)
        context['ti'].xcom_push(key='processed_row_count', value=processed_count)

    def purge_database_records(**context):
        """백업 완료 데이터를 원본 DB에서 정리합니다.

        Airflow가 주기적으로 같은 구간을 다시 백업하지 않도록 처리 완료된 로그를 삭제하고,
        외래키 제약조건을 지키기 위해 자식 테이블에서 부모 테이블 순서로 정리합니다.
        """
        logger = logging.getLogger("airflow.task")
        has_data = context['ti'].xcom_pull(task_ids='extract_and_backup_to_s3', key='has_data')
        if not has_data:
            logger.info("BATCH: Purge step skipped. No data processed.")
            return

        target_date = context['ti'].xcom_pull(task_ids='extract_and_backup_to_s3', key='target_date')
        row_count = context['ti'].xcom_pull(task_ids='extract_and_backup_to_s3', key='processed_row_count')
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_main')
        
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

    def report_pipeline_metrics(**context):
        """이번 DAG 실행에서 처리된 행 수를 Airflow 로그에 남깁니다."""
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
