from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'admin',
    'retries': 0,
}

with DAG(
    dag_id='airflow_infra_connection_test',
    default_args=default_args,
    schedule_interval=None,  # 수동으로만 실행 테스트
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    def test_postgres_connection():
        logger = logging.getLogger("airflow.task")
        logger.info("========== DB 연결 테스트 시작 ==========")
        
        try:
            # Airflow 내부의 기본 Postgres 커넥션 사용
            hook = PostgresHook(postgres_conn_id='postgres_default')
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            # 간단한 쿼리 실행 테스트
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()
            
            logger.info(f"👍 [SUCCESS] 데이터베이스 연결 성공! 결과값: {result[0]}")
            
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"💥 [FAIL] 데이터베이스 연결 실패: {e}")
            raise e

    run_test = PythonOperator(
        task_id='run_connection_test',
        python_callable=test_postgres_connection
    )