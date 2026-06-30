import os
import time
import psycopg2
from dotenv import load_dotenv

print("🚀 라이브클래스 로그 제네레이터 구동 시작!")

# 환경변수 로드
load_dotenv()

db_host = os.getenv("DB_HOST", "postgres-db")
db_name = os.getenv("DB_NAME", "liveklass_db")
db_user = os.getenv("DB_USER", "admin")
db_password = os.getenv("DB_PASSWORD", "admin")

# DB 연결 확인용 무한 루프
while True:
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print("✅ PostgreSQL 데이터베이스 연결 성공! 대기 중...")
        conn.close()
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}")
    
    # 5초마다 한 번씩 실행하며 컨테이너가 죽지 않도록 유지
    time.sleep(5)