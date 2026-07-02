import os
import time
import uuid
import random
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

class LogColor:
    """컨테이너 로그에서 이벤트 종류를 빠르게 구분하기 위한 ANSI 컬러 포맷터."""

    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RED = "\033[91m"
    PURPLE = "\033[95m"
    RESET = "\033[0m"

    @classmethod
    def success(cls, text): return f"{cls.GREEN}[SUCCESS] {text}{cls.RESET}"
    @classmethod
    def warn(cls, text): return f"{cls.YELLOW}[WARN] {text}{cls.RESET}"
    @classmethod
    def info(cls, text): return f"{cls.BLUE}[INFO] {text}{cls.RESET}"
    @classmethod
    def error(cls, text): return f"{cls.RED}[ERROR] {text}{cls.RESET}"
    @classmethod
    def action(cls, text): return f"{cls.PURPLE}[ACTION] {text}{cls.RESET}"

print(LogColor.success("SYSTEM: LiveKlass Log Generator initialized."))

load_dotenv()
db_host = os.getenv("DB_HOST", "postgres-db")
db_name = os.getenv("DB_NAME", "liveklass_db")
db_user = os.getenv("DB_USER", "admin")
db_password = os.getenv("DB_PASSWORD", "admin")

# 시뮬레이션 대상이 되는 수강생, 강사, 강의 후보군입니다.
STUDENT_POOL = list(range(1000,3000))
CREATOR_POOL = list(range(1, 5))
LECTURE_POOL = list(range(1, 10))
USER_AGENTS = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", "Mobile_Safari", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/120.0"]

def get_db_connection():
    """환경변수에 정의된 PostgreSQL 접속 정보를 사용해 커넥션을 생성합니다."""
    return psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)

def init_master_data():
    """이벤트 로그가 참조할 수강생과 강사 마스터 데이터를 중복 없이 준비합니다."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for uid in STUDENT_POOL:
            cur.execute("""
                INSERT INTO users (user_id, email, name, role, joined_at)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING;
            """, (uid, f"student_{uid}@liveklass.com", f"수강생_{uid}", "STUDENT", datetime.now()))
        for cid in CREATOR_POOL:
            cur.execute("""
                INSERT INTO users (user_id, email, name, role, joined_at)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING;
            """, (cid, f"creator_{cid}@liveklass.com", f"강사_{cid}", "CREATOR", datetime.now()))
        conn.commit()
        cur.close()
        print(LogColor.success("DATABASE: Master user integrity check passed."))
    except Exception as e:
        if conn: conn.rollback()
        print(LogColor.error(f"DATABASE: Master data seeding failed: {e}"))
        raise e
    finally:
        if conn: conn.close()

def insert_log_to_db(event_id, user_id, creator_id, event_type, detail_query, detail_params, task_query=None, task_params=None):
    """공통 이벤트와 상세 이벤트를 하나의 트랜잭션으로 저장합니다.

    결제 성공 이벤트처럼 후속 처리가 필요한 경우에는 같은 트랜잭션 안에서
    outbox 태스크까지 함께 기록해 데이터 정합성을 맞춥니다.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        parent_query = """
            INSERT INTO master_event_logs (event_id, user_id, creator_id, event_type, created_at)
            VALUES (%s, %s, %s, %s, %s);
        """
        cur.execute(parent_query, (event_id, user_id, creator_id, event_type, datetime.now()))
        cur.execute(detail_query, detail_params)
        
        if task_query and task_params:
            cur.execute(task_query, task_params)
            
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        if conn: conn.rollback()
        print(LogColor.error(f"TRANSACTION: Rollback executed due to integrity failure - {e}"), flush=True)
        return False
    finally:
        if conn: conn.close()

def trigger_scenario_1_cart(user_id, creator_id, lecture_id):
    """시나리오 1: 장바구니 담기 (이탈/잔류 분석 데이터 확보 목적)"""
    event_id = str(uuid.uuid4())
    detail_query = "INSERT INTO cart_add_details (event_id, lecture_id) VALUES (%s, %s);"
    detail_params = (event_id, lecture_id)
    if insert_log_to_db(event_id, user_id, creator_id, "cart_add", detail_query, detail_params):
        print(LogColor.info(f"TRACKING: User {user_id} added lecture {lecture_id} to cart. Status: Retained."), flush=True)

def trigger_scenario_2_pageview(user_id, creator_id, lecture_id):
    """시나리오 2: 상세 페이지 유입 (유입 경로 마케팅 분석 목적)"""
    event_id = str(uuid.uuid4())
    referrer = random.choice(["instagram", "naver_ad", "google", "direct"])
    detail_query = "INSERT INTO page_view_details (event_id, lecture_id, referrer) VALUES (%s, %s, %s);"
    detail_params = (event_id, lecture_id, referrer)
    if insert_log_to_db(event_id, user_id, creator_id, "page_view", detail_query, detail_params):
        print(LogColor.info(f"TRACKING: User {user_id} viewed lecture {lecture_id} page via {referrer}."), flush=True)

def trigger_scenario_3_buffering(user_id, creator_id, lecture_id):
    """시나리오 3: 영상 재생 장애 (인프라 안정성 분석 목적)"""
    event_id = str(uuid.uuid4())
    duration = random.randint(3, 30)
    agent = random.choice(USER_AGENTS)
    detail_query = "INSERT INTO video_buffering_details (event_id, lecture_id, buffering_duration, user_agent) VALUES (%s, %s, %s, %s);"
    detail_params = (event_id, lecture_id, duration, agent)
    if insert_log_to_db(event_id, user_id, creator_id, "video_buffering", detail_query, detail_params):
        print(LogColor.warn(f"METRIC: Media player alert - User {user_id}, Lecture {lecture_id}, Buffering: {duration}s, Agent: {agent[:15]}..."), flush=True)

def trigger_scenario_4_payment(user_id, creator_id):
    """시나리오 4: 결제 시도 및 트랜잭션 아웃박스 발행"""
    event_id = str(uuid.uuid4())
    order_id = f"ORD-{datetime.now().strftime('%Y%m%d')}-{random.randint(100000, 999999)}"
    amount = random.choice([50000, 99000, 150000])
    status = "SUCCESS" if random.random() > 0.2 else "FAIL"
    error_code = None if status == "SUCCESS" else random.choice(["PG_TIMEOUT", "CARD_DENIED"])
    
    detail_query = "INSERT INTO payment_details (event_id, order_id, amount, status, error_code) VALUES (%s, %s, %s, %s, %s);"
    detail_params = (event_id, order_id, amount, status, error_code)
    
    task_query, task_params = None, None
    if status == "SUCCESS":
        task_query = "INSERT INTO payment_tasks (order_id, status, retry_count, updated_at) VALUES (%s, 'PENDING', 0, %s);"
        task_params = (order_id, datetime.now())
        
    if insert_log_to_db(event_id, user_id, creator_id, "payment", detail_query, detail_params, task_query, task_params):
        if status == "SUCCESS":
            print(LogColor.action(f"TRANSACTION: Payment succeeded. Order: {order_id}, User: {user_id}. Transaction Outbox task registered."), flush=True)
        else:
            print(LogColor.error(f"TRANSACTION: Payment failed. Order: {order_id}, User: {user_id}, Reason: {error_code}."), flush=True)

if __name__ == "__main__":
    init_master_data()
    
    while True:
        user_id = random.choice(STUDENT_POOL)
        creator_id = random.choice(CREATOR_POOL)
        lecture_id = random.choice(LECTURE_POOL)
        
        # 페이지 조회가 가장 자주 발생하고, 결제는 상대적으로 드물게 발생하도록 가중치를 둡니다.
        scenario = random.choices([1, 2, 3, 4], weights=[25, 50, 15, 10], k=1)[0]
        
        if scenario == 1:
            trigger_scenario_1_cart(user_id, creator_id, lecture_id)
        elif scenario == 2: 
            trigger_scenario_2_pageview(user_id, creator_id, lecture_id)
        elif scenario == 3:
            trigger_scenario_3_buffering(user_id, creator_id, lecture_id)
        elif scenario == 4:
            trigger_scenario_4_payment(user_id, creator_id)
            
        time.sleep(0.1)
