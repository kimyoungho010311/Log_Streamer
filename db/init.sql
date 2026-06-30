-- 테이블이 존재하면 먼저 삭제 (초기화용)
DROP TABLE IF EXISTS payment_tasks CASCADE;
DROP TABLE IF EXISTS payment_details CASCADE;
DROP TABLE IF EXISTS video_buffering_details CASCADE;
DROP TABLE IF EXISTS cart_add_details CASCADE;
DROP TABLE IF EXISTS page_view_details CASCADE;
DROP TABLE IF EXISTS master_event_logs CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- 1. [마스터] 사용자 테이블
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(50) NOT NULL,
    role VARCHAR(20) NOT NULL,
    joined_at TIMESTAMPTZ NOT NULL
);

-- 2. [공통] 부모 로그 테이블
CREATE TABLE master_event_logs (
    event_id UUID PRIMARY KEY,
    user_id INT NOT NULL,
    creator_id INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_master_user FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT fk_master_creator FOREIGN KEY (creator_id) REFERENCES users(user_id)
);

-- 3. [시나리오 2] 유입 경로 상세 테이블
CREATE TABLE page_view_details (
    event_id UUID PRIMARY KEY REFERENCES master_event_logs(event_id),
    lecture_id INT NOT NULL,
    referrer VARCHAR(100) NOT NULL
);

-- 4. [시나리오 1] 장바구니 상세 테이블
CREATE TABLE cart_add_details (
    event_id UUID PRIMARY KEY REFERENCES master_event_logs(event_id),
    lecture_id INT NOT NULL
);

-- 5. [시나리오 3] 영상 버퍼링 상세 테이블
CREATE TABLE video_buffering_details (
    event_id UUID PRIMARY KEY REFERENCES master_event_logs(event_id),
    lecture_id INT NOT NULL,
    buffering_duration INT NOT NULL,
    user_agent VARCHAR(255) NOT NULL
);

-- 6. [시나리오 4] 결제 로그 테이블
CREATE TABLE payment_details (
    event_id UUID PRIMARY KEY REFERENCES master_event_logs(event_id),
    order_id VARCHAR(100) UNIQUE NOT NULL,
    amount INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_code VARCHAR(50)
);

-- 7. [시나리오 4 핵심] 아웃박스 패턴 태스크 테이블
CREATE TABLE payment_tasks (
    task_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_tasks_payment_details FOREIGN KEY (order_id) REFERENCES payment_details(order_id)
);