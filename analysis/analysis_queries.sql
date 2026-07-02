-- =========================================================================
-- 시나리오 1. 강의별 장바구니 결제 이탈률 (Cart-to-Payment Dropout Rate)
--
-- [의미]
-- 특정 강의를 장바구니에 담은 유저 중, 실제 결제 성공까지 도달하지 못하고 
-- 중간에 이탈한 비율을 추출합니다. 기획 및 마케팅 팀이 퍼널 병목을 파악하는 데 사용됩니다.
--
-- [작동 방식]
-- 1. 장바구니 이벤트(cart_add_details)를 기준으로 마스터 테이블과 조인하여 모수를 구합니다.
-- 2. 서브쿼리를 이용해 해당 유저의 결제 이력(payment_details)을 LEFT JOIN으로 가져와 매칭합니다.
-- 3. 장바구니 유저 수 대비 결제 성공(SUCCESS) 유저 수의 비율을 구한 뒤, 1에서 빼서 최종 이탈률(%)을 연산합니다.
-- =========================================================================
SELECT 
    c.lecture_id,
    COUNT(DISTINCT m.user_id) AS cart_user_count,
    COUNT(DISTINCT CASE WHEN p.status = 'SUCCESS' THEN p.order_id END) AS actual_payment_count,
    ROUND(
        (1 - COUNT(DISTINCT CASE WHEN p.status = 'SUCCESS' THEN m.user_id END)::NUMERIC 
        / NULLIF(COUNT(DISTINCT m.user_id), 0)) * 100, 
        2
    ) AS dropout_rate
FROM master_event_logs m
JOIN cart_add_details c ON m.event_id = c.event_id
LEFT JOIN payment_details p ON m.user_id = (
    SELECT pm.user_id 
    FROM master_event_logs pm 
    WHERE pm.event_id = p.event_id
)
GROUP BY c.lecture_id
ORDER BY dropout_rate DESC;


-- =========================================================================
-- 시나리오 2. 마케팅 유입 채널별 비디오 버퍼링 인시던트 통계
--
-- [의미]
-- 구글, 네이버 등 특정 마케팅 매체(Referrer)를 통해 유입된 유저들이 
-- 동영상 시청 중 버퍼링 장애를 얼마나 겪었는지 채널별로 교차 분석하여 서비스 품질 지표를 산출합니다.
--
-- [작동 방식]
-- 1. FROM 절 내에 두 개의 서브쿼리(인라인 뷰)를 생성합니다.
--    - pv_master: 유저별 유입 채널 정보 셋
--    - vb_detail: 유저별 버퍼링 지속 시간 정보 셋
-- 2. 두 데이터 셋을 user_id 기준으로 LEFT JOIN하여, 특정 채널로 들어온 유저의 버퍼링 경험 유무를 결합합니다.
-- 3. 마케팅 채널(marketing_channel) 기준으로 그룹화하여 총 유입수, 버퍼링 발생 건수, 평균 지연 시간을 집계합니다.
-- =========================================================================
SELECT 
    pv_master.marketing_channel,
    COUNT(DISTINCT pv_master.page_event_id) AS total_page_views,
    COUNT(DISTINCT vb_detail.buffer_event_id) AS buffering_incident_count,
    COALESCE(ROUND(AVG(vb_detail.buffering_duration), 2), 0.00) AS avg_buffering_seconds
FROM (
    SELECT m.user_id, m.event_id AS page_event_id, pv.referrer AS marketing_channel
    FROM master_event_logs m
    JOIN page_view_details pv ON m.event_id = pv.event_id
) pv_master
LEFT JOIN (
    SELECT m.user_id, m.event_id AS buffer_event_id, vb.buffering_duration
    FROM master_event_logs m
    JOIN video_buffering_details vb ON m.event_id = vb.event_id
) vb_detail ON pv_master.user_id = vb_detail.user_id
GROUP BY pv_master.marketing_channel
ORDER BY buffering_incident_count DESC;


-- =========================================================================
-- 시나리오 3. 트랜잭션 아웃박스 정합성 모니터링 (Outbox Stuck Rate)
--
-- [의미]
-- 결제는 정상적으로 완료(SUCCESS)되었으나, 외부 시스템(권한 지급, 알림톡 등)으로 
-- 이벤트가 발행되지 못하고 대기열(PENDING)에 멈춰있는 트랜잭션 장애 비율을 실시간으로 감시합니다.
--
-- [작동 방식]
-- 1. 메인 결제 상세(payment_details) 테이블을 기준으로 아웃박스(payment_tasks) 큐 테이블을 조인합니다.
-- 2. 상태값이 SUCCESS인 총 결제 성공 건수와, PENDING 상태로 정체된 태스크 건수를 각각 카운트합니다.
-- 3. 결제 성공 건수 대비 미처리 태스크의 비율을 백분율(%)로 계산하여 시스템 후속 처리 누락률을 도출합니다.
-- =========================================================================
SELECT 
    COUNT(CASE WHEN p.status = 'SUCCESS' THEN 1 END) AS total_success_payments,
    COUNT(CASE WHEN pt.status = 'PENDING' THEN 1 END) AS pending_outbox_tasks,
    ROUND(
        (COUNT(CASE WHEN pt.status = 'PENDING' THEN 1 END)::NUMERIC / 
        NULLIF(COUNT(CASE WHEN p.status = 'SUCCESS' THEN 1 END), 0)) * 100, 
        2
    ) AS outbox_stuck_rate
FROM payment_details p
LEFT JOIN payment_tasks pt ON p.order_id = pt.order_id;


-- =========================================================================
-- 시나리오 4. 강사 콘텐츠별 평균 버퍼링 지연 시간 위험도
--
-- [의미]
-- 플랫폼 내 특정 크리에이터(강사)의 콘텐츠 시청 시 유독 버퍼링이 길게 발생하는지 확인하여, 
-- 특정 비디오 서버 인프라의 자원 병목이나 콘텐츠 최적화 문제를 식별하는 데 활용됩니다.
--
-- [작동 방식]
-- 1. 공통 마스터 로그(master_event_logs)와 버퍼링 상세 로그(video_buffering_details)를 식별키(event_id)로 조인합니다.
-- 2. 콘텐츠 제공자인 강사(creator_id)를 기준으로 데이터를 그룹화(GROUP BY)합니다.
-- 3. 강사별 총 발생 이벤트 수와 버퍼링 발생 횟수, 평균 버퍼링 지연 시간(AVG)을 계산하고 지연 시간이 긴 순서대로 내림차순 정렬합니다.
-- =========================================================================
SELECT 
    m.creator_id,
    COUNT(DISTINCT m.event_id) AS total_creator_events,
    COUNT(DISTINCT vb.event_id) AS total_buffering_incidents,
    COALESCE(ROUND(AVG(vb.buffering_duration), 2), 0.00) AS avg_buffering_duration_secs
FROM master_event_logs m
JOIN video_buffering_details vb ON m.event_id = vb.event_id
GROUP BY m.creator_id
ORDER BY avg_buffering_duration_secs DESC;