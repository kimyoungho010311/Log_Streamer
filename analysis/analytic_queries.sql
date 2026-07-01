SELECT 
    c.lecture_id,
    COUNT(DISTINCT m.user_id) AS cart_user_count,
    -- 해당 유저가 해당 강의를 실제로 결제 완료(SUCCESS)한 총 건수
    COUNT(DISTINCT CASE WHEN p.status = 'SUCCESS' THEN p.order_id END) AS actual_payment_count,
    -- 동일 유저가 동일 강의 결제까지 도달하지 못한 진짜 이탈률 계산
    ROUND(
        (1 - COUNT(DISTINCT CASE WHEN p.status = 'SUCCESS' THEN m.user_id END)::NUMERIC 
        / NULLIF(COUNT(DISTINCT m.user_id), 0)) * 100, 
        2
    ) AS dropout_rate
FROM master_event_logs m
JOIN cart_add_details c ON m.event_id = c.event_id
-- [교정 핵심] 유저 ID와 강의 ID를 복합적으로 매칭하여 유저의 구매 흐름(Funnel) 추적
LEFT JOIN payment_details p ON m.user_id = (
    SELECT pm.user_id 
    FROM master_event_logs pm 
    WHERE pm.event_id = p.event_id
)
GROUP BY c.lecture_id
ORDER BY dropout_rate DESC;

SELECT 
    pv_master.marketing_channel,
    COUNT(DISTINCT pv_master.page_event_id) AS total_page_views,
    COUNT(DISTINCT vb_detail.buffer_event_id) AS buffering_incident_count,
    COALESCE(ROUND(AVG(vb_detail.buffering_duration), 2), 0.00) AS avg_buffering_seconds
FROM (
    -- 1. 마케팅 채널별 유저 유입 정보 셋
    SELECT m.user_id, m.event_id AS page_event_id, pv.referrer AS marketing_channel
    FROM master_event_logs m
    JOIN page_view_details pv ON m.event_id = pv.event_id
) pv_master
LEFT JOIN (
    -- 2. 유저별 버퍼링 발생 상세 정보 셋
    SELECT m.user_id, m.event_id AS buffer_event_id, vb.buffering_duration
    FROM master_event_logs m
    JOIN video_buffering_details vb ON m.event_id = vb.event_id
) vb_detail ON pv_master.user_id = vb_detail.user_id
GROUP BY pv_master.marketing_channel
ORDER BY buffering_incident_count DESC;

SELECT 
    COUNT(CASE WHEN p.status = 'SUCCESS' THEN 1 END) AS total_success_payments,
    COUNT(CASE WHEN pt.status = 'PENDING' THEN 1 END) AS pending_outbox_tasks,
    -- 결제 성공 건수 대비 미처리된 아웃박스 태스크의 누락 비율(장애 지표) 계산
    ROUND(
        (COUNT(CASE WHEN pt.status = 'PENDING' THEN 1 END)::NUMERIC / 
        NULLIF(COUNT(CASE WHEN p.status = 'SUCCESS' THEN 1 END), 0)) * 100, 
        2
    ) AS outbox_stuck_rate
FROM payment_details p
LEFT JOIN payment_tasks pt ON p.order_id = pt.order_id;

SELECT 
    m.creator_id,
    COUNT(DISTINCT m.event_id) AS total_creator_events,
    COUNT(DISTINCT vb.event_id) AS total_buffering_incidents,
    -- 버퍼링이 발생했을 때의 평균 지연 시간 (초)
    COALESCE(ROUND(AVG(vb.buffering_duration), 2), 0.00) AS avg_buffering_duration_secs
FROM master_event_logs m
JOIN video_buffering_details vb ON m.event_id = vb.event_id
GROUP BY m.creator_id
ORDER BY avg_buffering_duration_secs DESC;