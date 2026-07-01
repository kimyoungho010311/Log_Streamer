# Log_Streamer

LiveKlass 서비스의 사용자 행동을 이벤트 로그로 생성하고, PostgreSQL에 적재한 뒤 분석/시각화하는 간단한 데이터 파이프라인입니다.

## 1. 실행 방법

필요한 도구:

- Docker
- Docker Compose
- Python 3.11 이상

전체 스택 실행:

```bash
docker compose up --build
```

실행하면 `postgres-db`, `python-generator`, `airflow` 컨테이너가 함께 올라가며 이벤트 생성기가 PostgreSQL에 로그를 지속적으로 적재합니다.

대시보드 이미지 생성:

```bash
python3 analysis/dashboard.py 2026-06-30 2026-07-01
```

날짜 인자를 생략하면 전체 CSV 데이터를 대상으로 시각화합니다. 결과 이미지는 `s3_data_lake/liveklass_production_dashboard.png`에 저장됩니다.

## 2. 이벤트 설계

이벤트는 온라인 강의 플랫폼에서 자주 발생하는 행동을 기준으로 설계했습니다.

- `page_view`: 강의 상세 페이지 조회. 마케팅 유입 경로 분석에 사용합니다.
- `cart_add`: 장바구니 담기. 결제 전환/이탈 분석에 사용합니다.
- `video_buffering`: 영상 버퍼링. 콘텐츠/인프라 품질 분석에 사용합니다.
- `payment`: 결제 시도. 결제 성공/실패 및 아웃박스 패턴 실험에 사용합니다.

## 3. 스키마 설명

공통 이벤트 필드는 `master_event_logs`에 저장하고, 이벤트 타입별 상세 정보는 별도 상세 테이블에 저장했습니다.

- `users`: 수강생과 강사 마스터 데이터
- `master_event_logs`: 모든 이벤트의 공통 필드(`event_id`, `user_id`, `creator_id`, `event_type`, `created_at`)
- `page_view_details`: 강의 ID와 유입 경로
- `cart_add_details`: 장바구니에 담은 강의 ID
- `video_buffering_details`: 버퍼링 시간과 user agent
- `payment_details`: 주문 ID, 결제 금액, 성공/실패 상태
- `payment_tasks`: 결제 성공 후 후속 처리를 위한 트랜잭션 아웃박스 태스크

JSON을 통째로 저장하지 않고 필드를 분리한 이유는 이벤트 타입별 분석 쿼리를 명확하게 만들고, 공통 로그와 상세 로그를 조인해서 확장하기 쉽게 하기 위해서입니다.

## 4. 데이터 집계 분석

분석 쿼리는 `analysis/analytic_queries.sql`에 작성했습니다.

- 강의별 장바구니 결제 이탈률
- 마케팅 채널별 영상 버퍼링 장애 건수
- 결제 성공 건 대비 아웃박스 대기 상태 비율
- 강사 콘텐츠별 평균 버퍼링 지연 시간

## 5. 구현하면서 고민한 점

저장소는 PostgreSQL을 선택했습니다. 이벤트 로그는 단순 파일로도 저장할 수 있지만, 이번 과제에서는 필드를 구분해 저장하고 SQL 집계를 작성하는 요구사항이 있어 관계형 DB가 더 적합하다고 판단했습니다.

이벤트는 무작위로 생성하되 실제 서비스에서 의미 있는 분석으로 이어질 수 있도록 `page_view`, `cart_add`, `video_buffering`, `payment`로 나눴습니다. 특히 결제 성공 시 `payment_tasks`를 함께 생성해 트랜잭션 아웃박스 패턴을 간단히 표현했습니다.

## 6. Kubernetes 선택 과제 A

Kubernetes 매니페스트는 `k8s/` 디렉토리에 작성했습니다.

적용 순서:

```bash
kubectl apply -f k8s/secret.yml
kubectl apply -f k8s/pvc.yml
kubectl apply -f k8s/service.yml
kubectl apply -f k8s/deployment.yml
```

리소스 역할:

- `Secret`: PostgreSQL 접속 정보와 초기 스키마 SQL을 관리합니다.
- `PersistentVolumeClaim`: PostgreSQL 데이터가 Pod 재시작 후에도 유지되도록 스토리지를 요청합니다.
- `Service`: 이벤트 생성기 Pod가 `postgres-db`라는 안정적인 DNS 이름으로 DB에 접근하게 합니다.
- `Deployment`: PostgreSQL과 이벤트 생성기 Pod의 실행 상태와 재시작을 관리합니다.

리소스 할당:

- PostgreSQL: request `250m CPU / 512Mi`, limit `1 CPU / 1Gi`
- Event Generator: request `100m CPU / 128Mi`, limit `500m CPU / 512Mi`

PostgreSQL은 디스크와 메모리를 상대적으로 더 사용하므로 이벤트 생성기보다 높은 request/limit을 설정했습니다. 이벤트 생성기는 단순 로그 생성 워커라 낮은 기본 자원으로 시작하되, 일시적인 처리량 증가를 고려해 CPU limit을 `500m`까지 허용했습니다.
