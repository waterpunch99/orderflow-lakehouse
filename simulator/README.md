# Source Transaction Simulator

시뮬레이터는 CDC 테스트를 위해 PostgreSQL 원천 테이블에 행 변경을 발생시킵니다. Kafka에 직접 쓰지 않으며, 이후 Debezium이 PostgreSQL WAL 변경을 캡처합니다.

## 시나리오

기본 실행은 다음 변경을 생성합니다.

- 고객 insert
- 상품 insert
- 주문 insert 및 동일 트랜잭션의 order_items insert
- 결제 요청 insert
- 별도 트랜잭션의 결제 승인 update
- 별도 트랜잭션의 결제 캡처 update
- 별도 트랜잭션의 주문 `PAID` update
- 결제 실패 update
- 주문 취소 update
- 환불 요청 insert
- 환불 완료 update
- 테스트 데이터 delete
- 동일 고객 행에 대한 빠른 연속 update

## 의존성

시뮬레이터는 Python 표준 라이브러리와 Docker Compose로 실행 중인 PostgreSQL 서비스만 사용합니다.

## 실행

```bash
./scripts/run-simulator.sh
```

사용자 지정 run id:

```bash
./scripts/run-simulator.sh --run-id demo-001
```

스크립트는 `.env`와 `simulator/config.yaml`을 읽습니다. 기본적으로 Docker Compose의 `postgres` 서비스 안에서 `psql`을 실행합니다.

SQL만 출력하고 실행하지 않기:

```bash
./scripts/run-simulator.sh --print-sql
```

## 참고 사항

- 주문과 주문 아이템 생성은 하나의 트랜잭션으로 커밋됩니다.
- 결제 상태 전이는 각각 별도 트랜잭션으로 커밋됩니다.
- 환불 상태 전이는 각각 별도 트랜잭션으로 커밋됩니다.
- delete 시나리오는 delete CDC 이벤트 검증을 위해 PostgreSQL 물리 삭제를 사용합니다. 하위 Lakehouse 레이어에서는 이를 soft delete로 처리합니다.
