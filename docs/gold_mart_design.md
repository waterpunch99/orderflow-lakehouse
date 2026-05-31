# Gold Mart 설계

## 목적

Gold mart는 Silver current table에서 만든 작고 분석 준비가 된 aggregate입니다. Mart는 Silver에서 재생성 가능하며 `s3a://lakehouse/warehouse`의 Hadoop Catalog warehouse에 Apache Iceberg table로 저장됩니다.

## Source Tables

- `lakehouse.silver.silver_orders_current`
- `lakehouse.silver.silver_payments_current`
- `lakehouse.silver.silver_refunds_current`

Gold mart는 `is_deleted = true`인 row를 제외합니다. 이렇게 하면 delete 이벤트는 Silver에서 볼 수 있으면서도, source에서 삭제된 row가 active business KPI에 반영되지 않습니다.

## Tables

### gold_daily_order_payment_summary

`summary_date`를 key로 하는 일별 주문/결제 KPI table입니다.

Metrics:

- `order_count`
- `paid_order_count`
- `payment_success_order_count`
- `payment_success_amount`
- `payment_failed_count`
- `payment_success_rate`
- `refund_count`
- `refund_amount`
- `refund_rate`

### gold_order_funnel_summary

`summary_date`를 key로 하는 주문 funnel table입니다.

Metrics:

- `created_order_count`
- `paid_order_count`
- `payment_completed_order_count`
- `cancelled_order_count`
- `refunded_order_count`
- `order_to_paid_rate`
- `order_to_payment_completed_rate`
- `order_to_refunded_rate`

### gold_payment_failure_summary

`failure_date`, `payment_method`, `failure_code`를 key로 하는 결제 실패 table입니다.

Metrics:

- `failed_payment_count`
- `failed_order_count`
- `failed_requested_amount`

### gold_refund_summary

`refund_date`, `refund_status`를 key로 하는 환불 table입니다.

Metrics:

- `refund_count`
- `completed_refund_count`
- `refund_amount`
- `completed_refund_amount`
- `affected_order_count`
- `refund_rate`

## Rebuild Strategy

Gold job은 필요한 table을 생성하고, 기존 Gold row를 삭제한 뒤, Silver의 최신 aggregate를 insert합니다. Gold mart는 Silver current table의 결정적 파생물이므로 의도적으로 batch-oriented 방식으로 구현합니다.

실행:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/rebuild.py
```

Count 검증:

```bash
./scripts/run-spark-job.sh /opt/orderflow/spark/jobs/gold_marts/check_counts.py
```

## Object Storage

Gold table data와 Iceberg metadata는 설정된 Iceberg warehouse 아래에 저장됩니다.

```text
s3a://lakehouse/warehouse/gold/
```

로컬 개발에서는 이 경로가 MinIO `lakehouse` bucket에 매핑됩니다. 같은 Spark/Iceberg 설정은 `docs/object_storage_design.md`에 문서화된 S3 endpoint, credential, bucket policy 설정 변경만으로 AWS S3로 이동할 수 있습니다.
