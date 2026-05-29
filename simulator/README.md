# Source Transaction Simulator

The simulator generates PostgreSQL row changes for CDC testing. It does not write to Kafka directly. Debezium captures the resulting WAL changes in later steps.

## Scenarios

The default run creates:

- customer insert
- product insert
- order insert with order_items in the same transaction
- payment request insert
- payment approval update in a separate transaction
- payment capture update in a separate transaction
- order `PAID` update in a separate transaction
- payment failure update
- order cancellation update
- refund request insert
- refund completion update
- test data delete
- rapid consecutive updates on the same customer row

## Dependencies

The simulator uses only the Python standard library and the running Docker Compose PostgreSQL service.

## Run

```bash
./scripts/run-simulator.sh
```

Custom run id:

```bash
./scripts/run-simulator.sh --run-id demo-001
```

The script loads `.env` and `simulator/config.yaml`. By default it executes `psql` inside the Docker Compose `postgres` service.

Print generated SQL without executing:

```bash
./scripts/run-simulator.sh --print-sql
```

## Notes

- Order creation and order item creation are committed in one transaction.
- Payment status transitions are committed as separate transactions.
- Refund status transitions are committed as separate transactions.
- Delete scenarios use physical deletes in PostgreSQL because the project needs delete CDC events. Downstream Lakehouse layers handle them as soft deletes.
