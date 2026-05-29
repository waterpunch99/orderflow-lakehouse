# Domain Model

## Entities

### customers

Represents a buyer account. Customer rows are referenced by orders.

Primary key:

- `customer_id`

Columns:

- `customer_name`
- `email`
- `phone`
- `customer_status`
- `created_at`
- `updated_at`

### products

Represents a sellable product. Product rows are referenced by order items.

Primary key:

- `product_id`

Columns:

- `sku`
- `product_name`
- `category`
- `unit_price`
- `product_status`
- `created_at`
- `updated_at`

### orders

Represents a customer order. An order owns one or more order items and may be connected to payment and refund records.

Primary key:

- `order_id`

Columns:

- `customer_id`
- `order_status`
- `currency`
- `total_amount`
- `ordered_at`
- `created_at`
- `updated_at`

### order_items

Represents line items under an order.

Primary key:

- `order_item_id`

Columns:

- `order_id`
- `product_id`
- `quantity`
- `unit_price`
- `item_amount`

### payments

Represents payment lifecycle events for an order. Payment changes are captured as row updates in PostgreSQL and then emitted as CDC events.

Primary key:

- `payment_id`

Columns:

- `order_id`
- `payment_status`
- `payment_method`
- `requested_amount`
- `approved_amount`
- `failure_code`
- `failure_message`
- `requested_at`
- `approved_at`
- `captured_at`
- `failed_at`
- `created_at`
- `updated_at`

### refunds

Represents refund lifecycle events for a paid order.

Primary key:

- `refund_id`

Columns:

- `payment_id`
- `order_id`
- `refund_status`
- `refund_amount`
- `reason`
- `requested_at`
- `approved_at`
- `completed_at`
- `rejected_at`
- `created_at`
- `updated_at`

## Domain Relationships

```text
customers 1 -- N orders
orders 1 -- N order_items
products 1 -- N order_items
orders 1 -- N payments
payments 1 -- N refunds
orders 1 -- N refunds
```

The PostgreSQL source schema is defined in `postgres/init.sql`.

## Source CDC Design

All six source tables are CDC targets and have primary keys. `updated_at` is maintained by a shared trigger. `REPLICA IDENTITY FULL` is enabled so Debezium can capture full previous row values for update and delete events.

The publication for Debezium is:

```text
orderflow_publication
```

## Lakehouse Modeling Strategy

Source tables are captured independently through Debezium topics. Bronze keeps table-level CDC events. Silver focuses first on current and history state for orders, payments, and refunds, because those entities carry the main lifecycle transitions. Gold then joins Silver state for analytics.
