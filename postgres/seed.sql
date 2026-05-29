BEGIN;

INSERT INTO customers (customer_id, customer_name, email, phone, customer_status, created_at, updated_at)
VALUES
    (1, 'Kim Minjun', 'minjun.kim@example.com', '+82-10-1000-0001', 'ACTIVE', now() - interval '5 days', now() - interval '5 days'),
    (2, 'Lee Seoyeon', 'seoyeon.lee@example.com', '+82-10-1000-0002', 'ACTIVE', now() - interval '4 days', now() - interval '4 days'),
    (3, 'Park Jiho', 'jiho.park@example.com', '+82-10-1000-0003', 'ACTIVE', now() - interval '3 days', now() - interval '3 days');

INSERT INTO products (product_id, sku, product_name, category, unit_price, product_status, created_at, updated_at)
VALUES
    (1, 'SKU-COFFEE-001', 'Cold Brew Coffee', 'beverage', 4500.00, 'ACTIVE', now() - interval '5 days', now() - interval '5 days'),
    (2, 'SKU-BAG-001', 'Reusable Tote Bag', 'goods', 12000.00, 'ACTIVE', now() - interval '5 days', now() - interval '5 days'),
    (3, 'SKU-MUG-001', 'Ceramic Mug', 'goods', 9000.00, 'ACTIVE', now() - interval '5 days', now() - interval '5 days'),
    (4, 'SKU-TEA-001', 'Black Tea Set', 'beverage', 15000.00, 'ACTIVE', now() - interval '5 days', now() - interval '5 days');

INSERT INTO orders (order_id, customer_id, order_status, currency, total_amount, ordered_at, created_at, updated_at)
VALUES
    (1, 1, 'PAID', 'KRW', 21000.00, now() - interval '2 days', now() - interval '2 days', now() - interval '2 days'),
    (2, 2, 'PENDING_PAYMENT', 'KRW', 9000.00, now() - interval '1 day', now() - interval '1 day', now() - interval '1 day'),
    (3, 3, 'REFUNDED', 'KRW', 15000.00, now() - interval '12 hours', now() - interval '12 hours', now() - interval '12 hours');

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, item_amount, created_at, updated_at)
VALUES
    (1, 1, 1, 2, 4500.00, 9000.00, now() - interval '2 days', now() - interval '2 days'),
    (2, 1, 2, 1, 12000.00, 12000.00, now() - interval '2 days', now() - interval '2 days'),
    (3, 2, 3, 1, 9000.00, 9000.00, now() - interval '1 day', now() - interval '1 day'),
    (4, 3, 4, 1, 15000.00, 15000.00, now() - interval '12 hours', now() - interval '12 hours');

INSERT INTO payments (
    payment_id,
    order_id,
    payment_status,
    payment_method,
    requested_amount,
    approved_amount,
    failure_code,
    failure_message,
    requested_at,
    approved_at,
    captured_at,
    failed_at,
    created_at,
    updated_at
)
VALUES
    (
        1, 1, 'CAPTURED', 'CARD', 21000.00, 21000.00, NULL, NULL,
        now() - interval '2 days', now() - interval '2 days' + interval '1 minute',
        now() - interval '2 days' + interval '2 minutes', NULL,
        now() - interval '2 days', now() - interval '2 days' + interval '2 minutes'
    ),
    (
        2, 2, 'REQUESTED', 'CARD', 9000.00, NULL, NULL, NULL,
        now() - interval '1 day', NULL, NULL, NULL,
        now() - interval '1 day', now() - interval '1 day'
    ),
    (
        3, 3, 'CAPTURED', 'BANK_TRANSFER', 15000.00, 15000.00, NULL, NULL,
        now() - interval '12 hours', now() - interval '12 hours' + interval '1 minute',
        now() - interval '12 hours' + interval '3 minutes', NULL,
        now() - interval '12 hours', now() - interval '12 hours' + interval '3 minutes'
    );

INSERT INTO refunds (
    refund_id,
    payment_id,
    order_id,
    refund_status,
    refund_amount,
    reason,
    requested_at,
    approved_at,
    completed_at,
    rejected_at,
    created_at,
    updated_at
)
VALUES
    (
        1, 3, 3, 'COMPLETED', 15000.00, 'Customer return completed',
        now() - interval '6 hours', now() - interval '6 hours' + interval '3 minutes',
        now() - interval '6 hours' + interval '10 minutes', NULL,
        now() - interval '6 hours', now() - interval '6 hours' + interval '10 minutes'
    );

SELECT setval(pg_get_serial_sequence('customers', 'customer_id'), (SELECT max(customer_id) FROM customers));
SELECT setval(pg_get_serial_sequence('products', 'product_id'), (SELECT max(product_id) FROM products));
SELECT setval(pg_get_serial_sequence('orders', 'order_id'), (SELECT max(order_id) FROM orders));
SELECT setval(pg_get_serial_sequence('order_items', 'order_item_id'), (SELECT max(order_item_id) FROM order_items));
SELECT setval(pg_get_serial_sequence('payments', 'payment_id'), (SELECT max(payment_id) FROM payments));
SELECT setval(pg_get_serial_sequence('refunds', 'refund_id'), (SELECT max(refund_id) FROM refunds));

COMMIT;
