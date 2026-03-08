WITH raw_orders AS (
    SELECT order_id, customer_id, amount, status, order_date
    FROM warehouse.orders
    WHERE order_date >= '2024-01-01'
),
valid_orders AS (
    SELECT * FROM raw_orders
    WHERE status IN ('completed', 'shipped')
),
customer_totals AS (
    SELECT
        customer_id,
        SUM(amount) AS total_revenue,
        COUNT(*) AS order_count
    FROM valid_orders
    GROUP BY customer_id
),
enriched_customers AS (
    SELECT
        ct.*,
        c.name,
        c.segment
    FROM customer_totals ct
    JOIN warehouse.customers c ON ct.customer_id = c.customer_id
)
SELECT * FROM enriched_customers WHERE total_revenue > 1000
