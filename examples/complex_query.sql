-- E-commerce analytics pipeline: customer lifetime value, cohort analysis,
-- product affinity, fraud scoring, and executive dashboard metrics.
-- Targets Snowflake but uses broadly compatible SQL.

WITH raw_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        o.channel,
        o.shipping_country,
        o.discount_code,
        o.total_amount,
        o.tax_amount,
        o.shipping_cost,
        o.created_at,
        DATEDIFF('day', o.created_at, o.order_date) AS processing_lag_days
    FROM warehouse.orders o
    WHERE o.order_date >= '2023-01-01'
      AND o.status NOT IN ('test', 'internal', 'cancelled_before_payment')
),

order_items AS (
    SELECT
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.discount_amount,
        oi.quantity * oi.unit_price - oi.discount_amount AS net_line_total,
        p.category_l1,
        p.category_l2,
        p.brand,
        p.supplier_id,
        p.cost_price,
        (oi.quantity * oi.unit_price - oi.discount_amount)
            - (oi.quantity * p.cost_price) AS gross_margin
    FROM warehouse.order_items oi
    JOIN warehouse.products p ON oi.product_id = p.product_id
),

enriched_orders AS (
    SELECT
        ro.*,
        COUNT(oi.product_id)          AS item_count,
        SUM(oi.net_line_total)         AS net_revenue,
        SUM(oi.gross_margin)           AS total_margin,
        SUM(oi.quantity)               AS total_units,
        COUNT(DISTINCT oi.category_l1) AS distinct_categories,
        ARRAY_AGG(DISTINCT oi.category_l1) AS categories_purchased
    FROM raw_orders ro
    JOIN order_items oi ON ro.order_id = oi.order_id
    GROUP BY ro.order_id, ro.customer_id, ro.order_date, ro.status,
             ro.channel, ro.shipping_country, ro.discount_code,
             ro.total_amount, ro.tax_amount, ro.shipping_cost,
             ro.created_at, ro.processing_lag_days
),

customer_first_order AS (
    SELECT
        customer_id,
        MIN(order_date)                       AS first_order_date,
        DATE_TRUNC('month', MIN(order_date))  AS cohort_month,
        MIN(channel)                          AS acquisition_channel
    FROM enriched_orders
    GROUP BY customer_id
),

customer_order_sequences AS (
    SELECT
        eo.customer_id,
        eo.order_id,
        eo.order_date,
        eo.net_revenue,
        eo.total_margin,
        eo.channel,
        cfo.first_order_date,
        cfo.cohort_month,
        cfo.acquisition_channel,
        ROW_NUMBER() OVER (PARTITION BY eo.customer_id ORDER BY eo.order_date) AS order_seq,
        DATEDIFF('day', cfo.first_order_date, eo.order_date)                  AS days_since_first,
        LAG(eo.order_date) OVER (PARTITION BY eo.customer_id ORDER BY eo.order_date) AS prev_order_date,
        DATEDIFF('day',
            LAG(eo.order_date) OVER (PARTITION BY eo.customer_id ORDER BY eo.order_date),
            eo.order_date
        ) AS days_between_orders
    FROM enriched_orders eo
    JOIN customer_first_order cfo ON eo.customer_id = cfo.customer_id
),

customer_lifetime_stats AS (
    SELECT
        customer_id,
        cohort_month,
        acquisition_channel,
        COUNT(*)                                     AS lifetime_orders,
        SUM(net_revenue)                             AS lifetime_revenue,
        SUM(total_margin)                            AS lifetime_margin,
        AVG(net_revenue)                             AS avg_order_value,
        MAX(order_date)                              AS last_order_date,
        DATEDIFF('day', MIN(order_date), MAX(order_date)) AS customer_tenure_days,
        AVG(days_between_orders)                     AS avg_days_between_orders,
        STDDEV(days_between_orders)                  AS stddev_days_between
    FROM customer_order_sequences
    GROUP BY customer_id, cohort_month, acquisition_channel
),

customer_segments AS (
    SELECT
        cls.*,
        c.email,
        c.name             AS customer_name,
        c.country           AS customer_country,
        c.signup_date,
        CASE
            WHEN lifetime_orders >= 10 AND lifetime_revenue >= 5000 THEN 'vip'
            WHEN lifetime_orders >= 5  AND lifetime_revenue >= 1500 THEN 'loyal'
            WHEN lifetime_orders >= 2  AND lifetime_revenue >= 300  THEN 'active'
            WHEN lifetime_orders = 1                                THEN 'one_time'
            ELSE 'low_value'
        END AS customer_segment,
        CASE
            WHEN DATEDIFF('day', last_order_date, CURRENT_DATE()) <= 30  THEN 'hot'
            WHEN DATEDIFF('day', last_order_date, CURRENT_DATE()) <= 90  THEN 'warm'
            WHEN DATEDIFF('day', last_order_date, CURRENT_DATE()) <= 180 THEN 'cooling'
            ELSE 'churned'
        END AS recency_tier,
        NTILE(10) OVER (ORDER BY lifetime_revenue DESC) AS revenue_decile
    FROM customer_lifetime_stats cls
    JOIN warehouse.customers c ON cls.customer_id = c.customer_id
),

cohort_retention AS (
    SELECT
        cfo.cohort_month,
        DATE_TRUNC('month', eo.order_date)                              AS activity_month,
        DATEDIFF('month', cfo.cohort_month, DATE_TRUNC('month', eo.order_date)) AS months_since_cohort,
        COUNT(DISTINCT eo.customer_id)                                   AS active_customers,
        SUM(eo.net_revenue)                                              AS cohort_revenue
    FROM enriched_orders eo
    JOIN customer_first_order cfo ON eo.customer_id = cfo.customer_id
    GROUP BY cfo.cohort_month, DATE_TRUNC('month', eo.order_date),
             DATEDIFF('month', cfo.cohort_month, DATE_TRUNC('month', eo.order_date))
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM customer_first_order
    GROUP BY cohort_month
),

cohort_retention_rates AS (
    SELECT
        cr.cohort_month,
        cr.activity_month,
        cr.months_since_cohort,
        cr.active_customers,
        cr.cohort_revenue,
        cs.cohort_size,
        ROUND(cr.active_customers * 100.0 / cs.cohort_size, 2) AS retention_pct,
        ROUND(cr.cohort_revenue / cr.active_customers, 2)       AS revenue_per_active
    FROM cohort_retention cr
    JOIN cohort_sizes cs ON cr.cohort_month = cs.cohort_month
),

product_affinity_pairs AS (
    SELECT
        a.product_id   AS product_a,
        b.product_id   AS product_b,
        a.category_l1  AS category_a,
        b.category_l1  AS category_b,
        COUNT(DISTINCT a.order_id)                         AS co_occurrence_count,
        COUNT(DISTINCT a.order_id) * 1.0
            / COUNT(DISTINCT a.order_id) OVER (PARTITION BY a.product_id) AS confidence_a_to_b
    FROM order_items a
    JOIN order_items b
        ON a.order_id = b.order_id
        AND a.product_id < b.product_id
    GROUP BY a.product_id, b.product_id, a.category_l1, b.category_l1
    HAVING COUNT(DISTINCT a.order_id) >= 5
),

top_product_pairs AS (
    SELECT
        pap.*,
        pa.brand AS brand_a,
        pb.brand AS brand_b,
        ROW_NUMBER() OVER (
            PARTITION BY pap.category_a
            ORDER BY pap.co_occurrence_count DESC
        ) AS rank_in_category
    FROM product_affinity_pairs pap
    JOIN warehouse.products pa ON pap.product_a = pa.product_id
    JOIN warehouse.products pb ON pap.product_b = pb.product_id
    WHERE pap.confidence_a_to_b >= 0.05
),

supplier_performance AS (
    SELECT
        oi.supplier_id,
        s.supplier_name,
        s.region AS supplier_region,
        COUNT(DISTINCT oi.order_id)                AS orders_fulfilled,
        SUM(oi.quantity)                            AS total_units_sold,
        SUM(oi.net_line_total)                      AS total_revenue,
        SUM(oi.gross_margin)                        AS total_margin,
        ROUND(SUM(oi.gross_margin) * 100.0
              / NULLIF(SUM(oi.net_line_total), 0), 2) AS margin_pct,
        AVG(oi.gross_margin / NULLIF(oi.net_line_total, 0)) AS avg_item_margin_pct,
        COUNT(DISTINCT oi.product_id)              AS products_sold,
        COUNT(DISTINCT CASE WHEN ro.status = 'returned' THEN ro.order_id END) AS return_orders,
        ROUND(COUNT(DISTINCT CASE WHEN ro.status = 'returned' THEN ro.order_id END) * 100.0
              / NULLIF(COUNT(DISTINCT oi.order_id), 0), 2) AS return_rate_pct
    FROM order_items oi
    JOIN raw_orders ro ON oi.order_id = ro.order_id
    JOIN warehouse.suppliers s ON oi.supplier_id = s.supplier_id
    GROUP BY oi.supplier_id, s.supplier_name, s.region
),

fraud_risk_signals AS (
    SELECT
        eo.order_id,
        eo.customer_id,
        eo.order_date,
        eo.net_revenue,
        eo.channel,
        eo.shipping_country,
        cs.customer_segment,
        cs.recency_tier,
        cs.avg_order_value  AS customer_avg_order_value,
        -- Signal: order value deviation
        CASE
            WHEN eo.net_revenue > cs.avg_order_value * 3 THEN 3
            WHEN eo.net_revenue > cs.avg_order_value * 2 THEN 2
            ELSE 0
        END AS value_deviation_score,
        -- Signal: shipping country mismatch
        CASE
            WHEN eo.shipping_country != cs.customer_country THEN 2
            ELSE 0
        END AS geo_mismatch_score,
        -- Signal: new customer + high value
        CASE
            WHEN cs.customer_segment = 'one_time' AND eo.net_revenue > 500 THEN 3
            ELSE 0
        END AS new_high_value_score,
        -- Signal: rapid repeat ordering
        CASE
            WHEN cos.days_between_orders IS NOT NULL
                 AND cos.days_between_orders < 1
                 AND eo.net_revenue > 200 THEN 3
            ELSE 0
        END AS rapid_repeat_score,
        eo.discount_code
    FROM enriched_orders eo
    JOIN customer_segments cs ON eo.customer_id = cs.customer_id
    LEFT JOIN customer_order_sequences cos
        ON eo.order_id = cos.order_id
),

fraud_scored_orders AS (
    SELECT
        frs.*,
        -- Signal: heavy discount abuse
        CASE
            WHEN frs.discount_code IS NOT NULL
                 AND frs.net_revenue > frs.customer_avg_order_value * 1.5 THEN 2
            ELSE 0
        END AS discount_abuse_score,
        (frs.value_deviation_score
         + frs.geo_mismatch_score
         + frs.new_high_value_score
         + frs.rapid_repeat_score
         + CASE
             WHEN frs.discount_code IS NOT NULL
                  AND frs.net_revenue > frs.customer_avg_order_value * 1.5 THEN 2
             ELSE 0
           END
        ) AS total_risk_score,
        CASE
            WHEN (frs.value_deviation_score + frs.geo_mismatch_score
                  + frs.new_high_value_score + frs.rapid_repeat_score
                  + CASE WHEN frs.discount_code IS NOT NULL
                              AND frs.net_revenue > frs.customer_avg_order_value * 1.5
                         THEN 2 ELSE 0 END) >= 6 THEN 'high'
            WHEN (frs.value_deviation_score + frs.geo_mismatch_score
                  + frs.new_high_value_score + frs.rapid_repeat_score
                  + CASE WHEN frs.discount_code IS NOT NULL
                              AND frs.net_revenue > frs.customer_avg_order_value * 1.5
                         THEN 2 ELSE 0 END) >= 3 THEN 'medium'
            ELSE 'low'
        END AS risk_tier
    FROM fraud_risk_signals frs
),

monthly_exec_summary AS (
    SELECT
        DATE_TRUNC('month', eo.order_date)            AS report_month,
        COUNT(DISTINCT eo.order_id)                     AS total_orders,
        COUNT(DISTINCT eo.customer_id)                  AS unique_customers,
        COUNT(DISTINCT CASE WHEN cos.order_seq = 1
                            THEN eo.customer_id END)    AS new_customers,
        SUM(eo.net_revenue)                             AS gross_revenue,
        SUM(eo.total_margin)                            AS gross_margin,
        ROUND(SUM(eo.total_margin) * 100.0
              / NULLIF(SUM(eo.net_revenue), 0), 2)     AS margin_pct,
        AVG(eo.net_revenue)                             AS avg_order_value,
        SUM(CASE WHEN fso.risk_tier = 'high'
                 THEN eo.net_revenue ELSE 0 END)        AS flagged_revenue,
        COUNT(DISTINCT CASE WHEN fso.risk_tier = 'high'
                            THEN eo.order_id END)       AS flagged_orders,
        COUNT(DISTINCT eo.channel)                      AS channels_active,
        SUM(eo.total_units)                             AS total_units_shipped
    FROM enriched_orders eo
    JOIN customer_order_sequences cos ON eo.order_id = cos.order_id
    LEFT JOIN fraud_scored_orders fso ON eo.order_id = fso.order_id
    GROUP BY DATE_TRUNC('month', eo.order_date)
)

SELECT
    mes.report_month,
    mes.total_orders,
    mes.unique_customers,
    mes.new_customers,
    mes.gross_revenue,
    mes.gross_margin,
    mes.margin_pct,
    mes.avg_order_value,
    mes.flagged_revenue,
    mes.flagged_orders,
    mes.total_units_shipped,
    crr.retention_pct          AS cohort_m1_retention,
    crr.revenue_per_active     AS cohort_m1_rev_per_active
FROM monthly_exec_summary mes
LEFT JOIN cohort_retention_rates crr
    ON crr.cohort_month = mes.report_month
    AND crr.months_since_cohort = 1
ORDER BY mes.report_month DESC
