DELETE FROM mart.f_customer_retention
WHERE period_id = DATE_PART('week', '{{ds}}'::TIMESTAMP);

WITH customers AS (
    SELECT
        fsl. * ,
        dc.week_of_year,
        CASE WHEN fsl.payment_amount < 0 THEN 'refunded'
            ELSE 'shipped'
        END AS status
    FROM mart.f_sales fsl
    JOIN mart.d_calendar dc ON dc.date_id = fsl.date_id
),
customer_data AS (
    SELECT 
        customer_id, 
        week_of_year, 
        item_id,
        CASE 
            WHEN status = 'shipped' AND COUNT(DISTINCT date_id) = 1 THEN 'new_customer'
            WHEN status = 'shipped' AND COUNT(DISTINCT date_id) > 1 THEN 'returning_customer'
            WHEN status = 'refunded' THEN 'refunded_customer'
        END AS customer_type,
        payment_amount
    FROM customers
    GROUP BY customer_id, week_of_year, item_id, status, payment_amount
),
revenue_data AS (
    SELECT 
        week_of_year, 
        item_id, 
        customer_type,
        SUM(payment_amount) AS revenue
    FROM customer_data
    WHERE status = 'shipped'
    GROUP BY week_of_year, item_id, customer_type
)
INSERT INTO mart.f_customer_retention (new_customers_count, returning_customers_count, refunded_customer_count, period_name, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
SELECT 
    COUNT(DISTINCT CASE WHEN customer_type = 'new_customer' THEN customer_id END) AS new_customers_count,
    COUNT(DISTINCT CASE WHEN customer_type = 'returning_customer' THEN customer_id END) AS returning_customers_count,
    COUNT(DISTINCT CASE WHEN customer_type = 'refunded_customer' THEN customer_id END) AS refunded_customer_count,
    'weekly' AS period_name,
    week_of_year,
    item_id,
    COALESCE(SUM(CASE WHEN customer_type = 'new_customer' THEN revenue END), 0) AS new_customers_revenue,
    COALESCE(SUM(CASE WHEN customer_type = 'returning_customer' THEN revenue END), 0) AS returning_customers_revenue,
    COALESCE(SUM(CASE WHEN customer_type = 'refunded_customer' THEN revenue END), 0) AS customers_refunded
FROM customer_data
LEFT JOIN revenue_data USING (week_of_year, item_id, customer_type)
GROUP BY week_of_year, item_id;