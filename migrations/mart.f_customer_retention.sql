DELETE FROM mart.f_customer_retention
WHERE period_id = DATE_PART('week', '{{ds}}'::TIMESTAMP);

WITH temp1 AS (
    SELECT
        fsl.*,
        dc.week_of_year,
        CASE WHEN fsl.payment_amount < 0 THEN 'refunded'
            ELSE 'shipped'
        END AS status
    FROM mart.f_sales fsl
    JOIN mart.d_calendar dc ON dc.date_id = fsl.date_id
    ORDER BY fsl.customer_id
),
new_customer_count AS (
    SELECT
        customer_id,
        week_of_year,
        item_id,
        COUNT(DISTINCT date_id) AS c11
    FROM temp1
    GROUP BY customer_id, week_of_year, item_id
    HAVING COUNT(DISTINCT date_id) = 1
),
new_customer_count2 AS (
    SELECT
        week_of_year,
        item_id,
        COUNT(DISTINCT customer_id) AS new_customers_count
    FROM new_customer_count
    GROUP BY week_of_year, item_id
),
returning_customer_count AS (
    SELECT
        customer_id,
        week_of_year,
        item_id,
        COUNT(DISTINCT date_id) AS returning_customers_count
    FROM temp1
    GROUP BY customer_id, week_of_year, item_id
    HAVING COUNT(DISTINCT date_id) > 1
),
returning_customer_count2 AS (
    SELECT
        week_of_year,
        item_id,
        COUNT(DISTINCT customer_id) AS returning_customers_count
    FROM returning_customer_count
    GROUP BY week_of_year, item_id
),
refunded_customer AS (
    SELECT
        week_of_year,
        item_id,
        COUNT(DISTINCT customer_id) AS refunded_customer_count
    FROM temp1
    WHERE status = 'refunded'
    GROUP BY week_of_year, item_id
),
new_customers_revenue AS (
    SELECT
        nc.week_of_year,
        nc.item_id,
        SUM(temp1.payment_amount) AS new_customers_revenue
    FROM new_customer_count nc
    LEFT JOIN temp1 ON nc.customer_id = temp1.customer_id AND nc.week_of_year = temp1.week_of_year AND nc.item_id = temp1.item_id
    GROUP BY nc.week_of_year, nc.item_id
),
returning_customers_revenue AS (
    SELECT
        rtc.week_of_year,
        rtc.item_id,
        SUM(temp1.payment_amount) AS returning_customers_revenue
    FROM returning_customer_count rtc
    LEFT JOIN temp1 ON rtc.customer_id = temp1.customer_id AND rtc.week_of_year = temp1.week_of_year AND rtc.item_id = temp1.item_id
    GROUP BY rtc.week_of_year, rtc.item_id
),
customer_refunded1 AS (
    SELECT
        customer_id,
        week_of_year,
        item_id,
        COUNT(*) AS c
    FROM temp1
    WHERE temp1.status = 'refunded'
    GROUP BY customer_id, week_of_year, item_id
),
customers_refunded2 AS (
    SELECT
        week_of_year,
        item_id,
        SUM(c) AS customers_refunded
    FROM customer_refunded1
    GROUP BY week_of_year, item_id
)
INSERT INTO mart.f_customer_retention
SELECT DISTINCT
    ncc.new_customers_count,
    rcc.returning_customers_count,
    rc.refunded_customer_count,
    'weekly' AS period_name,
    dcl.week_of_year AS period_id,
    di.item_id,
    ncr.new_customers_revenue,
    rcr.returning_customers_revenue,
    crf.customers_refunded
FROM mart.d_item di
LEFT JOIN temp1 dcl ON 1 = 1
LEFT JOIN new_customer_count2 ncc ON dcl.week_of_year = ncc.week_of_year AND di.item_id = ncc.item_id
LEFT JOIN returning_customer_count2 rcc ON dcl.week_of_year = rcc.week_of_year AND di.item_id = rcc.item_id
LEFT JOIN refunded_customer rc ON dcl.week_of_year = rc.week_of_year AND di.item_id = rc.item_id
LEFT JOIN new_customers_revenue ncr ON dcl.week_of_year = ncr.week_of_year AND di.item_id = ncr.item_id