-- Запрос для подсчета количества покупателей в таблице user_order_log
SELECT COUNT(DISTINCT customer_id) AS order_log_customer_count
FROM public.user_order_log
WHERE customer_id IS NOT NULL;

-- Запрос для подсчета количества покупателей в таблице user_activity_log
SELECT COUNT(DISTINCT customer_id) AS activity_log_customer_count
FROM public.user_activity_log
WHERE customer_id IS NOT NULL;