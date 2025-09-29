-- Key Business Metrics
-- Total orders, revenue, and customer metrics

SELECT
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(SUM(payment_value)::numeric, 2) as total_revenue,
    ROUND(AVG(payment_value)::numeric, 2) as avg_order_value,
    ROUND(AVG(review_score)::numeric, 2) as avg_rating
FROM ecommerce.fact_orders;

-- Monthly Revenue Trend
SELECT
    TO_CHAR(order_purchase_timestamp, 'YYYY-MM') as month,
    COUNT(DISTINCT order_id) as orders,
    ROUND(SUM(payment_value)::numeric, 2) as revenue
FROM ecommerce.fact_orders
GROUP BY month
ORDER BY month;

-- Top Product Categories
SELECT
    p.product_category_name,
    COUNT(DISTINCT f.order_id) as orders,
    ROUND(SUM(f.payment_value)::numeric, 2) as revenue
FROM ecommerce.fact_orders f
JOIN ecommerce.dim_products p ON f.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10;

-- Customer Segmentation (RFM)
WITH customer_rfm AS (
    SELECT
        customer_id,
        CURRENT_DATE - MAX(order_purchase_timestamp::date) as recency,
        COUNT(DISTINCT order_id) as frequency,
        SUM(payment_value) as monetary
    FROM ecommerce.fact_orders
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency) as f_score,
        NTILE(5) OVER (ORDER BY monetary) as m_score
    FROM customer_rfm
)
SELECT
    CASE
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal'
        WHEN r_score >= 3 AND f_score <= 2 THEN 'Potential'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        ELSE 'Lost'
    END as segment,
    COUNT(*) as customers,
    ROUND(AVG(monetary)::numeric, 2) as avg_revenue
FROM rfm_scores
GROUP BY segment
ORDER BY customers DESC;
