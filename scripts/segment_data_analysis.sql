-- Segment products into cost ranges and count how many products fall into each segment
WITH cost_ranges AS (
	SELECT
	product_key,
	product_name,
	cost,
	CASE
		WHEN cost < 100 THEN 'Below 100'
		WHEN cost BETWEEN 100 AND 500 THEN '100 - 500'
		WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
		WHEN cost BETWEEN 500 AND 1000 THEN '1000-1500'
		WHEN cost BETWEEN 500 AND 1000 THEN '1500-2000'
		ELSE 'Over 2000'
	END AS cost_range
	FROM gold.dim_products
	)

SELECT 
cost_range,
COUNT(product_key) AS products_count
FROM cost_ranges
GROUP BY cost_range
ORDER BY products_count DESC;

/*
Group customers into three segments based on their spending behavior
	VIP: At least 12 months of history and spending more than 5000
	Regular: At least 12 months of history and spending less than 5000
	New: Lifespan less than 12 months
*/
WITH customer_groups AS (
	SELECT
	f.customer_key,
	CONCAT(c.first_name,' ',c.last_name) AS full_name,
	SUM(f.sales_amount) AS total_spent,
	DATEDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) AS history_months,
	CASE
		WHEN DATEDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) >= 12 AND SUM(f.sales_amount) > 5000 THEN 'VIP'
		WHEN DATEDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) >= 12 AND SUM(f.sales_amount) <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS cust_group
	FROM gold.fact_sales AS f
	LEFT JOIN gold.dim_customers AS c
	ON f.customer_key = c.customer_key
	GROUP BY f.customer_key, CONCAT(c.first_name,' ',c.last_name)
)

SELECT
cust_group,
SUM(total_spent) AS total_spent,
COUNT(customer_key) AS total_customers
FROM customer_groups
GROUP BY cust_group
ORDER BY total_customers DESC;

