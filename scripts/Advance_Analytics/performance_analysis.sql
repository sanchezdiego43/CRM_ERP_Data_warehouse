-- Yearly performance of products compared with average sales performance and previous year sales.
WITH yearly_product_sales AS (  --CTE for querying later
SELECT 
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY YEAR(f.order_date), p.product_name
)

SELECT
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales, -- Get the Avg per Product
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg_current, --Calculate the difference
CASE
	WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Over the Average'
	WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Under the Average'
	ELSE 'Same as the average'
END AS avg_change,
LAG (current_sales,1) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_sales, --Get the previous value
current_sales - LAG (current_sales,1) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_prev_current, --Calculate the difference
CASE
	WHEN current_sales - LAG (current_sales,1) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Over Last Year'
	WHEN current_sales - LAG (current_sales,1) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Under Last Year'
	ELSE 'Same as Last Year'
END AS prev_change
FROM yearly_product_sales
ORDER BY product_name,order_year;