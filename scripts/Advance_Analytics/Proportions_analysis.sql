--Proportions test
WITH category_sales AS (  --CTE to get the total sales per category
	SELECT
	p.category AS category,
	SUM(f.sales_amount) AS total_sales
	FROM gold.fact_sales AS f
	LEFT JOIN gold.dim_products AS p
	ON f.product_key = p.product_key
	GROUP BY p.category
)

SELECT 
category,
total_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100,2),'%') AS prop_sales --Calculate the sum of the whole categories and get the proportion
FROM category_sales
ORDER BY total_sales DESC;