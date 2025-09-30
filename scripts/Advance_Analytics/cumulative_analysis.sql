--Total sales per month and running total of sales over time
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (
	PARTITION BY DATETRUNC(year, order_date) --Reset every year
	ORDER BY order_date --Running total for sales over time
	) AS running_sales_total 
FROM 
(
	SELECT
	DATETRUNC(month, order_date) AS order_date,
	SUM(sales_amount) AS total_sales
	FROM gold.fact_sales
	WHERE DATETRUNC(month, order_date) IS NOT NULL
	GROUP BY DATETRUNC(month, order_date)
	) AS sales_analysis;	