SELECT
MONTH(order_date) AS "order_month",
SUM(sales_amount) AS "total_sales",
COUNT(DISTINCT customer_key) AS "total_clients",
SUM(quantity) AS "total_quantity"
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date);

SELECT
DATETRUNC(month, order_date) AS "date",
SUM(sales_amount) AS "total_sales",
COUNT(DISTINCT customer_key) AS "total_clients",
SUM(quantity) AS "total_quantity"
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date);