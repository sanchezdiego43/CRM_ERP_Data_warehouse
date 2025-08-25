--DIMENSIONS EXPLORATION
SELECT DISTINCT country FROM gold.dim_customers;

SELECT DISTINCT product_name FROM gold.dim_products;
SELECT DISTINCT category FROM gold.dim_products;
SELECT DISTINCT subcategory FROM gold.dim_products;
SELECT DISTINCT product_line FROM gold.dim_products;

--There are related, and hierarchical fields, as:
SELECT DISTINCT category, subcategory, product_name FROM gold.dim_products
ORDER BY 1,2,3;

--DATE EXPLORATION
SELECT
	MIN(order_date) AS min_orderdate,
	MAX(order_date) AS max_orderdate,
	DATEDIFF(year, MIN(order_date),MAX(order_date)) AS order_date_range,
	MIN(due_date) AS min_due_date,
	MAX(due_date) AS max_duedate,
	DATEDIFF(year, MIN(due_date),MAX(due_date)) AS due_date_range,
	MIN(shipping_date) AS min_shipping_date,
	MAX(shipping_date) AS max_shippingdate,
	DATEDIFF(year, MIN(shipping_date),MAX(shipping_date)) AS shipping_date_range
FROM gold.fact_sales;

SELECT
	MIN(birthdate) AS Oldest_cust,
	DATEDIFF(year,MIN(birthdate),GETDATE()) AS oldest_cust_age,
	MAX(birthdate) AS Youngest_cust,
	DATEDIFF(year,MAX(birthdate),GETDATE()) AS youngest_cust_age
FROM gold.dim_customers;

--MEASURES AGGREGATION
--Find total sales
--Find how many items are sold
--Find average order price
--Find the total number of Orders
--Find the total number of products
--Find the total number of customers
--Find the total number of customers that has placed an orde

SELECT 
	SUM(fs.sales_amount) AS total_sales,
	SUM(fs.quantity) AS items_sold,
	AVG(fs.price) AS average_price,
	COUNT(DISTINCT fs.order_number) AS total_orders,
	COUNT(DISTINCT p.product_key) AS total_products,
	COUNT(DISTINCT c.customer_key) AS total_customers,
	COUNT(DISTINCT fs.customer_key) AS total_customer_ordered
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_products AS p
ON fs.product_key=p.product_key
LEFT JOIN gold.dim_customers AS c
ON fs.customer_key=c.customer_key;

--MAGNITUDE ANALYSIS

--Find total customers by countries
SELECT 
	country,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

--Find total customers by gender
SELECT 
	genre,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY genre
ORDER BY total_customers DESC;

--Find total product by category
SELECT 
	category,
	COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;


--What is the AVG cost by category
SELECT 
	category,
	AVG(product_cost) AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC;


--What is the total revenue for each category
SELECT 
	dp.category,
	SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_products AS dp
ON fs.product_key=dp.product_key
GROUP BY category
ORDER BY total_revenue DESC;

--Find total revenue per customer
SELECT 
	dc.customer_id,
	CONCAT(dc.first_name,' ',dc.last_name) AS customer_name,
	SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customers AS dc
ON fs.customer_key=dc.customer_key
GROUP BY customer_id, CONCAT(dc.first_name,' ',dc.last_name)
ORDER BY total_revenue DESC;

--Distribution of sold items across countries
SELECT 
	dc.country,
	SUM(fs.quantity) AS total_items
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customers AS dc
ON fs.customer_key=dc.customer_key
GROUP BY dc.country
ORDER BY total_items DESC;

--RANKING ANALYSIS
--Which 5 product generate the highest revenue?
SELECT TOP 5
	dp.product_name,
	SUM(fs.sales_amount) AS total_revenue,
	RANK() OVER (ORDER BY SUM(fs.sales_amount) DESC) AS Ranking
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_products AS dp
ON fs.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY total_revenue DESC;
 


--What are the 5 worst-performing products in terms of sales?
SELECT TOP 5
	dp.product_name,
	SUM(fs.sales_amount) AS total_revenue,
	RANK() OVER (ORDER BY SUM(fs.sales_amount)) AS Ranking
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_products AS dp
ON fs.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY total_revenue ;

--Find top 10 customer who have generated the most revenue
SELECT TOP 10
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	SUM(fs.sales_amount) AS total_revenue,
	RANK() OVER (ORDER BY SUM(fs.sales_amount) DESC) AS Ranking
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customers AS dc
ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_key, dc.first_name, dc.last_name
ORDER BY total_revenue DESC;

--Find the 3 that have placed less orders

SELECT TOP 3
	dc.customer_key,
	dc.first_name,
	dc.last_name,
	COUNT(DISTINCT order_number) AS total_orders,
	RANK() OVER (ORDER BY COUNT(DISTINCT order_number)) AS Ranking
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customers AS dc
ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_key, dc.first_name, dc.last_name
ORDER BY total_orders;
