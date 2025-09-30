/*
===============================================================================
Customer Report
===============================================================================

Purpose:
  - This report consolidates key customer metrics and behaviors

Highlights:
  1. Gathers essential fields such as names, ages, and transaction details.
  2. Segments customers into categories (VIP, Regular, New) and age groups.
  3. Aggregates customer-level metrics:
     - total orders
     - total sales
     - total quantity purchased
     - total products
     - lifespan (in months)
  4. Calculates valuable KPIs:
     - recency (months since last order)
     - average order value
     - average monthly spend

===============================================================================
*/

CREATE VIEW gold.report_customers AS
/*
===============================================================================
Base query: Retrieves core columns from tables
===============================================================================
*/
WITH base_query AS (
    SELECT --Select the columns that may be necessary for executing the different measures and KPIs required.
    f.order_number,
    f.product_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    c.customer_key,
    c.customer_number,
    CONCAT(c.first_name,' ',c.last_name) as full_name,
    DATEDIFF(YEAR, c.birthdate,GETDATE()) AS age
    FROM gold.fact_sales as f
    LEFT JOIN gold.dim_customers as c
    ON c.customer_key = f.customer_key
    WHERE -- Filter the NULLs or other not desired data
    f.order_date IS NOT NULL
),
/*
===============================================================
Aggregations query: Calculate every aggregation required
===============================================================
*/
agg_query AS (
    SELECT 
    customer_key,
    customer_number,
    full_name,
    age,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS quantity_purchased,
    COUNT(DISTINCT product_key) products_purchased,
    MAX(order_date) AS last_order_date,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
    FROM base_query
    GROUP BY customer_key, customer_number, full_name,age
)

/*
=================================================================
Final report query: Get all the relevant columns and measurements, and calculate the final KPIs
=================================================================
*/
SELECT
customer_key,
customer_number,
full_name,
age,
CASE
	WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
	WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
	ELSE 'New'
END AS cust_group,
CASE
    WHEN age < 20 THEN 'Under 20'
    WHEN age BETWEEN 20 AND 29 THEN '20-29'
    WHEN age BETWEEN 20 AND 39 THEN '30-39'
    WHEN age BETWEEN 20 AND 29 THEN '40-49'
    ELSE '50 and above'
END AS age_range,
total_orders,
total_sales,
quantity_purchased,
products_purchased,
last_order_date,
lifespan,
DATEDIFF(MONTH,last_order_date,GETDATE())AS recency,
ISNULL(total_sales / NULLIF(total_orders,0),0) AS avg_order_value,
ISNULL(total_sales / NULLIF(lifespan,0),0) AS avg_monthly_spent
FROM agg_query;










