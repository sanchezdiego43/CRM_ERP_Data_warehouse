/*
===============================================================================
Product Report
===============================================================================

Purpose:
  - This report consolidates key product metrics and behaviors.

Highlights:
  1. Gathers essential fields such as product name, category, subcategory, and cost.
  2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
  3. Aggregates product-level metrics:
     - total orders
     - total sales
     - total quantity sold
     - total customers (unique)
     - lifespan (in months)
  4. Calculates valuable KPIs:
     - recency (months since last sale)
     - average order revenue (AOR)
     - average monthly revenue

===============================================================================
*/

CREATE VIEW gold.report_products AS
/*
===============================================================================
Base query: Retrieves core columns from tables
===============================================================================
*/
WITH base_query AS (
    SELECT
    f.order_number,
    f.customer_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    p.product_key,
    p.product_name,
    p.category,
    p.subcategory,
    p.cost
    FROM gold.fact_sales AS f
    LEFT JOIN gold.dim_products as p
    ON f.product_key = p.product_key
    ),

/*
===============================================================================
Aggregations query: Calculate every aggregation required
===============================================================================
*/
agg_query AS (
    SELECT 
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT customer_key) AS total_customers,
    DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS lifespan,
    MAX(order_date) AS last_order_date
    FROM base_query
    GROUP BY  product_key,product_name,category,subcategory,cost
)

/*
=================================================================
Final report query: Get all the relevant columns and measurements, and calculate the final KPIs
=================================================================
*/

SELECT
product_key,
product_name,
category,
subcategory,
cost,
CASE 
    WHEN total_sales > 50000 THEN 'High Performer'
    WHEN total_sales >= 10000 THEN 'Mid-Range'
    ELSE 'Low-performer'
END AS product_performance,
total_orders,
total_sales,
total_quantity,
total_customers,
lifespan,
DATEDIFF(MONTH,last_order_date,GETDATE()) AS recency,
ISNULL(total_sales / NULLIF(total_orders,0),0) AS avg_order_revenue,
ISNULL(total_sales / NULLIF(lifespan,0),0) AS avg_monthly_revenue
FROM agg_query;