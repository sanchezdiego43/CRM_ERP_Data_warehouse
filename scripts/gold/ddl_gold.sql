/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================

Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- CUSTOMERS BUSINESS OBJECT CREATION
-- The GOLD LAYER starts from the base of the SILVER LAYER
-- ALWAYS START QUERYING FROM THE MASTER TABLE OF THE BUSINESS OBJECT
CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_fistname AS first_name,
ci.cst_lastname AS last_name,
la.CNTRY AS country,
CASE
	WHEN ci.cst_gnder != 'n/a' THEN ci.cst_gnder
	ELSE COALESCE(ca.GEN,'n/a')
END AS genre,
ci.cst_marital_status AS marital_status,
ca.BDATE AS birthdate,
ci.cst_create_date AS creation_date
FROM silver.crm_cust_info AS ci  --ci for Cust Info
LEFT JOIN silver.erp_CUST_AZ12 AS ca --ca for CUST AS12
ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_LOC_A101 AS la --la for LOC A101}
ON ci.cst_key = la.CID;

-- PRODUCT BUSINESS OBJECT CREATION
-- The GOLD LAYER starts from the base of the SILVER LAYER
-- ALWAYS START QUERYING FROM THE MASTER TABLE OF THE BUSINESS OBJECT
DROP VIEW IF EXISTS gold.dim_product;

CREATE VIEW gold.dim_product AS
SELECT 
ROW_NUMBER() OVER(ORDER BY ppi.prd_id) AS product_key, 
ppi.prd_id AS product_id,
ppi.prd_key AS product_number,
ppi.prd_nm AS product_name,
ppi.category_id AS category_id,
pcg.CAT AS category,
pcg.SUBCAT AS subcategory,
pcg.MAINTENANCE AS maintenance,
ppi.prd_cost AS product_cost,
ppi.prd_line AS product_line,
ppi.prd_start_dt AS start_date
FROM silver.crm_prd_info AS ppi
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pcg
ON ppi.category_id = pcg.ID
WHERE ppi.prd_end_dt IS NULL; --Filter historical data


-- SALES BUSINESS OBJECT CREATION
-- The GOLD LAYER starts from the base of the SILVER LAYER
-- ALWAYS START QUERYING FROM THE MASTER TABLE OF THE BUSINESS OBJECT
CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_number,
dp.product_key,
dc.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS ship_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customers AS dc
ON sd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_product AS dp
ON sd.sls_prd_key = dp.product_key;
