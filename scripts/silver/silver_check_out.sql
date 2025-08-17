--=======================================
--Checking for crm_cust_info
--=======================================

--Check for Nulls and Duplicates in Primary KEY
-- Expected: No results
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--Check for unwanted spaces
--Expected: No results
SELECT * FROM silver.crm_cust_info
WHERE cst_gnder != TRIM(cst_gnder);
-- cst_firstname and cst_lastname require a TRIM transformation

--Check for unwanted entries
--Expected: Just F / M
SELECT DISTINCT cst_gnder
FROM silver.crm_cust_info;

--Check for unwanted entries
--Expected: Just S / M
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;


--=======================================
--Checking for crm_prd_info
--=======================================

--Check for Nulls and Duplicates in Primary KEY
-- Expected: No results
SELECT prd_id, COUNT(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
--There are no NULLs or DUPLICATES

--Check for unwanted spaces
--Expected: No results
SELECT * FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line);
-- There are no TRIM operations required in ANY column

--Check for unwanted entries in prl_line
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;
--There are no problems

--Check for NULLs or Negative values in prd_cost
SELECT * FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
-- In this context, we can change that NULLs for 0

--Check for Invalid dates
--An invalid date is when the prd_end_dt is EARLIER than the prd_start_dt
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt OR prd_start_dt IS NULL;

SELECT * FROM silver.crm_prd_info;

--=======================================
--Checking for crm_sales_details
--=======================================

--Check for unwanted spaces
--Expected: No results
SELECT * FROM silver.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key);
-- There are no TRIM operations required in ANY column


--Check for 0 or invalid values in Date Columns.
SELECT * FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101 --This should be coherent ranges (Ex: Today)
OR sls_due_dt < 19000101;--This should be coherent ranges (Ex: Creation date of the records)
-- In sls_ship_date and sls_due_dt all fine

--Check for data validity
--sls_order_dt must be the lowest, then sls_ship_dt and then sls_due_dt the most recent one
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt;

--Check for inconsistencies for quantity * price = sales

--Check NULLs, Zero or Negatives from sales, quantity and columns
SELECT DISTINCT sls_sales,sls_quantity,sls_price FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <=0 OR sls_sales IS NULL OR
sls_price <=0 OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity,sls_price;

--=======================================
--Checking for erp_CUST_AZ12
--=======================================

--Check for Nulls and Duplicates in CID
-- Expected: No results
SELECT CID, COUNT(*) FROM silver.erp_CUST_AZ12
GROUP BY CID
HAVING COUNT(*) > 1 OR CID IS NULL;
--No NULLS No duplicates

--Check for 0 or invalid values in Date Columns.
SELECT * FROM silver.erp_CUST_AZ12
WHERE BDATE > GETDATE() --This should be coherent ranges (Ex: Today)
	OR BDATE < '1900-01-01';--This should be coherent ranges (Ex: Creation date of the records)
--As there are results, we need to CLEAN the data. We can replace the values with NULLs.

--Check for unwanted spaces
--Expected: No results
SELECT * FROM silver.erp_CUST_AZ12
WHERE CID != TRIM(CID);
-- No transformations required

--Check standarization of GEN
SELECT DISTINCT GEN FROM silver.erp_CUST_AZ12;
--Standarization of the data is required

--=======================================
--Checking for erp_LOC_A101
--=======================================

--Check for unwanted spaces
--Expected: No results
SELECT * FROM silver.erp_LOC_A101
WHERE CNTRY != TRIM(CNTRY);
-- There are no TRIM operations required in ANY column

--Check for Data Standarization
SELECT DISTINCT CNTRY FROM silver.erp_LOC_A101;
--Requires Data Standarization

--=======================================
--Checking for erp_PX_CAT_G1V2
--=======================================
--Check for unwanted spaces
--Expected: No results
SELECT * FROM bronze.erp_PX_CAT_G1V2
WHERE MAINTENANCE != TRIM(MAINTENANCE);
-- There are no TRIM operations required in ANY column

--Data standarization & consistency check
SELECT DISTINCT MAINTENANCE FROM bronze.erp_PX_CAT_G1V2;
--There are no issues with the data standarization
