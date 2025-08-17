/*
=================================================
This script is used to data ingestion for the SILVER layer

It TRUNCATES the current tables 
Then CLEAN and INSERT the data

It also has a TRY CATCH verification to debug errors, comments for debugging and time variables to measure loading time
=================================================
*/

--Create stored procedure
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	--Define time variables (Including a Whole Batch Variable to calculate the time of the whole query)
	DECLARE @start_time DATETIME, @end_time DATETIME, @whole_start_time DATETIME, @whole_end_time DATETIME;
	BEGIN TRY
		SET @whole_start_time= GETDATE();
		PRINT '==========================================='
		PRINT 'Truncating Table: silver.crm_cust_info';
		PRINT '==========================================='
		--Define start_time variable for this process
		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '==========================================='
		PRINT 'Inserting Data into: silver.crm_cust_info'
		PRINT '==========================================='

		--Insert the clean data into the silver.crm_cust_info
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_fistname,
			cst_lastname,
			cst_marital_status,
			cst_gnder,
			cst_create_date)

		-- Query to select just the UNIQUE, NOT NULL and TOPN 1 duplicated records.
		SELECT
		cst_id, cst_key, 
		--TRIM the columns with undesired spaces
		TRIM(cst_fistname),
		TRIM(cst_lastname),
		--Replace Single characters with WHOLE NAMES and NULL treatment
		CASE
		--Use UPPER just to make sure that the comparison is always valid, even if the record in table is lower case
			WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
			WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
			ELSE 'n/a'
		END,
		CASE
			WHEN UPPER(cst_gnder) = 'M' THEN 'Male'
			WHEN UPPER(cst_gnder) = 'F' THEN 'Female'
			ELSE 'n/a'
		END,
		cst_create_date
		FROM (
			-- Query to rank the duplicated records by date. We are looking to preserve the MOST RECENT record.
			SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS "recent_record_track"
			FROM bronze.crm_cust_info
			-- Filter no avoid NULL values in the cst_id
			WHERE cst_id IS NOT NULL
			) AS ranked
			--Filter the subquery in order to get just the UNIQUE and MOST RECENT duplicated records. 
		WHERE ranked.recent_record_track = 1;

		--Define end time for the process
		SET @end_time = GETDATE();
		PRINT('---------------------------------------------------');
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT('---------------------------------------------------');

		PRINT('---------------------------------------------------');
		PRINT('Loading crm_prd_info');
		PRINT('---------------------------------------------------');

		PRINT '==========================================='
		PRINT 'Truncating Table: silver.crm_prd_info';
		PRINT '==========================================='

		--Define start_time variable for this process
		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '==========================================='
		PRINT 'Inserting Data into: silver.crm_prd_info'
		PRINT '==========================================='

		--Insert the clean data into the silver.crm_prd_info
		INSERT INTO silver.crm_prd_info (
			prd_id,
			prd_key,
			category_id,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)

		--New column for Category ID from the prd_key
		SELECT 
		prd_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, --Take the characters from char 7 (Avoid the -) to the end of the record.
		LEFT(REPLACE(prd_key,'-','_'),5) AS category_id, --Take the first 5 characters from the left 
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE
			WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
			WHEN UPPER(TRIM(prd_line))='S' THEN 'Other sales'
			ELSE 'n/a'
		END AS 'prd_line',
		prd_start_dt AS prd_start_dt,
		DATEADD(DAY,-1,LEAD(prd_start_dt,1) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM bronze.crm_prd_info;

		--Define end time for the process
		SET @end_time = GETDATE();
		PRINT('---------------------------------------------------');
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT('---------------------------------------------------');

		PRINT('---------------------------------------------------');
		PRINT('Loading crm_sales_details');
		PRINT('---------------------------------------------------');

		PRINT '==========================================='
		PRINT 'Truncating Table: silver.crm_sales_details';
		PRINT '==========================================='

		--Define start_time variable for this process
		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '==========================================='
		PRINT 'Inserting Data into: silver.crm_sales_details'
		PRINT '==========================================='

		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)

		--Data cleansing
		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		--Replace invalid values following the rules 
		--RULES:
			--Sales=Price*Quantity
			--If Price < 0, then * (-1)
		CASE
			WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * sls_price 
				THEN ABS(sls_price) * sls_quantity
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price =0 OR sls_price IS NULL 
				THEN sls_sales / sls_quantity
			WHEN sls_price <= 0 THEN sls_price * (-1)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;

				--Define end time for the process
		SET @end_time = GETDATE();
		PRINT('---------------------------------------------------');
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT('---------------------------------------------------');

		PRINT('---------------------------------------------------');
		PRINT('Loading erp_CUST_AZ12');
		PRINT('---------------------------------------------------');

		PRINT '==========================================='
		PRINT 'Truncating Table: silver.erp_CUST_AZ12';
		PRINT '==========================================='

		--Define start_time variable for this process
		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.erp_CUST_AZ12;
		PRINT '==========================================='
		PRINT 'Inserting Data into: silver.erp_CUST_AZ12'
		PRINT '==========================================='

		INSERT INTO silver.erp_CUST_AZ12 (
			CID,
			BDATE,
			GEN
			)

		SELECT
		CASE WHEN CID LIKE 'NAS%' THEN TRIM(SUBSTRING(CID,4,LEN(CID)))
			ELSE CID
		END AS CID,
		CASE --Set future BDAYS to NULL
			WHEN BDATE >= GETDATE() THEN NULL
			ELSE BDATE
		END AS BDATE,
		CASE --Standartdize the data
			WHEN GEN = 'F' THEN 'Female'
			WHEN GEN = 'M' THEN 'Male'
			WHEN GEN = '' OR GEN IS NULL THEN 'n/a'
			ELSE GEN
		END AS GEN
		FROM bronze.erp_CUST_AZ12
		WHERE CID NOT IN (SELECT cst_key FROM silver.crm_cust_info);

				--Define end time for the process
		SET @end_time = GETDATE();
		PRINT('---------------------------------------------------');
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT('---------------------------------------------------');

		PRINT('---------------------------------------------------');
		PRINT('Loading erp_LOC_A101');
		PRINT('---------------------------------------------------');

		PRINT '==========================================='
		PRINT 'Truncating Table: silver.erp_LOC_A101';
		PRINT '==========================================='

		--Define start_time variable for this process
		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.erp_LOC_A101;
		PRINT '==========================================='
		PRINT 'Inserting Data into: silver.erp_LOC_A101'
		PRINT '==========================================='

		INSERT INTO silver.erp_LOC_A101 (
			CID,CNTRY
		)

		SELECT
		REPLACE(CID,'-','') AS CID,
		CASE
			WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY)= 'USA' OR CNTRY = 'US' THEN 'United States'
			WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
			ELSE TRIM(CNTRY)
		END AS CNTRY
		FROM bronze.erp_LOC_A101;

				--Define end time for the process
		SET @end_time = GETDATE();
		PRINT('---------------------------------------------------');
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT('---------------------------------------------------');

		PRINT('---------------------------------------------------');
		PRINT('Loading erp_PX_CAT_G1V2');
		PRINT('---------------------------------------------------');

		PRINT '==========================================='
		PRINT 'Truncating Table: silver.erp_PX_CAT_G1V2';
		PRINT '==========================================='

		--Define start_time variable for this process
		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		PRINT '==========================================='
		PRINT 'Inserting Data into: silver.erp_PX_CAT_G1V2'
		PRINT '==========================================='

		INSERT INTO silver.erp_PX_CAT_G1V2 (
			ID,CAT,SUBCAT,MAINTENANCE
			) 
		SELECT ID,CAT,SUBCAT,MAINTENANCE
		FROM bronze.erp_PX_CAT_G1V2;

				--Define end time for the process
		SET @end_time = GETDATE();
		PRINT('---------------------------------------------------');
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT('---------------------------------------------------');

		SET @whole_end_time=GETDATE();

		PRINT('===================================================');
		PRINT('Silver layer LOADED');
		PRINT'>> Load duration of the WHOLE BATCH was '+ CAST(DATEDIFF(second,@whole_start_time,@whole_end_time) AS NVARCHAR) + 'seconds';
		PRINT('===================================================');
	END TRY
	BEGIN CATCH
		PRINT '===================================================';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================';
	END CATCH
END
