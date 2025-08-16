/*
=================================================
This script is used to data ingestion for the bronze layer

It TRUNCATES the current tables 
Then BULK INSERT new data

It also has a TRY CATCH verification to debug errors, comments for debugging and time variables to measure loading time
=================================================
*/

--Create stored procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	--Define time variables (Including a Whole Batch Variable to calculate the time of the whole query)
	DECLARE @start_time DATETIME, @end_time DATETIME, @whole_start_time DATETIME, @whole_end_time DATETIME;
	BEGIN TRY
		SET @whole_start_time= GETDATE();
		PRINT('===================================================');
		PRINT('Loading the Bronze Layer');
		PRINT('===================================================');

		PRINT('---------------------------------------------------');
		PRINT('Loading CRM files');
		PRINT('---------------------------------------------------');

	
		PRINT('---------------------------------------------------');
		PRINT('Loading crm_cust_info');
		PRINT('---------------------------------------------------');

		--Define start_time variable for this process
		SET @start_time = GETDATE();
		--DROP PREVIOUS FILES
		TRUNCATE TABLE bronze.crm_cust_info;
		--Insert the data from CSV files into tables
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\diego\OneDrive - Universidad de la Sabana\DATA SCIENCE\PORTAFOLIO\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2, --At which row the data starts. Avoid column naming
			FIELDTERMINATOR=',', --How is the data split in the file
			TABLOCK --Improvement of performance. Lock the table while it is loading
			);
		--Define end time for the process
		SET @end_time = GETDATE();
		PRINT('---------------------------------------------------');
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT('---------------------------------------------------');

		PRINT('---------------------------------------------------');
		PRINT('Loading crm_prd_info');
		PRINT('---------------------------------------------------');

		SET @start_time = GETDATE();
		--DROP PREVIOUS FILES
		TRUNCATE TABLE bronze.crm_prd_info;
		--Insert the data from CSV files into tables
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\diego\OneDrive - Universidad de la Sabana\DATA SCIENCE\PORTAFOLIO\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2, --At which row the data starts. Avoid column naming
			FIELDTERMINATOR=',', --How is the data split in the file}
			TABLOCK --Improvement of performance. Lock the table while it is loading
			);

		SET @end_time= GETDATE();
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	
		PRINT('---------------------------------------------------');
		PRINT('Loading crm_sales_details');
		PRINT('---------------------------------------------------');
		
		SET @start_time= GETDATE(); 
		--DROP PREVIOUS FILES
		TRUNCATE TABLE bronze.crm_sales_details;
		--Insert the data from CSV files into tables
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\diego\OneDrive - Universidad de la Sabana\DATA SCIENCE\PORTAFOLIO\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2, --At which row the data starts. Avoid column naming
			FIELDTERMINATOR=',', --How is the data split in the file}
			TABLOCK --Improvement of performance. Lock the table while it is loading
			);

		SET @end_time= GETDATE();
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

		PRINT('---------------------------------------------------');
		PRINT('Loading ERP files');
		PRINT('---------------------------------------------------');

	
		PRINT('---------------------------------------------------');
		PRINT('Loading erp_CUST_AZ12');
		PRINT('---------------------------------------------------');
		--DROP PREVIOUS FILES

		SET @start_time= GETDATE();
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		--Insert the data from CSV files into tables
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\diego\OneDrive - Universidad de la Sabana\DATA SCIENCE\PORTAFOLIO\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2, --At which row the data starts. Avoid column naming
			FIELDTERMINATOR=',', --How is the data split in the file}
			TABLOCK --Improvement of performance. Lock the table while it is loading
			);

		SET @end_time= GETDATE();
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	
		PRINT('---------------------------------------------------');
		PRINT('Loading erp_LOC_A101');
		PRINT('---------------------------------------------------');
		--DROP PREVIOUS FILES
		SET @start_time=GETDATE()
		TRUNCATE TABLE bronze.erp_LOC_A101;
		--Insert the data from CSV files into tables
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\diego\OneDrive - Universidad de la Sabana\DATA SCIENCE\PORTAFOLIO\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2, --At which row the data starts. Avoid column naming
			FIELDTERMINATOR=',', --How is the data split in the file}
			TABLOCK --Improvement of performance. Lock the table while it is loading
			);
	SET @end_time= GETDATE();
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	
		PRINT('---------------------------------------------------');
		PRINT('Loading erp_PX_CAT_G1V2');
		PRINT('---------------------------------------------------');

		SET @start_time= GETDATE();
		--DROP PREVIOUS FILES

		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		--Insert the data from CSV files into tables
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\diego\OneDrive - Universidad de la Sabana\DATA SCIENCE\PORTAFOLIO\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2, --At which row the data starts. Avoid column naming
			FIELDTERMINATOR=',', --How is the data split in the file}
			TABLOCK --Improvement of performance. Lock the table while it is loading
			);

		SET @end_time= GETDATE();
		PRINT '>> Load duration time was ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

		SET @whole_end_time=GETDATE();

		PRINT('===================================================');
		PRINT('Bronze layer LOADED');
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

EXEC bronze.load_bronze;
