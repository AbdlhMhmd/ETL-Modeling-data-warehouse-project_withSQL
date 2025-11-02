/*
===============================================================================
Data Ingestion Procedure: Load Bronze Layer (from source to Bronze layer)
===============================================================================
This code loads the data from the source to the Bronze layer using the 'Full Load' method from the external CSV files stored on my laptop 
Using Truncate (to avoid double loading) and BULK INSERT for the data.
The duration of each table has been calculated.
The duration of the whole Bronze loading is also calculated.
TRY CATCH method is used to print and overcome errors

Usage Example:
EXEC bronze.load_bronze;
===============================================================================
*/


-- create a procedure for the whole process
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN

  -- define some variables for the duration
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY

		PRINT '===========================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===========================================================';

		-- TRUNCATE to delete all the records in the table before the 'FULL LOAD'
		-- start with tables from the first database 'crm'

		-- set start time for the whole bronze layer 
		SET @batch_start_time = GETDATE();

		SET @start_time = GETDATE();
		PRINT '-----------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------';

		SET @start_time = GETDATE();	 
		PRINT '>>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Nile University\BDDS\@ Data engineering projects\sql-data-warehouse-project-Baraa\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE(); 
		PRINT '> Loading duration: ' + CAST( DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Nile University\BDDS\@ Data engineering projects\sql-data-warehouse-project-Baraa\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Loading duration: ' + CAST(DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------';


		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Nile University\BDDS\@ Data engineering projects\sql-data-warehouse-project-Baraa\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Loading duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' Seconds';
		PRINT '---------------------------';

		-- start with tables from the first database 'erp'
		PRINT '-----------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Nile University\BDDS\@ Data engineering projects\sql-data-warehouse-project-Baraa\datasets\source_erp\cust_az12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Loading duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' Seconds';
		PRINT '---------------------------';


		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Nile University\BDDS\@ Data engineering projects\sql-data-warehouse-project-Baraa\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Loading duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' Seconds';
		PRINT '---------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Nile University\BDDS\@ Data engineering projects\sql-data-warehouse-project-Baraa\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '> Loading duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+ ' Seconds';
		PRINT '---------------------------';

		PRINT '===========================================================';
		PRINT 'BRONZE LAYER LOADING IS COMPLETED SUCCESSFULLY';
		-- set end time for the whole bronze layer 
		SET @batch_end_time = GETDATE(); 
		PRINT '> Bronze Layer Loading duration: ' + CAST( DATEDIFF (second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '===========================================================';


	END TRY
	BEGIN CATCH 
		PRINT '===========================================================';
		PRINT 'ERROR OCCURS DURING LOADING THE BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================================';
	END CATCH
END



EXEC bronze.load_bronze;

