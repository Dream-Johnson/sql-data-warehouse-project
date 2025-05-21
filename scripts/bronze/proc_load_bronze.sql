/*
	FULL LOAD
================================================================================

STORED PROCEDURE:
    This stored procedure loads the data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncate the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

PARAMETERS:
     This stored procedure does not erturn any values nor accept any parameters.

USAGE:
  EXEC bronze.load_bronze;
or
  EXECUTE bronze.load_bronze;

================================================================================
*/

--exec bronze.load_bronze
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @whole_batch_start DATETIME, @whole_batch_end DATETIME;								--Declaring Variables
	BEGIN TRY
		PRINT '===========================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===========================================================';

		PRINT '---------------------------------';
		PRINT 'Loading CRM TABLES';
		PRINT '---------------------------------';

		SET @whole_batch_start = GETDATE()
			SET @start_time = GETDATE();												-- defining variable
			PRINT '>>Truncating Table: [bronze].[crm_cust_info]';
			TRUNCATE TABLE [bronze].[crm_cust_info]
			PRINT '>>Inserting Data into: [bronze].[crm_cust_info]';
			BULK INSERT [bronze].[crm_cust_info]
			FROM 'D:\Course\Data_Warehouse_Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '-------------------------------------';
			--SELECT COUNT(*) AS [Num_of_ToltalRows] FROM [bronze].[crm_cust_info]


			SET @start_time = GETDATE();
			PRINT '>>Truncating Table: [bronze].[crm_prd_info]';
			TRUNCATE TABLE [bronze].[crm_prd_info]
			PRINT '>>Inserting Data into: [bronze].[crm_prd_info]';
			BULK INSERT [bronze].[crm_prd_info]
			FROM 'D:\Course\Data_Warehouse_Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '-------------------------------------';


			SET @start_time = GETDATE();
			PRINT '>>Truncating Table: [bronze].[crm_sales_details]';
			TRUNCATE TABLE [bronze].[crm_sales_details]
			PRINT '>>Inserting Data into: [bronze].[crm_sales_details]';
			BULK INSERT [bronze].[crm_sales_details]
			FROM 'D:\Course\Data_Warehouse_Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '-------------------------------------';


			SET @start_time = GETDATE();
			PRINT '>>Truncating Table: [bronze].[erp_cust_az12]';
			TRUNCATE TABLE [bronze].[erp_cust_az12]
			PRINT '>>Inserting Data into: [bronze].[erp_cust_az12]';
			BULK INSERT [bronze].[erp_cust_az12]
			FROM 'D:\Course\Data_Warehouse_Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '-------------------------------------';


			SET @start_time = GETDATE();
			PRINT '>>Truncating Table: [bronze].[erp_loc_a101]';
			TRUNCATE TABLE [bronze].[erp_loc_a101]
			PRINT '>>Inserting Data into: [bronze].[erp_loc_a101]'
			BULK INSERT [bronze].[erp_loc_a101]
			FROM 'D:\Course\Data_Warehouse_Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
			PRINT '-------------------------------------';


			SET @start_time = GETDATE();
			PRINT '>>Truncating Table: [bronze].[erp_px_cat_g1v2]';
			TRUNCATE TABLE [bronze].[erp_px_cat_g1v2]
			PRINT '>>Inserting Data into: bronze].[erp_px_cat_g1v2]'
			BULK INSERT [bronze].[erp_px_cat_g1v2]
			FROM 'D:\Course\Data_Warehouse_Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT 'Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			PRINT '-------------------------------------';
		SET @whole_batch_end = GETDATE()
		PRINT '+++++++++++++++++++++++'
		PRINT 'Bronze layer load duration: ' + CAST(DATEDIFF(second,@whole_batch_start,@whole_batch_end) AS NVARCHAR) + ' seconds';
		PRINT '+++++++++++++++++++++++'
		END TRY
		BEGIN CATCH
			PRINT '================================================='
			PRINT 'ERROR OCCURED LOADING BRONZE LAYER'
			PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
			PRINT 'ERROR MESSAGE' + CAST( ERROR_NUMBER() AS NVARCHAR);
			PRINT 'ERROR MESSAGE' + CAST( ERROR_STATE() AS NVARCHAR);
			PRINT '================================================='
		END CATCH
END




