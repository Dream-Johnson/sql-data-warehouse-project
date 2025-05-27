/*
+++++++++++++++++++++++++++++
Stored Procedure: Load Silver Layer (Bronze -> Silver)
+++++++++++++++++++++++++++++
Script Purpose:
    This stored procedure performs the ETL process to populate the silver schema from the bronze schema

Actions Performed:
    - Truncate silver tables
    - Insert Transformed and cleaned data

Usage Example: 
EXEC silver.load_silver
*/

--EXEC silver.load_silver
CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @whole_batch_start DATETIME, @whole_batch_end DATETIME;
	BEGIN TRY
			PRINT '===========================================================';
			PRINT 'Loading Silver Layer';
			PRINT '===========================================================';

			PRINT '---------------------------------';
			PRINT 'Loading CRM TABLES';
			PRINT '---------------------------------';
	SET @whole_batch_start = GETDATE();
			SET @start_time = GETDATE();
			PRINT '>>Truncating Table: crm_cust_info'
			TRUNCATE TABLE silver.crm_cust_info;
			PRINT '>>Inserting Data Info: silver.crm_cust_info'

			INSERT INTO [silver].[crm_cust_info] (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)

			SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS [cst_firstname],
			TRIM(cst_lastname) AS [cst_lastname],
			CASE 
				--Removes whitespaces and make sure string is upper case and Normalized the values
				WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status,
			CASE 
				--Removes whitespaces and make sure string is upper case and Normalized the values
				WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date
			FROM(
			SELECT 
			*,
			--Query contaings code that ranks the dublicates but ranks them using the latest creation date
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			)t WHERE flag_last = 1;
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			---------------------------------------------------------------------------
			SET @start_time = GETDATE();
			PRINT '>>Truncating Table: [crm_prd_info]'
			TRUNCATE TABLE silver.[crm_prd_info];
			PRINT '>>Inserting Data Info: silver.[crm_prd_info]'

			INSERT INTO [silver].[crm_prd_info]
					   ([prd_id]
					   ,[cat_id]
					   ,[prd_key]
					   ,[prd_nm]
					   ,[prd_cost]
					   ,[prd_line]
					   ,[prd_start_date]
					   ,[prd_end_date])
   
			SELECT [prd_id]
				  ,REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id
				  ,SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key -- we need this column to join it with sales details
				  ,[prd_nm]
				  ,COALESCE([prd_cost],0) AS prd_cost --removing NULL
				  ,CASE UPPER(TRIM(prd_line))
						WHEN  'M' THEN 'Mountain'
						WHEN  'R' THEN 'Road'
						WHEN  'S' THEN 'Other Sales'
						WHEN  'T' THEN 'Touring'
						ELSE 'n/a'
					END prd_line
				  ,CAST([prd_start_date] AS DATE) AS prd_start_date
				  ,CAST(LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date)-1 AS DATE) AS prd_end_date
			  FROM [DataWarehouse].[bronze].[crm_prd_info] 
			  SET @end_time = GETDATE()
			  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
  
			  ------------------------------------------------------------
			  SET @start_time = GETDATE()
			  PRINT '>>Truncating Table: [crm_sales_details]'
			TRUNCATE TABLE silver.[crm_sales_details];
			PRINT '>>Inserting Data Info: silver.[crm_sales_details]'


			INSERT INTO [silver].[crm_sales_details]
					   ([sls_ord_num]
					   ,[sls_prd_key]
					   ,[sls_cust_id]
					   ,[sls_order_dt]
					   ,[sls_ship_dt]
					   ,[sls_due_dt]
					   ,[sls_sales]
					   ,[sls_quantity]
					   ,[sls_price]
					   )
    
			SELECT [sls_ord_num]
				  ,[sls_prd_key]
				  ,[sls_cust_id]
				  ,CASE 
						WHEN [sls_order_dt] = 0 or LEN(sls_order_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				   END AS sls_order_dt
				   ,CASE
						WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				   END AS sls_ship_dt
				  ,CASE
						WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				   END AS sls_due_dt
				  ,CASE 
						WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
							THEN sls_quantity * ABS(sls_price)
						ELSE sls_sales
					END sls_sales
				  ,[sls_quantity]
				  ,CASE
						WHEN sls_price IS NULL OR sls_price <=0
							THEN sls_sales / NULLIF(sls_quantity,0)
						ELSE sls_price
				   END AS sls_price
			  FROM [bronze].[crm_sales_details]
			  SET @end_time = GETDATE()
			  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			  ----------------------------------------------------------

			PRINT '---------------------------------';
			PRINT 'Loading ERP TABLES';
			PRINT '---------------------------------';

			SET @start_time = GETDATE()
			PRINT '>>Truncating Table: silver.[erp_cust_az12]'
			TRUNCATE TABLE silver.[erp_cust_az12];
			PRINT '>> Inserting Data into: silver.[erp_cust_az12]'
			INSERT INTO [silver].[erp_cust_az12]
					   ([CID]
					   ,[BDATE]
					   ,[GEN]
					   )
     

			SELECT 
					CASE 
						WHEN CID like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
						ELSE CID
					END CID

				  ,CASE 
						WHEN [BDATE] > GETDATE() THEN NULL
						ELSE BDATE
					END AS BDATE
				  ,CASE 
					WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
					ELSE 'n/a'
				END GEN
			  FROM [bronze].[erp_cust_az12]
			  SET @end_time = GETDATE();
			  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';


			--SELECT * FROM [silver].[crm_cust_info]
			-----------------------------------------------------

			SET @start_time = GETDATE()
			PRINT '>> Truncating Table: silver.[erp_loc_a101]'
			TRUNCATE TABLE silver.[erp_loc_a101];
			PRINT '>> Inserting Data into: silver.[erp_loc_a101]'
			INSERT INTO silver.[erp_loc_a101]
			(CID,CNTRY)
			SELECT 
			REPLACE(CID,'-','') CID,
			CASE 
				WHEN TRIM(CNTRY) = 'DE' THEN 'GERMANY'
				WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
				WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
				ELSE TRIM(CNTRY)
			END CNTRY --check missing,blank and unsanitized 
			FROM [bronze].[erp_loc_a101]
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

			--(SELECT cst_key FROM [silver].[crm_cust_info])

			--DAta satandarization and consistency

			--SELECT

			--CASE 
			--	WHEN TRIM(CNTRY) = 'DE' THEN 'GERMANY'
			--	WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
			--	WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
			--	ELSE TRIM(CNTRY)
			--END CNTRY,


			--CNTRY AS oldcntry

			--FROM [bronze].[erp_loc_a101]
			--select * from [silver].[erp_loc_a101]

			---------------------------------------------


			SET @start_time = GETDATE()
			PRINT '>> Truncating Table: silver.[erp_px_cat_g1v2]'
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
			PRINT '>> Inserting Data into: silver.[erp_px_cat_g1v2]'

			INSERT INTO silver.erp_px_cat_g1v2(ID,CAT,SUBCAT,[MAINTENANCE])


			SELECT [ID]
				  ,[CAT]
				  ,[SUBCAT]
				  ,[MAINTENANCE]
			  FROM [bronze].[erp_px_cat_g1v2]
			  SET @end_time = GETDATE()
			  PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		SET @whole_batch_end = GETDATE();
		PRINT '+++++++++++++++++++++++'
		PRINT 'Silver layer load duration: ' + CAST(DATEDIFF(second,@whole_batch_start,@whole_batch_end) AS NVARCHAR) + ' seconds';
		PRINT '+++++++++++++++++++++++'

	  --------------------------------------------------
	  END TRY
	  BEGIN CATCH
			PRINT '================================================='
			PRINT 'ERROR OCCURED LOADING SILVER LAYER'
			PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
			PRINT 'ERROR MESSAGE' + CAST( ERROR_NUMBER() AS NVARCHAR);
			PRINT 'ERROR MESSAGE' + CAST( ERROR_STATE() AS NVARCHAR);
			PRINT '================================================='
		END CATCH
END





