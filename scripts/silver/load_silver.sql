/*
==========================================
Stored Procedure : Load Silver Layer
==========================================

Script Purpose:
	This script performs the ETL (Extract, Transform, Load) process to  populate the data from 'bronze' schema tables 
  to our 'Silver' schema tables.
	It perform the following action:
	- Truncate the silver tables before loading data.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
	Parameters:
	None : This stored procedure does not accept any parameters or return any values.

You can executive this stored procedure using 
EXEC silver.load_silver
*/

IF OBJECT_ID('silver.load_silver', 'P') IS NOT NULL
    DROP PROCEDURE silver.load_silver;
GO
CREATE PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@start_batch DATETIME,@end_batch DATETIME;
	BEGIN TRY
		SET @start_batch =GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Silver Layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';
		PRINT '>> Truncate Table: silver.crm_cust_info'
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Insert Data Into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
		SELECT 
			cst_id,
			cst_key,
			LTRIM(RTRIM(cst_firstname)),
			LTRIM(RTRIM(cst_lastname)),
			CASE 
			WHEN UPPER(LTRIM(RTRIM(cst_marital_status)))='M' THEN 'Married'
			WHEN UPPER(LTRIM(RTRIM(cst_marital_status)))='S' THEN 'Single'
			ELSE 'n/a' END AS cst_marital_status, -- Normalize martial status values to redable format
			CASE 
			WHEN UPPER(LTRIM(RTRIM(cst_gndr)))='M' THEN 'Male'
			WHEN UPPER(LTRIM(RTRIM(cst_gndr)))='F' THEN 'Female'
			ELSE 'n/a' END AS cst_gndr, -- Normalize gender status values to redable format
			cst_create_date
		FROM
			(SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
			FROM bronze.crm_cust_info) Anurag
			WHERE rn=1 AND cst_id IS NOT NULL
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Insert Data Into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id, -- Extract Category ID
			SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,      -- Extract product key
			prd_nm,
			ISNULL(prd_cost,0) as prd_cost,
			CASE UPPER(LTRIM(RTRIM(prd_line)))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'n/a'
			END AS prd_line,-- Map product line codes to descriptive values
			CAST(prd_start_dt AS Date) AS prd_start_dt,
			CAST(LEAD(DATEADD(DAY, -1, prd_start_dt)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS Date) AS prd_end_dt --Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Insert Data Into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
			END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
				THEN sls_quantity*ABS(sls_price) ELSE sls_sales
			END sls_sales, -- Recalculating Sales if Original value is missing or incorrect
			sls_quantity,
			CASE WHEN sls_price<=0 OR sls_price IS NULL THEN sls_sales/NULLIF(sls_quantity,0)
				 ELSE sls_price
			END sls_price -- Derive price if original value is invalid
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12 
		PRINT '>> Insert Data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN RIGHT(cid,LEN(cid)-3) --Revmove 'NAS' prefix if present
				ELSE cid 
			END AS cid,
			CASE WHEN bdate>GETDATE() THEN NULL
				ELSE bdate 
			END AS bdate, -- Set future birthdates to NULL
			CASE WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE') THEN 'Male'
				ELSE 'n/a' 
			END AS gen -- Normalize gender values and handle unkown cases
		FROM bronze.erp_cust_az12 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101 
		PRINT '>> Insert Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry)
		SELECT 
			REPLACE(cid,'-','') cid,
			CASE 
				WHEN LTRIM(RTRIM(cntry))='DE' THEN 'Germany'
				WHEN LTRIM(RTRIM(cntry)) IN ('US','USA') THEN 'United States'
				WHEN LTRIM(RTRIM(cntry))='' OR cntry IS NULL THEN 'n/a'
				ELSE LTRIM(RTRIM(cntry))
			END AS cntry -- Normalize and handle missing or blank country codes
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Insert Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';
		SET @end_batch=GETDATE();
		PRINT 'Loading Silver Layer is completed';
		PRINT'>>Total Load Duration '+CAST(DATEDIFF(second,@start_batch,@end_batch)AS NVARCHAR) +' seconds';
		
	END TRY
	BEGIN CATCH
		PRINT '===========================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message '+ERROR_MESSAGE();
		PRINT 'Error Message '+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message '+CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================';
	END CATCH
END
