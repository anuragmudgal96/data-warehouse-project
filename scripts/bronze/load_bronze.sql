/*
==========================================
Stored Procedure : Load Bronze Layer
==========================================

Script Purpose:
	This script load the data from external csv files to our 'bronze' schema
	It perform the following action:
	- Truncate the bronze tables before loading data.
	- Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

	Parameters:
	None : This stored procedure does not accept any parameters or return any values.
*/


IF OBJECT_ID('bronze.load_bronze', 'P') IS NOT NULL
    DROP PROCEDURE bronze.load_bronze;
GO
CREATE PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@start_batch DATETIME,@end_batch DATETIME;
	BEGIN TRY
		SET @start_batch =GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ANKIT SHARMA\Desktop\data_warehouse_project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,          -- Start reading data from the second row of the file (skip the header).
			FIELDTERMINATOR=',', -- Specify that fields in the CSV file are separated by commas.
			TABLOCK              -- Table-level lock to improve performance during the bulk insert.
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ANKIT SHARMA\Desktop\data_warehouse_project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,          
			FIELDTERMINATOR=',',  
			TABLOCK              
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\ANKIT SHARMA\Desktop\data_warehouse_project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,          
			FIELDTERMINATOR=',',  
			TABLOCK              
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\ANKIT SHARMA\Desktop\data_warehouse_project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW=2,          
			FIELDTERMINATOR=',',  
			TABLOCK              
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\ANKIT SHARMA\Desktop\data_warehouse_project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW=2,          
			FIELDTERMINATOR=',',  
			TABLOCK              
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ANKIT SHARMA\Desktop\data_warehouse_project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW=2,          
			FIELDTERMINATOR=',',  
			TABLOCK              
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT '----------------';
		SET @end_batch=GETDATE();
		PRINT 'Loading Bronze Layer is completed';
		PRINT'>>Total Load Duration '+CAST(DATEDIFF(second,@start_batch,@end_batch)AS NVARCHAR) +' seconds';
		
	END TRY
	BEGIN CATCH
		PRINT '===========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message '+ERROR_MESSAGE();
		PRINT 'Error Message '+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message '+CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================';
	END CATCH
END;

EXEC bronze.load_bronze
