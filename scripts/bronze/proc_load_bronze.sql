/*
 Stored Procedure : Load data from source => Bronze Layer

Script Purpose: 

- Implement `bronze.load_bronze` stored procedure for batch data loading.
- Bulk load CRM & ERP datasets with `BULK INSERT` for efficient ETL.
- Add execution time tracking for each table and total batch duration.
- Improve error handling with `TRY...CATCH` and transaction rollback.
- Enhance logging for better debugging and monitoring.

Usage Example : 
  EXEC [bronze].[load_bronze]
*/

-- CREATE Store Procedured
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    SET @batch_start_time = GETDATE(); -- Measure the loading time of the whole batch
    BEGIN TRY
        PRINT '===================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '---------------------------------------------------';
        PRINT 'Loading CRM tables';
        
        -- Load crm_cust_info
        SET @start_time = GETDATE();
        TRUNCATE TABLE [bronze].[crm_cust_info];

        BULK INSERT [bronze].[crm_cust_info]
        FROM 'C:\Users\Windows\OneDrive\1. Data Analyst\3. Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration for crm_cust_info: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load crm_prd_info
        SET @start_time = GETDATE();
        TRUNCATE TABLE [bronze].[crm_prd_info];

        BULK INSERT [bronze].[crm_prd_info]
        FROM 'C:\Users\Windows\OneDrive\1. Data Analyst\3. Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration for crm_prd_info: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load crm_sales_details
        SET @start_time = GETDATE();
        TRUNCATE TABLE [bronze].[crm_sales_details];

        BULK INSERT [bronze].[crm_sales_details]
        FROM 'C:\Users\Windows\OneDrive\1. Data Analyst\3. Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration for crm_sales_details: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '---------------------------------------------------';
        PRINT 'Loading ERP tables';

        -- Load erp_cust_az12
        SET @start_time = GETDATE();
        TRUNCATE TABLE [bronze].[erp_cust_az12];

        BULK INSERT [bronze].[erp_cust_az12]
        FROM 'C:\Users\Windows\OneDrive\1. Data Analyst\3. Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration for erp_cust_az12: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load erp_loc_a101
        SET @start_time = GETDATE();
        TRUNCATE TABLE [bronze].[erp_loc_a101];

        BULK INSERT [bronze].[erp_loc_a101]
        FROM 'C:\Users\Windows\OneDrive\1. Data Analyst\3. Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration for erp_loc_a101: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load erp_px_cat_g1v2
        SET @start_time = GETDATE();
        TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

        BULK INSERT [bronze].[erp_px_cat_g1v2]
        FROM 'C:\Users\Windows\OneDrive\1. Data Analyst\3. Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration for erp_px_cat_g1v2: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end_time = GETDATE();
		PRINT 'Loading Bronze Layer is completed'
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION; -- Rollback if any error occurs

        PRINT '===================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH;
END;
