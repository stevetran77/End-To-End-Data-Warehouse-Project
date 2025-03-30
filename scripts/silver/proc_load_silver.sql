/*
===============================================================================
ETL Script: Load Data into Silver Tables
===============================================================================
Script Purpose:
    This stored procedure loads and transforms data from the 'bronze' layer 
    into cleaned, standardized tables in the 'silver' schema.

    It applies business rules, data cleaning, deduplication, and normalization 
    for downstream analytics.

    Includes step-by-step execution logging and duration tracking.

    Error handling is implemented using TRY...CATCH to capture failures during 
    the transformation process.

Metadata:
    This script does not modify table structures.
    It operates purely as a transformation and loading logic from bronze → silver.
===============================================================================
*/

EXEC silver.load_silver
CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE(); -- Total duration tracker

	BEGIN TRY
		PRINT '===================================================';
		PRINT 'Loading Silver Layer';
		PRINT '---------------------------------------------------';

		PRINT 'Processing CRM tables';

		-- silver.crm_cust_info
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_cust_info;
		INSERT INTO silver.crm_cust_info (
			cst_id, cst_key, cst_firstname, cst_lastname,
			cst_marital_status, cst_gndr, cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname),
			TRIM(cst_lastname),
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'N/A'
			END,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'N/A'
			END,
			cst_create_date
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rb
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE rb = 1;
		SET @end_time = GETDATE();
		PRINT 'Load Duration for silver.crm_cust_info: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- silver.crm_prd_info
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info (
			prd_id, cat_id, prd_key, prd_nm,
			prd_cost, prd_line, prd_start_dt, prd_end_dt
		)
		SELECT 
			prd_id,
			REPLACE(TRIM(SUBSTRING(prd_key,1,5)),'-','_'),
			TRIM(SUBSTRING(prd_key, 7, LEN(prd_key))),
			prd_nm,
			ISNULL(prd_cost, 0),
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END,
			CAST(prd_start_dt AS DATE),
			CAST(
				CASE 
					WHEN prd_end_dt < prd_start_dt 
					THEN DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
					ELSE prd_end_dt
				END AS DATE)
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT 'Load Duration for silver.crm_prd_info: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- silver.crm_sales_details
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details (
			sls_ord_num, sls_prd_key, sls_cust_id,
			sls_order_dt, sls_ship_dt, sls_due_dt,
			sls_sales, sls_quantity, sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) END,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt)!=8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) END,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt)!=8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE) END,
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
				 THEN sls_quantity * ABS(sls_price)
				 ELSE sls_sales
			END,
			sls_quantity,
			CASE
				WHEN sls_price = 0 OR sls_price IS NULL THEN sls_sales/NULLIF(sls_quantity, 0)
				WHEN sls_price < 0 THEN ABS(sls_price)
				ELSE sls_price
			END
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT 'Load Duration for silver.crm_sales_details: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '---------------------------------------------------';
		PRINT 'Processing ERP tables';

		-- silver.erp_cust_az12
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_cust_az12;
		INSERT INTO silver.erp_cust_az12 (
			cid, bdate, gen
		)
		SELECT 
			RIGHT(cid, 5),
			CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
			CASE 
				WHEN TRIM(gen) = 'F' THEN 'Female'
				WHEN TRIM(gen) = 'M' THEN 'Male'
				WHEN TRIM(gen) = '' OR gen IS NULL THEN 'N/A'
				ELSE TRIM(gen)
			END
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT 'Load Duration for silver.erp_cust_az12: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- silver.erp_loc_a101
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101 (
			cid, cntry
		)
		SELECT 
			RIGHT(cid, 5),
			CASE 
				WHEN cntry IS NULL OR cntry = '' THEN 'N/A'
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
				ELSE cntry
			END
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT 'Load Duration for silver.erp_loc_a101: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		INSERT INTO silver.erp_px_cat_g1v2 (
			id, cat, subcat, maintenance
		)
		SELECT id, cat, subcat, maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT 'Load Duration for silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		-- ✅ Total Duration
		SET @batch_end_time = GETDATE();
		PRINT 'Loading Silver Layer is completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH
		PRINT '===================================================';
		PRINT '❌ ERROR OCCURRED DURING LOADING SILVER LAYER';
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END;
