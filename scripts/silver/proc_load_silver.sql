
/*	
=====================================================================================================================
Stored Procedure: Load Silver Layer (Bronze => Silver)
=====================================================================================================================
Script Purpose:
  This stored procedure performs the ETL (Extract, Transform, Load) process to
  populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
  - Truncates Silver tables.
  - Inserts transforemed and cleansed data from Bronze into Silver tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or retrun any values.

Usage Example:
  EXEC silver.load_silver

My Notes:
    Quality Checks:
    	- A Primary Key must be unique and not null
    	- Check for unwanted spaces in string values
    	- Check the consistency of values in low cardinality columns and apply your rules
    	-> in our dwh as a general rule we say, don't use abbreviations and replace any nulls with 'n/a'
    	- Check if Dates are all valid dates
    	- Re-run the quality check queries (from the bronze layer) to verify the quality of data in the silver layer

    List of all Data Transformations (for reference):
    	- Data Cleaning
    	- Data Standardization
    	- Data Normalization
    	- Derived Columns
    	- Data Enrichment
======================================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================';
		PRINT 'Loading Silver Layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
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
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE UPPER(TRIM(cst_marital_status))
				WHEN 'S' THEN 'Single'
				WHEN 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE UPPER(TRIM(cst_gndr))
				WHEN 'M' THEN 'Male'
				WHEN 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
		cst_create_date
		FROM (
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM
			bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1; -- Select the most recent record per customer;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info ;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
			REPLACE(SUBSTRING (prd_key,1,5),'-','_') AS cat_id, -- Extract category ID (Derived column)
			SUBSTRING (prd_key,7,len(prd_key)) AS prd_key,		-- Extract product key (Derived column)
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Sport'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			prd_start_dt,
			DATEADD(day, -1,LEAD(prd_start_dt,1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) 
			AS prd_end_dt -- Calculate end date as one day before the next start date (Data enrichment)
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		-- Loading silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
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
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt)!= 8 THEN NULL
				ELSE CAST(sls_order_dt AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt)!= 8 THEN NULL
				ELSE CAST(sls_ship_dt AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt)!= 8 THEN NULL
				ELSE CAST(sls_due_dt AS DATE)
			END AS sls_due_dt,

			-- Business Rules: 
			-- If Sales is negative, zero, or null, derive it using Quantity and Price
			-- If Price is negative, zero, or null, derive it using Sales and Quantity
			-- If Price is negative, convert it to a positive value
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!= sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <0 
				THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
				ELSE ABS(sls_price)
			END AS sls_price -- Derive price if original value is invalid
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		PRINT '---------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------';

		-- Loading silver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid 
			END AS cid,
			CASE 
				WHEN bdate>GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		-- Loading silver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		) 
		SELECT 
			REPLACE(cid,'-','') AS cid,
			CASE 
				WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		-- Loading silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		select
			CASE id	
				WHEN 'CO_PD' THEN 'CO_PE'
				ELSE id
			END AS id, -- Updated Category for pedals from CO_PD to CO_PE
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '========================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '    - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) +' seconds';
		PRINT '========================================';		
	END TRY
	BEGIN CATCH
		PRINT'=====================================================================';
		PRINT'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT'Error Message ' + ERROR_MESSAGE();
		PRINT'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error State ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'=====================================================================';
	END CATCH
END







