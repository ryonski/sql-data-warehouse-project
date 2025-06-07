/*
===========================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===========================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncated the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv files to bronze tables.

Parameters:
    None.
  This store procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

My Notes:
    To avoid loading the same data more than once we use the Trucate expression (to make the table empty) before 
    the bulk insert. Remember after successful loading to make sure to go through each table and check the contents,
    mainly to check if the data is in the right columns and if the number of records is the same as in the source files.

    As this script will be run frequently (daily basis) to get the new content into the data warehouse, we can store it in a procedure
    with CREATE OR ALTER PROCEDURE <name> AS  and then put everything inside a BEGIN... END block.

    Once procedure is exectuted it can be called with this command: EXEC <name of procedure>, so will be EXCE bronze.load_bronze
    
    Since the output message of the procedure alone is not clear, it's always a good idea to add PRINTS.
    
    Should always add error handling, by adding TRY...CATCH blocks, where SQL will try to execute the TRY block and only if there is 
    an error, it will execute the CATCH block.
    
    Also important to identify where are the bottlenecks (in the performance), so we can add the ETL Duration for each load, by using 
    @start and @end time variables. And to capture the total time for the whole load of the bronze layer, we define two new variables: 
    @batch_start and @batch_end times.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ryons\OneDrive\Documents\Data_Engineering\Tutorial\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ryons\OneDrive\Documents\Data_Engineering\Tutorial\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\ryons\OneDrive\Documents\Data_Engineering\Tutorial\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		PRINT '---------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\ryons\OneDrive\Documents\Data_Engineering\Tutorial\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\ryons\OneDrive\Documents\Data_Engineering\Tutorial\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ryons\OneDrive\Documents\Data_Engineering\Tutorial\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +' seconds';
		PRINT '-----------------------------';
		
		SET @batch_end_time = GETDATE();
		PRINT '========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '    - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) +' seconds';
		PRINT '========================================';
	END TRY
	BEGIN CATCH
		-- here we put what to do if there is an error, for example add it to a logging table or print a nice message for example:
		PRINT'=====================================================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'Error Message ' + ERROR_MESSAGE();
		PRINT'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error State ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'=====================================================================';
	END CATCH
END
