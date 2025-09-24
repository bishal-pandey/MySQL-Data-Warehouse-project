/*
	Stored Procedure: Load Bronze layer
	
	This procedure load the data into "bronze" schema from csv files
	First it Truncate the bronze tables before loading data.
	Use BULK INSERT command to load data from csv files to bronze tables

	Parameters:None
	Usage Example: EXEC bronze.load_bronze
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

	DECLARE @start_time DATETIME,@end_time DATETIME;

	PRINT '================================';
	PRINT 'Loading Bronze Layer';
	PRINT '================================';

	PRINT '================================';
	PRINT 'Loading CRM Tables';
	PRINT '================================';


	PRINT 'Inserting Data int: bronze.crm_cust_info';
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT	bronze.crm_cust_info
	FROM 'C:\Users\Dell\Downloads\dwh_project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT 'Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR(20)) + ' seconds';

	PRINT '';PRINT '---------------------------------------------------------';
	PRINT 'Inserting Data int: bronze.crm_prd_info';
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT	bronze.crm_prd_info
	FROM 'C:\Users\Dell\Downloads\dwh_project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT 'Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR(20)) + ' seconds';

	PRINT '';PRINT '---------------------------------------------------------';
	PRINT 'Inserting Data int: bronze.crm_sales_details';
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT	bronze.crm_sales_details
	FROM 'C:\Users\Dell\Downloads\dwh_project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT 'Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR(20)) + ' seconds';


	PRINT '================================';
	PRINT 'Loading CRM Tables';
	PRINT '================================';

	PRINT '';PRINT '---------------------------------------------------------';
	PRINT 'Inserting Data int: bronze.erp_cust_az12';
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_cust_az12;
	BULK INSERT	bronze.erp_cust_az12
	FROM 'C:\Users\Dell\Downloads\dwh_project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT 'Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR(20)) + ' seconds';


	PRINT '';PRINT '---------------------------------------------------------';
	PRINT 'Inserting Data int: bronze.erp_loc_a101';
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT	bronze.erp_loc_a101
	FROM 'C:\Users\Dell\Downloads\dwh_project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT 'Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR(20)) + ' seconds';


	PRINT '';PRINT '---------------------------------------------------------';
	PRINT 'Inserting Data int: bronze.erp_px_cat_g1v2';
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	BULK INSERT	bronze.erp_px_cat_g1v2
	FROM 'C:\Users\Dell\Downloads\dwh_project\datasets\source_erp\PX_CAT_G1v2.csv'
	WITH (
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT 'Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR(20)) + ' seconds';

END



EXEC bronze.load_bronze;
