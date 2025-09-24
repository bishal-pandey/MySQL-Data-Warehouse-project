/*
	Stored Procedure: Load silver layer
	
	This procedure perfrom ETL(Extract Transform Load) process to populate data into "silver" schema from 'bronze' schema
	
  First it Truncate the silver tables before loading data.
	Insert transformed and cleaned data Bronze into Silver tables

	Parameters:None
	Usage Example: EXEC silver.load_silver
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
	
		PRINT '==========================================================================================';
		PRINT 'Load Silver Layer';
		PRINT '==========================================================================================';
	
		PRINT '==========================================================================================';
		PRINT 'Loading CRM Tables';
		PRINT '==========================================================================================';
		PRINT 'INSERTING DATA INTO silver.crm_cust_info';

		TRUNCATE TABLE silver.crm_cust_info;
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
			CASE
				WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'single' 
				WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'single'
				ELSE 'n/a'
			END cst_marital_status,

			CASE
				WHEN UPPER(TRIM(cst_gndr))='M' THEN 'male' 
				WHEN UPPER(TRIM(cst_gndr))='F' THEN 'female'
				ELSE 'n/a'
			END cst_gndr,

			cst_create_date
		FROM(
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last

			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)T WHERE flag_last=1;


		--===================================crm_prd_info=================================================
		PRINT 'INSERTING DATA INTO silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_key,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_key,
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST (prd_start_dt AS DATE) AS prd_start_dt,

			CAST(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 AS DATE) AS prd_end_dt
		from bronze.crm_prd_info ;

		--================================CRM_SALES_DETAILS==================================================
		PRINT 'INSERTING DATA INTO silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
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
		CASE 
			WHEN LEN(sls_order_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN LEN(sls_ship_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN LEN(sls_due_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

		CASE
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,

		sls_quantity,

		CASE
		WHEN sls_price IS NULL OR sls_price = 0THEN   ABS(sls_sales)/NULLIF(sls_quantity,0)
			WHEN  sls_price < 0 THEN ABS(sls_price)
			ELSE sls_price
		END AS sls_price

		from bronze.crm_sales_details;


		--=========================================ERP_CUST_AZ12=================================================================
		PRINT '==========================================================================================';
		PRINT 'Loading ERP Tables';
		PRINT '==========================================================================================';


		PRINT 'INSERTING DATA INTO silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen	
		)
		SELECT
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END as cid,

		CASE 
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END bdate,

		CASE
			WHEN TRIM(UPPER(gen)) IN  ('F','FEMALE') THEN 'female'
			WHEN TRIM(UPPER(gen)) IN  ('M','MALE')  THEN 'male'
			ELSE 'n/a'
		END gen

		from bronze.erp_cust_az12;

		--=========================ERP_LOC_A101=======================================================
		PRINT 'INSERTING DATA INTO silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101 (
				cid,
				cntry
		)
		select 
			REPLACE(cid,'-','') as cid,
	
			CASE 
				WHEN UPPER(TRIM(cntry))='DE' THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
				WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry

		from bronze.erp_loc_a101;

		--=====================================ERP_PX_CAT_G1V2=======================================================
		PRINT 'INSERTING DATA INTO silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance

		)
		select
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2;
	END TRY

	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR)
	END CATCH
END;

EXEC silver.load_silver
