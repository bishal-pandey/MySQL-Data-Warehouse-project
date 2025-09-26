/*
===================================================================================
DDL Script: Create Gold Views
===================================================================================
Script Purpose:
  This script creates views for Gold Layer in the data warehouse.
  The Gold Layer represent the final dimensions and fact table(star schema)

  Each views perform transformation and combines data from Silver Layer to produce
  clean, enriched and business-ready dataset.

Usage:
  Views can be queried directly for analytics and reporting
======================================================================================
*/

/*=================================================
-- Create Dimension : gold.dim_product
=================================================
*/
CREATE VIEW gold.dim_product AS
	select
		ROW_NUMBER() over(ORDER BY prd_key) AS product_key,
		pdi.prd_id AS product_id,
		pdi.prd_key AS product_number,
		pdi.prd_nm AS product_name,
		pdi.cat_key AS category_id,
		pcg.cat AS category,
		pcg.subcat AS subcategory,
		pcg.maintenance,
		pdi.prd_cost AS product_cost,
		pdi.prd_line AS product_line,
		pdi.prd_start_dt AS product_start_date

	from silver.crm_prd_info AS pdi
	LEFT JOIN silver.erp_px_cat_g1v2 AS PCG
	ON pdi.cat_key = pcg.id
	where prd_end_dt IS  NULL   --Filter out all historical data

/*=================================================
-- Create dimension : gold.dim_customer
=================================================
*/
CREATE VIEW gold.dim_customer AS
select
	ROW_NUMBER() OVER(order by ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	
	CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		WHEN ci.cst_gndr = 'n/a' AND  ca.gen IS NOT NULL THEN ca.gen
		ELSE ci.cst_gndr
	END AS gender,

	ca.bdate AS birth_date,
	ci.cst_create_date AS create_date

from silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid

/*=================================================
-- Create Dimension : gold.fact_sales
=================================================
*/
CREATE VIEW gold.fact_sales AS
	select 
		sd.sls_ord_num AS order_number,
		cu.customer_key,
		pd.product_key,
		sd.sls_sales AS sales,
		sd.sls_quantity AS quantity,
		sd.sls_price AS price,
		sd.sls_order_dt AS order_date,
		sd.sls_ship_dt AS ship_date,
		sd.sls_due_dt AS due_date

	from silver.crm_sales_details AS sd
	LEFT JOIN gold.dim_product AS pd
	ON sd.sls_prd_key=pd.product_number
	LEFT JOIN gold.dim_customer AS cu
	ON sd.sls_cust_id = cu.customer_id
