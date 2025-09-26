select Distinct
	ci.cst_gndr,
	CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		WHEN ci.cst_gndr = 'n/a' AND  ca.gen IS NOT NULL THEN ca.gen
		ELSE ci.cst_gndr
	END as new_gen,
	ca.gen
from silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid
order by 1,2
--=====================================
select * from gold.fact_sales

--======INTEGRATION check=============
select *
from gold.fact_sales as dp
left join gold.dim_product as pd
on dp.product_number = pd.product_number
left join gold.dim_customer as c
on dp.customer_id=c.customer_id
--where dp.product_number is null
where dp.customer_id is null
