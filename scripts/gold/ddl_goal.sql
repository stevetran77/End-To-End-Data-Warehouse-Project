-- CREATE view dim_customer
DROP VIEW IF EXISTS gold.dim_customer;
GO
CREATE VIEW gold.dim_customer AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- CREATE Surrgogate Key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr -- Final_Gender
			ELSE COALESCE(ea.gen,'N/A') 
	END AS gender,
	ea.bdate AS birthdate,
	ci.cst_create_date as create_date
FROM [silver].[crm_cust_info] ci
LEFT JOIN [silver].[erp_cust_az12] ea
	ON ci.cst_id = ea.cid
LEFT JOIN [silver].[erp_loc_a101] la
	ON ci.cst_id = la.cid

-- CREATE View Dim_product
DROP VIEW IF EXISTS gold.dim_product;
GO
CREATE VIEW gold.dim_product AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY ci.prd_start_dt, ci.prd_key) AS product_key,
	ci.prd_id AS product_id,
	ci.prd_key AS product_number,
	ci.prd_nm AS product_name,
	ci.cat_id AS category_id,
	ec.cat AS category,
	ec.subcat AS subcategory,
	ci.prd_cost AS cost,
	ci.prd_line AS product_line,
	ec.maintenance,
	ci.prd_start_dt AS start_date
FROM [silver].[crm_prd_info] ci
LEFT JOIN [silver].[erp_px_cat_g1v2] ec
		ON	ci.cat_id = ec.id
WHERE [prd_end_dt] IS NULL -- Filter out all historical data

-- CREATE VIEW fact_sales
DROP VIEW IF EXISTS gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
 SELECT 
		sd.sls_ord_num AS order_number
	  ,pr.product_key
	  ,cu.customer_key
      ,sd.[sls_order_dt] AS order_date
      ,sd.[sls_ship_dt] AS shipping_date
      ,sd.[sls_due_dt] AS due_date
      ,sd.[sls_sales] AS sales_amount
      ,sd.[sls_quantity] AS quantity
      ,sd.[sls_price] AS price
FROM [Datawarehouse].[silver].[crm_sales_details] sd
LEFT JOIN [gold].[dim_product] pr
	ON	sd. sls_prd_key = pr.product_number
LEFT JOIN [gold].[dim_customer] cu
	ON	sd.sls_cust_id = cu.customer_id

-- Foreign Key Intergrity 
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_product p
ON f.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL
