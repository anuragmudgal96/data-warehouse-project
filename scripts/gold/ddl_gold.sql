/*
==========================================
DDL Script : Create Gold Views
==========================================

Script Purpose:
	This script creates views for the Gold Layer in the Data Warehouse
  The Gold Layer represents the final dimension and fact table. (Start Schema)

  Each view perform transformations and combine data from the silver layer
  to prepare clear and crisp dataset for business ready purpose.

  Usage:
  You can directly use these views for the analytics purpose.
*/




==========================================
-- Create Dimension: gold.dim_customers
==========================================
IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, --Surrogate Key
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS martial_status,
CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr --CRM is the master for gender info
	ELSE ISNULL(ca.gen,'n/a') 
END as gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid
GO
==========================================
-- Create Dimension: gold.dim_products
==========================================
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER (ORDER BY prd_start_dt,prd_key) AS product_key, --Surrogate Key
prd_id AS product_id,
prd_key AS product_number,
prd_nm AS product_name,
cat_id AS category_id,
pcat.cat AS category,
pcat.subcat AS subcategory,
pcat.maintenance,
prd_cost AS cost,
prd_line AS product_line,
prd_start_dt AS start_date
FROM silver.crm_prd_info pinfo
LEFT JOIN silver.erp_px_cat_g1v2 pcat
ON pinfo.cat_id=pcat.id
WHERE prd_end_dt IS NULL -- Filter out all historical data
GO
==========================================
-- Create Fact: gold.fact_sales
==========================================
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT 
sls_ord_num AS order_number,
product_key,
customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date,
sls_sales AS sales_amount,
sls_quantity AS quantity,
sls_price price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers c
ON sd.sls_cust_id=c.customer_id
LEFT JOIN gold.dim_products p
ON sd.sls_prd_key=p.product_number
GO
