/*
==========================================
Quality Check of each Bronze Tables
==========================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'bronze' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks before starting transformation for Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
*/

/* Quality Check of crm_cust_info table*/

SELECT TOP 1000 *
FROM bronze.crm_cust_info

--1) Check for Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL --We have added NULL Check to see if we have single NULL

-- OUTPUT: Found some duplicates and 3 records where primary key is empty


--2) Check for Unwanted Space in string values
--Expectation: No Result

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != LTRIM(RTRIM(cst_firstname))

--  OUTPUT:15 Customers having space in their firstname

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != LTRIM(RTRIM(cst_lastname))

--  OUTPUT:17 Customers having space in their lastname

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != LTRIM(RTRIM(cst_gndr))

--  OUTPUT:No Result 

--3) Check the consistency of values in low cardinality columns (uniqueness of data values)

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

--  OUTPUT:3 DISTINCT unique values exists

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

--  OUTPUT:3 DISTINCT unique values exists


/* Quality Check of crm_prd_info table*/

SELECT TOP 1000 *
FROM bronze.crm_prd_info

--1) Check for Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL  

-- OUTPUT: No Result


--2) Filter out unmatched data after applying transformation

SELECT prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_')  NOT IN 
(SELECT id FROM bronze.erp_px_cat_g1v2)

-- OUTPUT: Found one category that is not part of crm_prd_info

SELECT prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7,LEN(prd_key)) NOT IN 
(SELECT sls_prd_key FROM bronze.crm_sales_details)

-- OUTPUT: Found Some products that don't have any orders

--3) Check for Unwanted Space in string values
--Expectation: No Result

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != LTRIM(RTRIM(prd_nm))

-- OUTPUT: No Result

--4) Check for -ve and NULL present in the prd_cost column
--Expectation: No Result

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- OUTPUT: 2 NULL value exists but no -ve value 

--5) Check for cardinality of the prd_cost column (Data Standardization & Consistency)
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- OUTPUT:5 distinct value exists including Null value  

--6) Check for Invalid Date Order
--Expectation: End date must not be earleir than the start date

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt<prd_start_dt

-- OUTPUT:200 results exists
--We have developed a logic to resolve this
SELECT prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(DATEADD(DAY, -1, prd_start_dt)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS new_date
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')


/* Quality Check of crm_sales_details table*/

SELECT TOP 1000 *
FROM bronze.crm_sales_details

--1) Check for Unwanted Space in string values
--Expectation: No Result

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num != LTRIM(RTRIM(sls_ord_num))

--OUTPUT: No Result

--2) Check for Product key prsence which will be utilise to join to table
--Expectation: No Result
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

--OUTPUT: No Result

--3) Check for cst id prsence which will be utilise to join to table
--Expectation: No Result
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

--OUTPUT: No Result

--Here we have one challenge that date is in integer format
--4) Check for -ve and 0 in date collumn since they cannot be cast to a date
--Expectation: No Result

SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<0
--OUTPUT: No -VE Result

SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0

--OUTPUT: 17 0s available
-- Let's make them null
SELECT NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0

--5) Check for the length of the date collumn since they cannot be cast to a date
--Check for Order Date Column

SELECT NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0 
OR LEN(sls_order_dt) !=8  
OR sls_order_dt>20500101 --CHECK for outliers by validating the boundaries of the date range
OR sls_order_dt<19000101 --CHECK for outliers by validating the boundaries of the date range

--OUTPUT: 2 Wrong Order Dates available

--Check for Ship Date Column
SELECT NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt<=0 
OR LEN(sls_ship_dt) !=8  
OR sls_ship_dt>20500101  
OR sls_ship_dt<19000101  

--OUTPUT: No Wrong Ship Date available

--Check for Due Date Column
SELECT NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt<=0 
OR LEN(sls_due_dt) !=8  
OR sls_due_dt>20500101  
OR sls_due_dt<19000101  

--OUTPUT: No Wrong Due Date available

--6) Order Date is always smaller than the shipping date or due date

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt>sls_due_dt

--OUTPUT: No Result

--7) Check Data Consistency: Between Sales, Quantity, and Price
-->> Sales=Quantity*Price
-->> Values must not be NULL, zero, or -ve
SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales!=sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

--OUTPUT: Lots of issue exists

--We have developed a logic to resolve this
--If Sales is -ve, 0, or NULL, deriving it using Quantity and Price
--If Price is 0 or NULL, calculating it using Sales and Quantity
--If Price is -ve, converting it to +ve value
SELECT DISTINCT
CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
	THEN sls_quantity*ABS(sls_price) ELSE sls_sales
	END sls_sales,
sls_quantity,
CASE WHEN sls_price<=0 OR sls_price IS NULL THEN sls_sales/NULLIF(sls_quantity,0)
	 ELSE sls_price
	 END sls_price
FROM bronze.crm_sales_details
WHERE sls_sales!=sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price
--OUTPUT: Everything is clean and perfect

/* Quality Check of erp_cust_az12 table*/

SELECT TOP 1000 *
FROM bronze.erp_cust_az12 

--1) Identify out-of-range Dates
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12 
WHERE bdate<'1924-01-01' OR bdate> GETDATE() 

--OUTPUT: 31 out-of-range dates exists

--2) Data Standardization & Consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12 
--OUTPUT: There are different variant exists need to resolve
--We have developed a logic to resolve this
SELECT DISTINCT gen,
CASE WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
	WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE') THEN 'Male'
	ELSE 'n/a'
END as gen
FROM bronze.erp_cust_az12 

/* Quality Check of erp_loc_a101 table*/

SELECT TOP 1000 *
FROM bronze.erp_loc_a101

--1) Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry
--OUTPUT: There are different variant exists need to resolve
--We have developed a logic to resolve this
SELECT DISTINCT cntry,
CASE 
WHEN LTRIM(RTRIM(cntry))='DE' THEN 'Germany'
WHEN LTRIM(RTRIM(cntry)) IN ('US','USA') THEN 'United States'
WHEN LTRIM(RTRIM(cntry))='' OR cntry IS NULL THEN 'n/a'
ELSE LTRIM(RTRIM(cntry))
END AS cntry
FROM bronze.erp_loc_a101
--OUTPUT: problem resolved

/* Quality Check of erp_px_cat_g1v2 table*/

SELECT * FROM bronze.erp_px_cat_g1v2

--1) Check for Unwanted Space in string values
--Expectation: No Result

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != LTRIM(RTRIM(cat)) OR subcat != LTRIM(RTRIM(subcat)) OR maintenance != LTRIM(RTRIM(maintenance))

--  OUTPUT: No Result

--2) Data Standardization & Consistency
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2
--OUTPUT: Looks fine

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2
--OUTPUT: Looks fine

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2
--OUTPUT: Looks fine
