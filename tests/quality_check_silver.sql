/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-------------------------------------------
--Quality Check of crm_cust_info table
-------------------------------------------

SELECT TOP 1000 *
FROM silver.crm_cust_info
WHERE cst_id IS NULL

--1) Check for Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL --We have added NULL Check to see if we have single NULL

-- OUTPUT: No Result

--2) Check for Unwanted Space in string values
--Expectation: No Result

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != LTRIM(RTRIM(cst_firstname))

--  OUTPUT:No Result

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != LTRIM(RTRIM(cst_lastname))

--  OUTPUT:No Result

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != LTRIM(RTRIM(cst_gndr))

--  OUTPUT:No Result 

--3) Check the consistency of values in low cardinality columns (uniqueness of data values)

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

--  OUTPUT:3 DISTINCT unique values

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

--  OUTPUT:3 DISTINCT unique values

-----------------------------------------------
--Quality Check of silver.crm_prd_info table
-----------------------------------------------

--1) Check for Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL  

-- OUTPUT: No Result

--2) Check for Unwanted Space in string values
--Expectation: No Result

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != LTRIM(RTRIM(prd_nm))

-- OUTPUT: No Result

--3) Check for -ve and NULL present in the prd_cost column
--Expectation: No Result

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- OUTPUT: 2 NULL value exists but no -ve value 

--4) Check for cardinality of the prd_cost column (Data Standardization & Consistency)
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--5) Check for Invalid Date Order
--Expectation: End date must not be earleir than the start date

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt<prd_start_dt

-- OUTPUT: No Result

------------------------------------------------------
--Quality Check of silver.crm_sales_details table
------------------------------------------------------

--1) Check for Invalid Date Order
--Expectation: End date must not be earleir than the start date

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt>sls_due_dt
-- OUTPUT: No Result

--2) Check Data Consistency: Between Sales, Quantity, and Price
-->> Sales=Quantity*Price
-->> Values must not be NULL, zero, or -ve

SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales!=sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

-- OUTPUT: No Result

SELECT * FROM silver.crm_sales_details

------------------------------------------------
--Quality Check of silver.erp_cust_az12  table
------------------------------------------------

--1) Identify out-of-range Dates
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12 
WHERE bdate<'1924-01-01' OR bdate> GETDATE() 

--OUTPUT: 31 out-of-range dates exists

--2) Data Standardization & Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12 

SELECT * FROM silver.erp_cust_az12 

--------------------------------------------------
--Quality Check of silver.erp_loc_a101  table
--------------------------------------------------

--1) Data Standardization & Consistency

SELECT DISTINCT cntry
FROM silver.erp_loc_a101
