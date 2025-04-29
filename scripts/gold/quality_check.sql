/*
==========================================
Quality Check of Dimension Tables
==========================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization for the new dimensions created after joining tables. 
    It includes checks for:
    - Duplicate Check 
    - Data Integration 

Usage Notes:
    - Run these checks after joining tables.
    - Investigate and resolve any discrepancies found during the checks.
*/


====================================
----Dimension:Customers Info Table
====================================
--Check if any duplicate were introduced by the join logic

SELECT cst_id, COUNT(*) FROM
(SELECT
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid) A
GROUP BY cst_id
HAVING COUNT(*)>1

--Output: NO Duplicate exists

--Data Integration since we have two column giving gender detail

SELECT DISTINCT
ci.cst_gndr,
ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid
Order BY 1,2

--OUTPUT: There is data discrepency visible
-- We have to resolve this and we are giving priority to CRM data

SELECT DISTINCT
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr --CRM is the master for gender info
	ELSE ISNULL(ca.gen,'n/a') 
END as gen2
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid
Order BY 1,2

========================================
--Dimension:Product Info Table 
========================================
--Check if any duplicate were introduced by the join logic

SELECT prd_id, COUNT(*)
FROM(
SELECT prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
pcat.cat,
pcat.subcat,
pcat.maintenance
FROM silver.crm_prd_info pinfo
LEFT JOIN silver.erp_px_cat_g1v2 pcat
ON pinfo.cat_id=pcat.id
WHERE prd_end_dt IS NULL) A
GROUP BY prd_id
HAVING COUNT(*)>1

--Output: NO Duplicate exists

