/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

SELECT * FROM gold.dim_products
SELECT * FROM gold.dim_customers
SELECT * FROM gold.fact_sales

-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;
--OUTPUT: No result obtained



-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;
--OUTPUT: No result obtained


--Foreign Key Integrity (Dimensions) Check
SELECT * 
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key=c.customer_key
WHERE c.customer_key IS NULL

--OUTPUT: No result obtained, it means everything is matching perfectly


-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key=c.customer_key
LEFT JOIN gold.dim_products p
ON s.product_key=p.product_key
WHERE p.product_key IS NULL

--OUTPUT: No result obtained, it means everything is matching perfectly
