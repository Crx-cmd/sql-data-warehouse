--- Tests: bronze.erp_px_cat_g1v2;

-- Test: unwanted spaces
-- TRIM cat != TRIM(cat) = "Show me all the rows where the value in the cat column has extra spaces at the beginning or end."
SELECT * FROM bronze.erp_px_cat_g1v2 WHERE cat != TRIM(cat);

SELECT prd_nm FROM bronze.crm_prd_info WHERE prd_nm != TRIM(prd_nm);


-- Test: Data Standirezation & Consistency
SELECT DISTINCT subcat 
FROM bronze.erp_px_cat_g1v2;

-- Test: ID-Columns
-- SELECT DISTINCT SUBSTRING(id,1,2), cat = Set the distinct pairs of: The first two characters of the id column, and the corresponding cat value."
SELECT DISTINCT SUBSTRING(id,1,2), cat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT SUBSTRING(id,4,2), subcat FROM bronze.erp_px_cat_g1v2;

--- 
---- REMOVE letters inkl. CHECK cid
SELECT * FROM bronze.erp_custaz12;

--- "Show me all rows from erp_custaz12 where the customer ID (cid)—after removing the 'NAS' prefix, 
--- if it exists—is not found in the CRM's list of customer keys. Also include the cleaned-up 
--- version of the ID (cid_neu) and the gen column."
SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	ELSE cid END AS cid_neu,
	gen
FROM bronze.erp_custaz12 WHERE 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
		ELSE cid END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

--- Identify out-of-date-ranges
SELECT bdate FROM bronze.erp_custaz12 WHERE bdate < '1924-01-01' AND bdate > CURRENT_DATE;

---- DATA Standidazation & Consitency
SELECT 
	DISTINCT gen,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_custaz12;

--- Tests
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
	prd_nm,
	-- ISNULL = COALESCE
	COALESCE(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
    		WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a' 
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_stat_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

-- Convert zero or invalid date integers to NULL
-- Show rows where the sales order date is either 0 or not in proper YYYYMMDD format.
SELECT NULLIF(sls_order_dt,0) 
FROM bronze.crm_sales_details 
WHERE sls_order_dt <= 0 
   OR LENGTH(sls_order_dt::TEXT)!=8;

-- Convert zero or invalid date integers to NULL
-- Show rows where the sales order date is either 0 or not in proper YYYYMMDD format.
SELECT NULLIF(sls_order_dt,0) 
FROM bronze.crm_sales_details 
WHERE sls_order_dt <= 0 
   OR LENGTH(sls_order_dt::TEXT)!=8;

-- Check for invalid date order
--  Convert integer date fields to real dates (if valid), and return rows where the order date is after the ship date.
SELECT
CASE 
    WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
    ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
END  AS sls_order_dt,
CASE 
    WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
    ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
END  AS sls_ship_dt,
CASE 
    WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
    ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
END  AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_order_dt >= sls_ship_dt;

-- Validate sales calculations
-- Fix rows where sales or price are missing, negative, or not equal to quantity × price. Recalculate as needed.
SELECT 
sls_sales AS old_sls_sales,
sls_price AS old_sls_price,
sls_quantity,
CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales 
END AS sls_sales,
CASE 
    WHEN sls_price IS NULL OR sls_price <= 0
    THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price 
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0  
   OR sls_price <= 0 
ORDER BY sls_quantity, sls_price;