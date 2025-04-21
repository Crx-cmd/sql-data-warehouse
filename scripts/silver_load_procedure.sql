CREATE OR REPLACE PROCEDURE silver.load_silver_data()
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_start_time TIMESTAMP;
    v_batch_end_time TIMESTAMP;
    v_execution_time INTERVAL;
    v_operation_start_time TIMESTAMP;
    v_operation_end_time TIMESTAMP;
    v_operation_time INTERVAL;
BEGIN
	v_batch_start_time := clock_timestamp();
    RAISE NOTICE '--- Truncating Table: silver.crm_cust_info---';
    TRUNCATE silver.crm_cust_info;

	v_operation_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '--- Instering Table: silver.crm_cust_info ---';
    ---- INSERT INTO silver.crm_cust_info
    INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gender, cst_create_date)
    SELECT 
        cst_id, 
        cst_key, 
        TRIM(cst_firstname) as cst_fistname, 
        TRIM(cst_lastname) as cst_lastname, 
        CASE 
            WHEN UPPER(cst_material_status) = 'M' THEN 'Married'
            WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
            ELSE 'n/a'
        END cst_marital_status, 
        CASE 
            WHEN UPPER(cst_gender) = 'M' THEN 'Male'
            WHEN UPPER(cst_gender) = 'F' THEN 'Female'
            ELSE 'n/a'
        END cst_gender, 
        cst_create_date
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
        FROM bronze.crm_cust_info) t 
    WHERE flag_last = 1;

	v_operation_end_time := CURRENT_TIMESTAMP;
    v_operation_time := v_operation_end_time - v_operation_start_time;
	RAISE NOTICE 'Insert silver.crm_cust_info completed in: %', v_operation_time;

    RAISE NOTICE '--- Truncating Table: silver.crm_prd_info---';
	
    TRUNCATE silver.crm_prd_info;
	
    v_operation_start_time := CURRENT_TIMESTAMP;
	
    RAISE NOTICE '--- Instering Table: silver.crm_prd_info ---';
    ---- INSERT INTO silver.crm_prd_info
    INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    SELECT 
        prd_id,
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

	v_operation_end_time := CURRENT_TIMESTAMP;
    v_operation_time := v_operation_end_time - v_operation_start_time;
    RAISE NOTICE 'Insert silver.crm_prd_info completed in: %', v_operation_time;

    RAISE NOTICE '--- Truncating Table: silver.crm_sls_details---';
    TRUNCATE silver.crm_sls_details;
	
    v_operation_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '--- Instering Table: silver.crm_sls_details ---';
    ------ INSTERT INTO silver.crm_sls_details
    INSERT INTO silver.crm_sls_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        END AS sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price END AS sls_price
    FROM bronze.crm_sales_details; 

	v_operation_end_time := CURRENT_TIMESTAMP;
    v_operation_time := v_operation_end_time - v_operation_start_time;
    RAISE NOTICE 'Insert silver.crm_sales_details completed in: %', v_operation_time;

    RAISE NOTICE '--- Truncating Table: silver.erp_loc_a101---';
    TRUNCATE silver.erp_loc_a101;
    v_operation_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '--- Instering Table: silver.erp_loc_a101 ---';
    ---- INSRT INTO silver.erp_loc_a101
    INSERT INTO silver.erp_loc_a101(cid, cntry)
    SELECT 
        REPLACE(cid,'-','') cid,
        CASE 
            WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
            WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
            WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
            ELSE cntry END AS cntry
    FROM bronze.erp_loc_a101;

	v_operation_end_time := CURRENT_TIMESTAMP;
    v_operation_time := v_operation_end_time - v_operation_start_time;
    RAISE NOTICE 'Insert silver.erp_loc_a101 completed in: %', v_operation_time;
	
    RAISE NOTICE '--- Truncating Table: silver.erp_custaz12---';
    TRUNCATE silver.erp_custaz12;
    v_operation_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '--- Instering Table: silver.erp_custaz12 ---';
    ---- INSRT INTO silver.erp_custaz
    INSERT INTO silver.erp_custaz12(cid, bdate, gen)
    SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
        ELSE cid END AS cid,
        CASE WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
            WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_custaz12;

	v_operation_end_time := CURRENT_TIMESTAMP;
    v_operation_time := v_operation_end_time - v_operation_start_time;
    RAISE NOTICE 'Insert silver.erp_custaz12 completed in: %', v_operation_time;

    RAISE NOTICE '--- Truncating Table: silver.erp_px_cat_g1v2---';
    TRUNCATE silver.erp_px_cat_g1v2;
    v_operation_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '--- Instering Table: silver.erp_px_cat_g1v2 ---';
    ---- INSTERT INTO silver.erp_px_cat_g1v2
    INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
    SELECT 
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

	v_operation_end_time := CURRENT_TIMESTAMP;
    v_operation_time := v_operation_end_time - v_operation_start_time;
    RAISE NOTICE 'Insert silver.erp_px_cat_g1v2 completed in: %', v_operation_time;
    
    RAISE NOTICE 'Silver layer update completed';
	    -- Endzeit erfassen und Ausführungszeit berechnen
    v_batch_end_time := CURRENT_TIMESTAMP;
    v_execution_time := v_batch_end_time - v_batch_start_time;
    
    RAISE NOTICE 'Silver layer update completed';
    RAISE NOTICE 'Batch gestartet um: %', v_batch_start_time;
    RAISE NOTICE 'Batch beendet um: %', v_batch_end_time;
    RAISE NOTICE 'Ausführungszeit: %', v_execution_time;
END;
$$;

CALL silver.load_silver_data();

SELECT * FROM silver.crm_prd_info;