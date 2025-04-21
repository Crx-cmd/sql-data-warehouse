SELECT inet_server_addr(), inet_server_port();

SELECT current_user;
SELECT current_database();

SELECT * FROM bronze.crm_cust_info;

GRANT pg_read_server_files TO heinerploog;


CREATE OR REPLACE PROCEDURE bronze.load_bronze_data()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_execution_time INTERVAL;
	v_batch_start_time TIMESTAMP;
	v_batch_end_time TIMESTAMP;
BEGIN
    -- Anzeige, dass das Laden des Bronze Layers startet
    RAISE NOTICE '--- LOADING BRONZE LAYER ---';

	v_batch_start_time := clock_timestamp();

    -- ERP-Daten laden
    RAISE NOTICE '--- LOADING BRONZE LAYER: ERP-DATA ---';

    -- Tabelle 1 laden
    BEGIN
        TRUNCATE bronze.erp_custaz12;
        v_start_time := clock_timestamp(); -- Startzeit erfassen
        EXECUTE format(
            'COPY bronze.erp_custaz12 FROM %L WITH (FORMAT csv, HEADER true)',
            '/tmp/datasets/source_erp/CUST_AZ12.csv'
        );
        v_end_time := clock_timestamp(); -- Endzeit erfassen
        v_execution_time := v_end_time - v_start_time; -- Berechnung der Ausführungszeit
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        RAISE NOTICE 'Rows affected by COPY into bronze.erp_custaz12: %, Execution time: %', v_rows_affected, v_execution_time;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading data into bronze.erp_custaz12: %', SQLERRM;
    END;

    -- Tabelle 2 laden
    BEGIN
        TRUNCATE bronze.erp_loc_a101;
        v_start_time := clock_timestamp(); -- Startzeit erfassen
        EXECUTE format(
            'COPY bronze.erp_loc_a101 FROM %L WITH (FORMAT csv, HEADER true)',
            '/tmp/datasets/source_erp/LOC_A101.csv'
        );
        v_end_time := clock_timestamp(); -- Endzeit erfassen
        v_execution_time := v_end_time - v_start_time; -- Berechnung der Ausführungszeit
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        RAISE NOTICE 'Rows affected by COPY into bronze.erp_loc_a101: %, Execution time: %', v_rows_affected, v_execution_time;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading data into bronze.erp_loc_a101: %', SQLERRM;
    END;

    -- Tabelle 3 laden
    BEGIN
        TRUNCATE bronze.erp_px_cat_g1v2;
        v_start_time := clock_timestamp(); -- Startzeit erfassen
        EXECUTE format(
            'COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT csv, HEADER true)',
            '/tmp/datasets/source_erp/PX_CAT_G1V2.csv'
        );
        v_end_time := clock_timestamp(); -- Endzeit erfassen
        v_execution_time := v_end_time - v_start_time; -- Berechnung der Ausführungszeit
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        RAISE NOTICE 'Rows affected by COPY into bronze.erp_px_cat_g1v2: %, Execution time: %', v_rows_affected, v_execution_time;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading data into bronze.erp_px_cat_g1v2: %', SQLERRM;
    END;

    -- CRM-Daten laden
    RAISE NOTICE '--- LOADING BRONZE LAYER: CRM-DATA ---';

    -- Tabelle 4 laden
    BEGIN
        TRUNCATE bronze.crm_sales_details;
        v_start_time := clock_timestamp(); -- Startzeit erfassen
        EXECUTE format(
            'COPY bronze.crm_sales_details FROM %L WITH (FORMAT csv, HEADER true)',
            '/tmp/datasets/source_crm/sales_details.csv'
        );
        v_end_time := clock_timestamp(); -- Endzeit erfassen
        v_execution_time := v_end_time - v_start_time; -- Berechnung der Ausführungszeit
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        RAISE NOTICE 'Rows affected by COPY into bronze.sales_details: %, Execution time: %', v_rows_affected, v_execution_time;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading data into bronze.sales_details: %', SQLERRM;
    END;

    -- Tabelle 5 laden
    BEGIN
        TRUNCATE bronze.crm_prd_info;
        v_start_time := clock_timestamp(); -- Startzeit erfassen
        EXECUTE format(
            'COPY bronze.crm_prd_info FROM %L WITH (FORMAT csv, HEADER true)',
            '/tmp/datasets/source_crm/prd_info.csv'
        );
        v_end_time := clock_timestamp(); -- Endzeit erfassen
        v_execution_time := v_end_time - v_start_time; -- Berechnung der Ausführungszeit
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        RAISE NOTICE 'Rows affected by COPY into bronze.prd_info: %, Execution time: %', v_rows_affected, v_execution_time;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading data into bronze.prd_info: %', SQLERRM;
    END;

    -- Tabelle 6 laden
    BEGIN
        TRUNCATE bronze.crm_cust_info;
        v_start_time := clock_timestamp(); -- Startzeit erfassen
        EXECUTE format(
            'COPY bronze.crm_cust_info FROM %L WITH (FORMAT csv, HEADER true)',
            '/tmp/datasets/source_crm/cust_info.csv'
        );
        v_end_time := clock_timestamp(); -- Endzeit erfassen
        v_execution_time := v_end_time - v_start_time; -- Berechnung der Ausführungszeit
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        RAISE NOTICE 'Rows affected by COPY into bronze.crm_cust_info: %, Execution time: %', v_rows_affected, v_execution_time;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading data into bronze.crm_cust_info: %', SQLERRM;
    END;

    -- Abschlussmitteilung
	v_batch_end_time := clock_timestamp();
	v_execution_time := v_batch_end_time - v_batch_start_time; -- Berechnung der Ausführungszeit
    RAISE NOTICE '--- ALL DATA LOADED INTO BRONZE LAYER --- Execution Time: %', v_execution_time;
	

END $$;

