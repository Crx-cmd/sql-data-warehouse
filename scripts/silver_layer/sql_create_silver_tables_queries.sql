--- Setup Tables Source CRM

DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
	cst_id INT, 
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(40),
	cst_gender VARCHAR (50),
	cst_create_date DATE,
	dwh_create_date DATE DEFAULT CURRENT_DATE
);

SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info GROUP BY cst_id HAVING COUNT(*) >1 OR cst_id IS NULL;

SELECT * 
	FROM(
	SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
		FROM bronze.crm_cust_info) t 
	WHERE flag_last = 1;

CREATE TABLE OR REPLACE silver.crm_prd_info(
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATE DEFAULT CURRENT_DATE

);

CREATE TABLE OR REPLACE silver.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATE DEFAULT CURRENT_DATE

);

--- Setup Tables Source CRM
CREATE TABLE OR REPLACE silver.erp_custaz12(
	CID VARCHAR(50),
	BDATE DATE,
	GEN VARCHAR(50),
	dwh_create_date DATE DEFAULT CURRENT_DATE

);

CREATE TABLE OR REPLACE silver.erp_loc_a101(
	CID VARCHAR(50),
	CNTRY VARCHAR(50),
	dwh_create_date DATE DEFAULT CURRENT_DATE


);

CREATE TABLE OR REPLACE silver.erp_px_cat_g1v2(
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50),
	dwh_create_date DATE DEFAULT CURRENT_DATE

);
