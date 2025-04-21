--- Setup Tables Source CRM

CREATE TABLE OR REPLACE bronze.crm_cust_info (
	cst_id INT, 
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_material_status VARCHAR(40),
	cst_gender VARCHAR (50),
	cst_create_date DATE
);

CREATE TABLE OR REPLACE bronze.crm_prd_info(
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

CREATE TABLE OR REPLACE bronze.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

--- Setup Tables Source CRM
CREATE TABLE OR REPLACE bronze.erp_custaz12(
	CID VARCHAR(50),
	BDATE DATE,
	GEN VARCHAR(50)
);

CREATE TABLE OR REPLACE bronze.erp_loc_a101(
	CID VARCHAR(50),
	CNTRY VARCHAR(50)

);

CREATE TABLE OR REPLACE bronze.erp_px_cat_g1v2(
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);


COPY bronze.crm_cust_info
FROM '/Users/heinerploog/Desktop/Data Warehouse/sql-data-warehouse-project/datasets/cust_info.csv'
WITH (FORMAT csv, HEADER true);

GRANT pg_read_server_files TO postgres;

