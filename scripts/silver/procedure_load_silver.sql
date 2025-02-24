
CREATE OR ALTER PROCEDURE silver.load_silver_crm AS
BEGIN
	SELECT * FROM bronze.crm_cust_info

	-- BUILDING SILVER TABLES from BRONZE STAGE
	-- INCLUDING SOME VERIFICATIONS
	-- PRIMARY KEY MUST BE UNIQUE AND NOT NULL
	/* ---------------------------------------------
	TABLE bronze.crm_cust_info
	-------------------------------------------------*/
	SELECT cst_id, COUNT (*) AS doublons FROM bronze.crm_cust_info
	GROUP BY cst_id
	HAVING COUNT(*) > 1 OR cst_id IS NULL ;

	-- VISUALIZE samples of DOUBLES values to understand origin
	SELECT * FROM bronze.crm_cust_info
	WHERE cst_id = 29466;

	-- RANK AND PICK MOST RECENT VALUES
	SELECT * FROM
	(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rang
	FROM bronze.crm_cust_info)
	t WHERE rang = 1;                --to ckeck an example addd:  AND cst_id = 29466

	-- CHECK FOR UNWANTED SPACES IN STRING VALUES
	SELECT cst_firsname FROM bronze.crm_cust_info
	WHERE cst_firsname != TRIM(cst_firsname);

	SELECT cst_lastname FROM bronze.crm_cust_info
	WHERE cst_lastname != TRIM(cst_lastname);

	SELECT cst_gndr FROM bronze.crm_cust_info
	WHERE cst_gndr != TRIM(cst_gndr);

	-- CLEANING UNWANTED SPACES
	TRUNCATE TABLE silver.crm_cust_info
	INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firsname, cst_lastname, cst_material_status, cst_gndr, cst_create_date)
	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firsname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	-- DATA STANDARDIZATION
	-- transform abbreviations into full names
	CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single' 
		WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Maried'
		ELSE 'n/a'
	END cst_material_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM 
	(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rang
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL)
	t WHERE rang = 1;

	--CHECK DATA CONSISTENCY FOR ONE CHARACTER COLUMNS
	-- 1st check values possibilities
	SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info

	/* ==========================================================
	TABLE bronze.crm_prd_info
	==============================================================*/

	SELECT prd_id, COUNT (*) AS doublons FROM bronze.crm_prd_info
	GROUP BY prd_id
	HAVING COUNT(*) > 1 OR prd_id IS NULL ;

	-- ABOUT prd_key , DATA NEEDS TO BE SPLITTED INTO 2 NEW COLUMNS
	TRUNCATE TABLE silver.crm_prd_info
	INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
	SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- SPLIT COLUMN FROM SPECIFIED CHARATER POSITION
	SUBSTRING (prd_key, 7, LEN(prd_key)) AS prd_key ,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'R' THEN 'Road'
		WHEN 'M' THEN 'Montain'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info
	/* WHERE SUBSTRING (prd_key, 7, LEN(prd_key)) IN
	-- 1st matching id's
	(SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details)
	*/
	;

	-- we need to join this table with sales_details so lets check sales details 
	SELECT * FROM bronze.crm_sales_details;

	--CKECKING UNWANTED SPACES NULLS OR NEGATIVE NUMBERS
	SELECT prd_cost FROM bronze.crm_prd_info
	WHERE prd_cost < 0 OR prd_cost IS NULL;

	-- CHECKING   for invalid date orders
	SELECT * FROM bronze.crm_prd_info
	WHERE prd_end_dt < prd_start_dt;

	--test sur colonnes de dates
	SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_line,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS test
	FROM bronze.crm_prd_info
	WHERE prd_key IN ('AC-HE-HL-U509-R' , 'AC-HE-HL-U509');


	/* ==========================================================
	TABLE bronze.crm_sales_details
	==============================================================*/


	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	FROM bronze.crm_sales_details
	WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info) -- empty so we can connect the tables
	;


	TRUNCATE TABLE silver.crm_sales_details
	INSERT INTO silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)  --SEUL MOYEN DE FORMATER DES INTEGER EN DATETIME
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)  --SEUL MOYEN DE FORMATER DES INTEGER EN DATETIME
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)  --SEUL MOYEN DE FORMATER DES INTEGER EN DATETIME
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price) 
		ELSE sls_sales
	END AS sls_sales, 
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;


	--- CHECKING INVALID DATES
	-- dates are integer so we need to change their format
	SELECT NULLIF(sls_order_dt, 0 ) FROM bronze.crm_sales_details
	WHERE sls_order_dt <= 0     -- those values cant be changed to datetime SO WE REPLACE WITH "NULL"
	OR  LEN(sls_order_dt) != 8 
	OR sls_order_dt > 20500101
	OR sls_order_dt < 19000101
	;

	SELECT NULLIF(sls_order_dt, 0 ) FROM bronze.crm_sales_details
	WHERE sls_ship_dt <= 0     -- those values cant be changed to datetime SO WE REPLACE WITH "NULL"
	OR  LEN(sls_ship_dt) != 8 
	OR sls_ship_dt > 20500101
	OR sls_ship_dt < 19000101
	;

	SELECT NULLIF(sls_order_dt, 0 ) FROM bronze.crm_sales_details
	WHERE sls_due_dt <= 0     -- those values cant be changed to datetime SO WE REPLACE WITH "NULL"
	OR  LEN(sls_due_dt) != 8 
	OR sls_due_dt > 20500101
	OR sls_due_dt < 19000101
	;

	--- CHECKING INVALID DATES ORDERS
	SELECT * FROM bronze.crm_sales_details
	WHERE sls_order_dt > sls_ship_dt 
	OR sls_order_dt > sls_due_dt ;

	--- CHECK QUANTITATIVE COLUMNS : SALES, QTE, PRICE
	SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price AS old_price,

	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price) 
		ELSE sls_sales
	END AS new_sls_sales

	FROM bronze.crm_sales_details
	WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL
	OR sls_quantity IS NULL
	OR sls_price IS NULL
	OR sls_sales <= 0 
	OR sls_quantity <= 0
	OR sls_price <= 0
	ORDER BY sls_sales, sls_quantity, sls_price
	;

	-- new colums with correct data
	SELECT DISTINCT
	sls_sales AS sls_old_sales,
	sls_quantity,
	sls_price AS sls_old_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price) 
		ELSE sls_sales
	END AS sls_sales, 
	CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;

-- BUILDING SILVER TABLES from BRONZE STAGE
-- INCLUDING SOME VERIFICATIONS
-- PRIMARY KEY MUST BE UNIQUE AND NOT NULL
/* ---------------------------------------------
TABLE bronze.erp_cust_az12
-------------------------------------------------*/

INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
SELECT 
CASE WHEN cid LIKE'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

-- at this stage we remenber to connect the previous tables with the ERP TABLES
SELECT * FROM [silver].[crm_cust_info];
-- we can match cst_key column and cid but we need to remove 3 first characters from column cid
-- CHECK BDATE VALIDITY
SELECT bdate FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

--CHECKER LE GENRE
SELECT DISTINCT gen FROM bronze.erp_cust_az12
;

/* ---------------------------------------------
TABLE silver.erp_loc_a101
-------------------------------------------------*/
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
REPLACE(cid, '-', '') AS cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry -- NORMALIZE AND HANDLE MISSING VALUES
FROM bronze.erp_loc_a101;

SELECT cst_key FROM silver.crm_cust_info;

--CHECKER LE GENRE
SELECT DISTINCT cntry FROM bronze.erp_loc_a101
ORDER BY cntry;

/* ---------------------------------------------
TABLE silver.erp_px_cat_g1v2
-------------------------------------------------*/

INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

--CHECK VALUES
SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2
ORDER BY subcat;
-- CHECK SPACES
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

SELECT * FROM silver.erp_px_cat_g1v2;
END
