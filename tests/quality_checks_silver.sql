
-- QUALITY CHECK (doublons et valeurs nulles) FRO SILVER STAGE
-- PRIMARY KEY MUST BE UNIQUE AND NOT NULL

/* ---------------------------------------------
TABLE silver.crm_cust_info
-------------------------------------------------*/
SELECT cst_id, COUNT (*) AS doublons FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL ;

-- VISUALIZE samples of DOUBLES values to understand origin
SELECT * FROM silver.crm_cust_info
WHERE cst_id = 29466;

-- RANK AND PICK MOST RECENT VALUES
SELECT * FROM
(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rang
FROM silver.crm_cust_info)
t WHERE rang = 1;


SELECT cst_firsname FROM silver.crm_cust_info
WHERE cst_firsname != TRIM(cst_firsname);

SELECT cst_lastname FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT * FROM  silver.crm_cust_info

/* ---------------------------------------------
TABLE silver.crm_prd_info
-------------------------------------------------*/

SELECT prd_id, COUNT (*) AS doublons FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL ;

SELECT prd_nm FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--CKECKING UNWANTED SPACES NULLS OR NEGATIVE NUMBERS
SELECT prd_cost FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- 1st check values possibilities
SELECT DISTINCT prd_line FROM silver.crm_prd_info

--check dates orders
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--look
SELECT * FROM silver.crm_prd_info

/* ---------------------------------------------
TABLE silver.crm_sales_details
-------------------------------------------------*/

--CHECK DATES
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
OR sls_order_dt > sls_due_dt
;

-- CHECK business logic
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

---look 
SELECT * FROM silver.crm_sales_details;
