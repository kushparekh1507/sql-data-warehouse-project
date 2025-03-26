USE DataWarehouse;

---- CRM Tables

-- Check For Nulls or Duplicates in Primary Key
  SELECT
    cst_id, 
    COUNT(*) 
  FROM bronze.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(*) > 1 OR CST_ID IS NULL;

-- Check for unwanted spaces
-- Epectation : No Results
  SELECT cst_firstname
  FROM bronze.crm_cust_info
  WHERE cst_firstname != TRIM(cst_firstname)


-- Data Standardization & Consistency
  SELECT DISTINCT cst_gender
  FROM bronze.crm_cust_info

SELECT
  cst_id, 
  COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR CST_ID IS NULL;


SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


SELECT DISTINCT cst_gender
FROM silver.crm_cust_info

SELECT *
FROM silver.crm_cust_info


-- Product Table
SELECT *
FROM bronze.crm_prd_info

-- Check For Nulls or Duplicates in Primary Key
SELECT 
  prd_id, 
  COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
  SELECT prd_nm
  FROM bronze.crm_prd_info
  WHERE PRD_NM != TRIM(prd_nm)

-- Check for Nulls Or Negative Numbers in cost
  SELECT prd_cost
  FROM bronze.crm_prd_info
  WHERE prd_cost IS NULL OR prd_cost < 0

-- Data Standardization & Consistency
  SELECT DISTINCT prd_line
  FROM bronze.crm_prd_info

-- Check for Invalid Order Dates
  SELECT *
  FROM bronze.crm_prd_info
  WHERE prd_start_dt > prd_end_dt

  SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS next_start_dt
  FROM bronze.crm_prd_info
  WHERE prd_key IN('AC-HE-HL-U509-R','AC-HE-HL-U509')

SELECT * 
FROM silver.crm_prd_info



-- Sales Details

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
WHERE sls_ord_num != TRIM(sls_ord_num)


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
WHERE sls_cust_id NOT IN(
  SELECT cst_id
  FROM silver.crm_cust_info
)

-- Check for Invalid Dates
SELECT 
  NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt < 0 
OR LEN(sls_ship_dt) !=8 
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales,Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, Zero or negative 
SELECT DISTINCT
  sls_sales AS old_sales,
  sls_quantity,
  sls_price as old_price,
  CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS sls_sales,
  CASE 
    WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
  END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales > 0 OR sls_quantity > 0 OR sls_price > 0



-- erp_cust_az12

SELECT 
  cid,
  bdate,
  gen
FROM bronze.erp_cust_az12
-- WHERE cid LIKE '%AW00011037%'

SELECT *
FROM silver.erp_cust_az12

-- Identify out of range Dates
SELECT *
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR 
bdate > GETDATE()

-- Data Standardization & Consistency
SELECT 
  DISTINCT gen,
  CASE
    WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
    WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
    ELSE 'n/a'
  END AS gen
FROM bronze.erp_cust_az12



-- erp_loc_a101

SELECT *
FROM bronze.erp_loc_a101

-- Data Standardization
SELECT 
  DISTINCT cntry AS old_cntry,
  CASE
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US','USA', 'United States') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
  END AS CNTRY
FROM bronze.erp_loc_a101

SELECT *
FROM silver.erp_loc_a101


-- erp_px_cat_g1v2

SELECT *
FROM bronze.erp_px_cat_g1v2

-- Check for unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

-- Data Standardrization & Conistency
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

SELECT *
FROM silver.erp_px_cat_g1v2

