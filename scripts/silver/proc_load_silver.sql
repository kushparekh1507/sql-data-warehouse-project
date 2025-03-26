USE DataWarehouse
GO

  
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME
  BEGIN TRY
    SET @batch_start = GETDATE()
    PRINT '=============================' 
    PRINT 'Loading The Silver Layer'
    PRINT '============================='

    PRINT '-----------------------------' 
    PRINT 'Loading The CRM Tables'
    PRINT '-----------------------------' 

    SET @start_time = GETDATE();
    PRINT '>> Truncatng table: silver.crm_cust_info'
    TRUNCATE TABLE silver.crm_cust_info

    PRINT '>> Inserting data into: silver.crm_cust_info'
    INSERT INTO silver.crm_cust_info(
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_material_status,
      cst_gender,
      cst_create_date
    )
    SELECT
      cst_id,
      cst_key,
      TRIM(cst_firstname) as cst_firstname,
      TRIM(cst_lastname) as cst_lastname,
      CASE
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Married'
        ELSE 'n/a'
      END as cst_material_status,  -- Normalize martial status to readable format
      CASE
        WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
        ELSE 'n/a'
      END as cst_gender,
      cst_create_date
    FROM(
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
      FROM bronze.crm_cust_info
      where cst_id is not null
    )a 
    where flag = 1;  -- Deduplicate by cst_id and keep the latest record

    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'


    -- crp_prd_info

    SET @start_time = GETDATE();
    PRINT '>> Truncatng table: silver.crm_prd_info'
    TRUNCATE TABLE silver.crm_prd_info

    PRINT '>> Inserting data into: silver.crm_prd_info'
    INSERT INTO silver.crm_prd_info(
      prd_id,
      cat_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
    )
    SELECT
      prd_id,
      REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
      SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
      prd_nm,
      ISNULL(prd_cost, 0) as prd_cost,
      CASE UPPER(TRIM(prd_line))
          WHEN  'M' THEN 'Mountain'
          WHEN 'S' THEN 'Other Sales'
          WHEN 'R' THEN 'Road'
          WHEN 'T' THEN 'Touring'
          ELSE 'n/a'
      END as prd_line,
      CAST(prd_start_dt as date) as prd_start_dt,
      CAST(
        LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE
      ) AS prd_end_dt
    FROM bronze.crm_prd_info

    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    -- crp_sales_details

    SET @start_time = GETDATE();
    PRINT '>> Truncatng table: silver.crm_sales_details'
    TRUNCATE TABLE silver.crm_sales_details

    PRINT '>> Inserting data into: silver.crm_sales_details'
    INSERT INTO silver.crm_sales_details(
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
    )
    SELECT 
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
      END AS sls_order_dt,
      CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
      END AS sls_ship_dt,
      CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE)
      END AS sls_due_dt,
      CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
      END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
      sls_quantity,
      CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
      END AS sls_price -- Derive price if original value is invalid
    FROM bronze.crm_sales_details

    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    PRINT '-----------------------------' 
    PRINT 'Loading The ERP Tables'
    PRINT '-----------------------------' 

    -- erp_cust_az12

    SET @start_time = GETDATE();
    PRINT '>> Truncatng table: silver.erp_cust_az12'
    TRUNCATE TABLE silver.erp_cust_az12

    PRINT '>> Inserting data into: silver.erp_cust_az12'
    INSERT INTO silver.erp_cust_az12
    (cid, bdate, gen)
    SELECT
      CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
      END AS cid, -- Remove 'NAS' prefix if present
      CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
      END AS bdate, -- Set future birthdates to NULL
      CASE
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        ELSE 'n/a'
      END AS gen -- Normalize gender values and handle unknown cases
    FROM bronze.erp_cust_az12

    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    -- erp_loc_a101

    SET @start_time = GETDATE();
    PRINT '>> Truncatng table: silver.erp_loc_a101'
    TRUNCATE TABLE silver.erp_loc_a101

    PRINT '>> Inserting data into: silver.erp_loc_a101'
    INSERT INTO silver.erp_loc_a101
    (cid, cntry)
    SELECT
      REPLACE(cid,'-','') AS cid,
      CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA', 'United States') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
      END -- Normalize and Handle missing or blank country codes
    FROM bronze.erp_loc_a101

    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    --erp_px_cat_g1v2

    SET @start_time = GETDATE();
    PRINT '>> Truncatng table: silver.erp_px_cat_g1v2'
    TRUNCATE TABLE silver.erp_px_cat_g1v2

    PRINT '>> Inserting data into: silver.erp_px_cat_g1v2'
    INSERT INTO silver.erp_px_cat_g1v2
    (id, cat, subcat, maintenance)
    SELECT
      id,
      cat,
      subcat,
      maintenance
    FROM bronze.erp_px_cat_g1v2

    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    SET @batch_end = GETDATE();
    PRINT '============================='
    PRINT 'Time Taken To Load The Silver Layer: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR) + ' seconds'
    PRINT '============================='
  END TRY

  BEGIN CATCH
    PRINT '====================================='
    PRINT 'An error occurred: ' + ERROR_MESSAGE()
    PRINT '====================================='
  END CATCH
END
GO

EXEC silver.load_silver
