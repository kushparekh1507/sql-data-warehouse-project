
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_start DATETIME, @bronze_end DATETIME
  BEGIN TRY
    SET @bronze_start = GETDATE();
    PRINT '=============================' 
    PRINT 'Loading The Bronze Layer'
    PRINT '============================='

    PRINT '-----------------------------' 
    PRINT 'Loading The CRM Tables'
    PRINT '-----------------------------' 

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.crm_cust_info'
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT '>> Inserting Table: bronze.crm_cust_info'
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\DELL\OneDrive\Desktop\data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR=',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.crm_prd_info'
    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT '>> Inserting Table: bronze.crm_prd_info'
    BULK INSERT bronze.crm_prd_info
    FROM
    'C:\Users\DELL\OneDrive\Desktop\data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR=',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.crm_sales_details'
    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT '>> Inserting Table: bronze.crm_sales_details'
    BULK INSERT bronze.crm_sales_details
    FROM
    'C:\Users\DELL\OneDrive\Desktop\data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR=',',
      TABLOCK
    )
    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'

    PRINT '-----------------------------' 
    PRINT 'Loading The ERP Tables'
    PRINT '-----------------------------' 

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_cust_az12'
    TRUNCATE TABLE bronze.erp_cust_az12;

    PRINT '>> Inserting Table: bronze.erp_cust_az12'
    BULK INSERT bronze.erp_cust_az12
    FROM
    'C:\Users\DELL\OneDrive\Desktop\data-warehouse-project\datasets\source_erp\cust_az12.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR=',',
      TABLOCK
    )
    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_loc_a101'
    TRUNCATE TABLE bronze.erp_loc_a101;

    PRINT '>> Inserting Table: bronze.erp_loc_a101'
    BULK INSERT bronze.erp_loc_a101
    FROM
    'C:\Users\DELL\OneDrive\Desktop\data-warehouse-project\datasets\source_erp\loc_a101.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR=',',
      TABLOCK
    )
    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    PRINT '>> Inserting Table: bronze.erp_px_cat_g1v2'
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM
    'C:\Users\DELL\OneDrive\Desktop\data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
    WITH(
      FIRSTROW = 2,
      FIELDTERMINATOR=',',
      TABLOCK
    )
    SET @end_time = GETDATE();
    PRINT '>> Time Taken: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '-----------------------------'

    SET @bronze_end = GETDATE();
    PRINT '============================='
    PRINT 'Time Taken To Load The Bronze Layer: ' + CAST(DATEDIFF(SECOND, @bronze_start, @bronze_end) AS NVARCHAR) + ' seconds'
    PRINT '============================='
  END TRY

  BEGIN CATCH
    PRINT '====================================='
    PRINT 'An error occurred: ' + ERROR_MESSAGE()
    PRINT '====================================='
  END CATCH
END

