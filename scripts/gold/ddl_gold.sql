USE DataWarehouse
GO

  
-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT 
  ROW_NUMBER() over(order by cst_id) as customer_key,
  ci.cst_id as customer_id,
  ci.cst_key customer_number,
  ci.cst_firstname first_name,
  ci.cst_lastname as last_name,
  la.cntry as country,
  ci.cst_material_status as marital_status,
   CASE 
    WHEN ci.cst_gender != 'n/a' then ci.cst_gender -- CRM is the master for gender Info
    ELSE COALESCE(ca.gen, 'n/a')
  END as gender,
  ca.bdate as birth_date,
  ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
GO


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE OR ALTER VIEW gold.dim_product AS
SELECT 
  ROW_NUMBER() OVER(order by pn.prd_start_dt, pn.prd_key) as product_key,
  pn.prd_id as product_id,
  pn.prd_key as product_number,
  pn.prd_nm as product_name,
  pn.cat_id as category_id,
  pc.cat as category,
  pc.subcat as subcategory,
  pc.maintenance,
  pn.prd_cost as cost,
  pn.prd_line as product_line,
  pn.prd_start_dt as start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt is null -- Filter out all historical data
GO


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
  sd.sls_ord_num AS order_num,
  pr.product_key,
  dc.customer_key,
  sd.sls_order_dt as order_date,
  sd.sls_ship_dt as shipping_date,
  sd.sls_due_dt as due_date,
  sd.sls_sales as sales_amount,
  sd.sls_quantity as quantity,
  sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id
GO








