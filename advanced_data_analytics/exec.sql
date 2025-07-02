USE DataWarehouse
GO


/*
=================================================
Product Report
=================================================
Purpose:
  - This report consolidates key product metrics and behaviours

Highlights:
  1. Gathers essential fields such as product name, category, subcategory and cost.
  2. Segments products by revenue to identify High-Performers. Mid-Range, or Low-Performers.
  3. Aggregates product-level metrics:
    - total orders
    - total sales
    - total quantity sold
    - total customers (unique)
    - lifespan (in months)
  4. Calculate valuable KPIsL
    - recency (months since last sale)
    - average order revenue = total sales/total orders
    - average monthly revenue = total sales/lifespan
=================================================
*/
WITH product_base_query AS(
    SELECT 
      f.product_key,
      f.customer_key,
      P.product_id,
      p.product_name,
      p.category,
      p.subcategory,
      f.order_num,
      f.sales_amount,
      f.quantity,
      f.order_date
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
    ON F.product_key = P.product_key
),
products_aggregation AS(
  SELECT 
    product_key,
    product_id,
    product_name,
    category,
    subcategory,
    SUM(sales_amount)  AS total_sales,
    COUNT(DISTINCT order_num) AS total_orders,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan   ,
    MIN(order_date) as first_order,
    MIN(order_date) as last_order 
  FROM product_base_query
  GROUP BY
    product_key,
    product_id,
    product_name,
    category,
    subcategory
)
SELECT 
  product_key,
    product_id,
    product_name,
    category,
    subcategory,
    total_sales,
    total_orders,
    total_quantity,
    total_customers,
    lifespan,
    DATEDIFF(month, last_order, GETDATE()) AS redency,
    total_sales / total_orders AS avg_order_revenue,
    total_sales / lifespan AS avg_monthly_revenue
FROM products_aggregation

SELECT *
FROM gold.dim_products

SELECT *
FROM gold.fact_sales

-- SELECT *
-- FROM gold.dim_customers