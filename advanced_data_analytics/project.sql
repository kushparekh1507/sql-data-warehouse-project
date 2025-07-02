USE DataWarehouse
GO

--   Change Over Time Analysis

-- Analyze Sales Performances over time.
SELECT 
  YEAR(order_date) order_year,
  SUM(sales_amount) total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)

SELECT 
  MONTH(order_date) order_month,
  SUM(sales_amount) total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date)

SELECT 
  YEAR(order_date) order_year,
  MONTH(order_date) order_month,
  SUM(sales_amount) total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)

SELECT 
  DATETRUNC(MONTH,order_date) order_date,
  SUM(sales_amount) total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH,order_date)
ORDER BY DATETRUNC(MONTH,order_date)


SELECT 
  FORMAT(order_date, 'yyyy-MMM') order_date,
  SUM(sales_amount) total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY FORMAT(order_date, 'yyyy-MMM')



--   Cumulative Analysis

-- Claculate the total sales for each month
-- and the running total of sales over time
SELECT
    DATETRUNC(month, F.order_date) AS sales_month,
    SUM(F.sales_amount) AS monthly_total_sales,
    SUM(SUM(F.sales_amount)) OVER (
        ORDER BY DATETRUNC(month, F.order_date)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_sales
FROM gold.fact_sales F
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, F.order_date)
ORDER BY sales_month;


SELECT
 order_date,
 total_sales,
 SUM(total_sales) OVER(ORDER BY order_date) AS running_total_sales,
 avg_price,
 AVG(avg_price) OVER(ORDER BY order_date) AS moving_avg_price
FROM(
  SELECT 
    DATETRUNC(MONTH,order_date) order_date,
    SUM(sales_amount) total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity,
    AVG(price) AS avg_price
  FROM gold.fact_sales
  WHERE order_date IS NOT NULL
  GROUP BY DATETRUNC(MONTH,order_date)
)a;



-- Performance Analysis

-- Analyze the yearly performance of products by 
-- comparing each product's sales to both its 
-- average sales performance and the previous year's sales

WITH yearly_product_sales AS(
  SELECT 
    YEAR(F.order_date) order_year,
    p.product_name,
    SUM(F.sales_amount) AS current_salse
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_products p
  ON F.product_key = P.product_key
  WHERE order_date IS NOT NULL
  GROUP BY YEAR(F.order_date), p.product_name
)
SELECT 
  order_year,
  product_name,
  current_salse,
  AVG(current_salse) OVER(PARTITION BY product_name) AS avg_sales,
  current_salse - AVG(current_salse) OVER(PARTITION BY product_name) AS difference,
  CASE
    WHEN current_salse - AVG(current_salse) OVER(PARTITION BY product_name) > 0 THEN 'Above Avg'
    WHEN current_salse - AVG(current_salse) OVER(PARTITION BY product_name) < 0 THEN 'Below Avg'
    ELSE 'Avg'
  END AS flag,
  LAG(current_salse) OVER(PARTITION BY product_name ORDER BY order_year) AS previous_year_salse,
  current_salse - LAG(current_salse) OVER(PARTITION BY product_name ORDER BY order_year) as differnece
FROM yearly_product_sales
ORDER BY product_name, order_year;


--   Part-To-Whole Analysis

-- Which categories contribute the most to overall sales?

WITH category_sales AS(
  SELECT
    p.category,
    SUM(F.sales_amount) AS total_sales
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_products p
  ON F.product_key = p.product_key
  GROUP BY p.category   
)
SELECT
  category,
  total_sales,
  SUM(total_sales) OVER() overall_sales,
  ROUND((CAST(total_sales AS float) / SUM(total_sales) OVER()) * 100, 2) AS 'percentage(%)'
FROM category_sales
ORDER BY total_sales DESC;


--   Data Segmentation

-- Segment products into cost ranges and 
-- count how many products fall into each segment
WITH cost_segmentation AS(
  SELECT 
    product_name,
    cost,
    CASE WHEN cost <= 100 THEN 'Below 100'
        WHEN cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
        ELSE 'Above 1000'  
    END AS segment
  FROM gold.dim_products
)
SELECT 
  segment,
  COUNT(*) AS total_products
FROM cost_segmentation
GROUP BY segment
GO

/* Group customers into three segments based on their spending behaviour:
  - VIP: Customers with at least 12 months of history and spending more then 5,000.
  - Regular: Customers with at least 12 months of history but spending 5,000 or less.
  -- VIP: Customers with lifespan less then 12 months
And find the total number of customers by each group
*/

WITH customer_first_last_orders AS(
  SELECT 
    customer_key,
    MIN(order_date) OVER(PARTITION BY customer_key) AS first_order_date,
    MAX(order_date) OVER(PARTITION BY customer_key) AS last_order_date,
    SUM(sales_amount) OVER(PARTITION BY customer_key) AS total_sales
  FROM gold.fact_sales
),
differenceBet AS(
  SELECT 
    distinct customer_key,
    first_order_date,
    last_order_date,
    DATEDIFF(MONTH,first_order_date,last_order_date) AS differenceInMonhs,
    total_sales
  FROM customer_first_last_orders
),
customer_segmentation AS(
  SELECT 
    customer_key,
    CASE WHEN differenceInMonhs >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN differenceInMonhs >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_type
  FROM differenceBet
)
SELECT 
  customer_type,
  COUNT(*) AS total_customers
FROM customer_segmentation
GROUP BY customer_type
GO



WITH customer_Spending AS(
  SELECT 
      customer_key,
      MIN(order_date)  AS first_order_date,
      MAX(order_date) AS last_order_date,
      SUM(sales_amount)  AS total_spending,
      DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
  FROM gold.fact_sales
  GROUP BY customer_key
)
SELECT 
  customer_segment,
  COUNT(customer_key)
FROM(
  SELECT
    customer_key,
    CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
          WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
          ELSE 'New'
    END AS customer_segment
  FROM customer_Spending
)a
GROUP BY customer_segment
GO



/*
====================================================
Customer Report
====================================================
Purpose:
  - This report consolidates key customer metrics and behaviours

Highlights:
  1. Gathers essential fields such as names, ages and transaction details.
  2. Segments customers into categories (VIP, Regular, New) and age groups.
  3. Aggregates customer-level metrics:
    - total orders
    - total sales
    - total quantity purchased
    - total products
    - lifespan (in months)
  4. Calculates valuable KPIs:
    - recency (months since last order)
    - average order value
    - average monthly spend
====================================================
*/

CREATE OR ALTER VIEW gold.reports_customers AS
WITH base_query AS(
  /*
  ---------------------------------------------------
  1) Base Query: Retrieves core columns from tables
  ---------------------------------------------------
  */
  SELECT 
    F.order_num,
    F.product_key,
    F.order_date,
    f.sales_amount,
    f.quantity,
    c.customer_key,
    c.customer_number,
    CONCAT(C.first_name,' ',C.last_name) AS customer_name,
    DATEDIFF(YEAR,C.birthdate, GETDATE()) age
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_customers c
  ON f.customer_key = C.customer_key
  WHERE F.order_date IS NOT NULL
),
custoemr_aggregation AS(
  /*
  ---------------------------------------------------
  2) Customer Aggregations: Summarizes key metrics at the customer level
  ---------------------------------------------------
  */
  SELECT 
    customer_key,
    customer_number,
    customer_name,
    age,
    COUNT(DISTINCT order_num) AS total_orders,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    SUM(DISTINCT product_key) AS total_products,
    MAX(order_date) AS last_order,
    MIN(order_date) AS first_order,
    DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
  FROM base_query
  GROUP by 
    customer_key,
    customer_number,
    customer_name,
    age
)
SELECT 
  customer_key,
    customer_number,
    customer_name,
    age,
    CASE WHEN age < 20 THEN 'Under 20'
         when age between 20 and 29 THEN '20-29'
         when age between 30 and 39 THEN '30-39'
         when age between 40 and 49 THEN '40-49'
         else '50 or Above'
    end AS age_group,
    CASE WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
          WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
          ELSE 'New'
    END AS customer_segment,
    DATEDIFF(month, last_order, GETDATE()) AS
    recency,
    total_orders,
    total_sales, 
    total_products,
    total_quantity,
    lifespan,
    -- Compute Average Order Value
    case when total_orders = 0 then 0
         else total_sales / total_orders 
    end avg_order_value,

    -- Compute Average Monthly spendings
    case when lifespan = 0 then total_sales
         else total_sales / lifespan
    end AS avg_monthly_spend
from custoemr_aggregation
GO


-- SELECT *
-- FROM gold.reports_customers;


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
    - average order revenue
    - average monthly revenue
=================================================
*/







