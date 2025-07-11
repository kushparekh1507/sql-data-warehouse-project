USE DataWarehouse
GO

-- Explore All Objects in the Database
SELECT * FROM INFORMATION_SCHEMA.TABLES

-- Explore All Columns in the Database
SELECT * 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers'



-- Dimensions Exploral

-- Explore All Countries our customers come from
SELECT DISTINCT country
FROM gold.dim_customers

-- Explore All Product Categories "The Major Divisions"
SELECT DISTINCT category, subcategory, product_name
FROM gold.dim_products
ORDER BY 1,2,3



--  Date Exploration

-- Identify the earliest and latest dates (boundaries)
-- Find the dates of First and Last order
SELECT 
  MIN(order_date) AS first_order_date,
  MAX(order_date) AS last_order_date
FROM gold.fact_sales

-- How many years of sales are available
SELECT
  DATEDIFF(YEAR, MIN(order_date), MAX(order_date)) AS order_range_years
FROM gold.fact_sales

-- Find the youngest and oldest customer
SELECT 
  MIN(birthdate) AS oldest_birthdate,
  DATEDIFF(YEAR, MIN(birthdate), GETDATE()) as oldest_age,
  MAX(birthdate) AS youngest_bithdate,
  DATEDIFF(YEAR, MAX(birthdate), GETDATE()) as youngest_age
FROM gold.dim_customers


--   Measures Exploration

-- Find the Total Sales
SELECT SUM(sales_amount) AS total_sales FROM gold.fact_sales

-- Find how many items are sold
SELECT SUM(quantity) AS total_quantity  FROM gold.fact_sales 

SELECT * FROM gold.dim_products

SELECT * FROM gold.fact_sales


-- Find the average selling price
SELECT AVG(sales_amount) AS avg_selling_price FROM gold.fact_sales

-- Find the total number of orders
SELECT COUNT(order_num) AS total_orders FROM gold.fact_sales
SELECT COUNT(DISTINCT order_num) AS total_orders FROM gold.fact_sales

-- Find the total number of products
SELECT COUNT(product_id) AS total_products FROM gold.dim_products

-- Find the total number of customers
SELECT COUNT(customer_id) AS total_customers FROM gold.dim_customers

-- Find the total number of customers that has plced an order
SELECT COUNT(DISTINCT customer_key) FROM gold.fact_sales 


-- Generate Report that shows all key metrics of the business

SELECT 'Total Sales' as measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity' as measure_name, SUM(quantity) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Average Price' as measure_name, AVG(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders' as measure_name, COUNT(DISTINCT order_num) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Products' as measure_name, COUNT(product_id) AS measure_value FROM gold.dim_products
UNION ALL
SELECT 'Total Customers' as measure_name,  COUNT(customer_id) AS measure_value FROM gold.dim_customers


--  Magnitude - Compares the values by categories

-- Find total customers by countries
SELECT 
  country, 
  COUNT(*) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC

-- Find total customers by gender
SELECT 
  gender, 
  COUNT(*) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC

-- Find total products by category
SELECT 
  category,
  COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC

-- What is the average costs in each category?
SELECT 
  category,
  AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC

-- What is the total revenue generated for each category?
SELECT
  *
FROM gold.fact_sales
WHERE product_key IN(
  SELECT product_key
  FROM gold.dim_products
  WHERE category = 'Components'
)

SELECT 
  dp.category,
  SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
ON fs.product_key = dp.product_key
GROUP BY dp.category
ORDER BY total_revenue DESC

-- Find total revenue is generated by each customer
SELECT 
  customer_key,
  SUM(sales_amount) AS total_revenue
FROM gold.fact_sales
GROUP BY customer_key
ORDER BY total_revenue DESC

-- What is the distribution of sold items across contries?
SELECT 
  dc.country,
  SUM(quantity) AS total_sold_items
FROM gold.fact_sales fs
INNER JOIN gold.dim_customers dc
ON fs.customer_key = dc.customer_key
GROUP BY dc.country
ORDER BY total_sold_items DESC


--  Ranking Analysis - Top N, Bottom N

-- Which 5 products generate the highest revenue?
SELECT TOP 5
  P.product_id,
  p.product_name,
  SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON F.product_key = p.product_key
GROUP BY P.product_id, p.product_name
ORDER BY total_revenue DESC

SELECT *
FROM(
  SELECT
    p.product_name,
    SUM(f.sales_amount) AS total_revenue,
    ROW_NUMBER() OVER(ORDER BY SUM(f.sales_amount) DESC) rank_products
  FROM gold.fact_sales f
  LEFT JOIN gold.dim_products p
  ON F.product_key = p.product_key
  GROUP BY p.product_name
)a
WHERE rank_products <= 5

-- What are the 5 worst-performing products in terms of sales?

SELECT TOP 5
  P.product_id,
  p.product_name,
  SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON F.product_key = p.product_key
GROUP BY P.product_id, p.product_name
ORDER BY total_revenue

-- Find the Top 10 cutoemrs who have generated the highest revenue
SELECT TOP 10
  C.customer_key,
  C.first_name,
  C.last_name,
  SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers C
ON f.customer_key = C.customer_key
GROUP BY C.customer_key, C.first_name, C.last_name
ORDER BY total_revenue DESC


-- The 3 customers with the fewest orders placed
SELECT TOP 3
  C.customer_key,
  C.first_name,
  C.last_name,
  COUNT(DISTINCT order_num) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers C
ON f.customer_key = C.customer_key
GROUP BY C.customer_key, C.first_name, C.last_name
ORDER BY total_orders
