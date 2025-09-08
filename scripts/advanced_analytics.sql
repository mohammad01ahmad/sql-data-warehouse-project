USE DataWarehouse;

/*
======================================================================

Advanced Analytics project
  -used different types of advanced analytics 
  - final report creted and stored in gold folder 'final_report.sql'

=====================================================================
*/
-- Total summary over the years
SELECT 
YEAR(order_date) as order_year,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)

-- Total summary over the months
SELECT 
MONTH(order_date) as order_month,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date)


-- Calculate the total sales per month and the running total of sales over time 
SELECT 
order_date, 
total_sales,
SUM(total_sales) OVER(PARTITION BY order_date ORDER BY order_date) as running_total_sales,
AVG(avg_price) OVER(ORDER BY order_date) as moving_average_price
FROM(
	SELECT 
	DATETRUNC(year, order_date) as order_date,
	SUM(sales_amount) as total_sales,
	AVG(price) as avg_price 
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(year, order_date)
)t


-- PERFORMANCE ANAYSIS 
/* Analyse the yearly performance of products by comparing each product's sales 
to both its average sales performance and the previous year's sales

1. current product sales - average sales performance 
2. current product sales - previous year sales performance 

Flag the results with proper terms
*/

 -- CTE to calculate the sale for each product for each year
WITH yearly_product_sales AS(
	SELECT 
	p.product_name as product_name,
	YEAR(s.order_date) AS order_year,
	SUM(s.sales_amount) AS total_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_product p ON s.product_key = p.product_key
	WHERE s.order_date IS NOT NULL
	GROUP BY YEAR(s.order_date), p.product_name
)

-- Use CTE to create GROUPBYs and all then use in main query for Window functions 
SELECT 
order_year,
product_name,
total_sales,
AVG(total_sales) OVER(PARTITION BY product_name) as average_price,
total_sales - AVG(total_sales) OVER(PARTITION BY product_name) as diff_avg,
CASE WHEN total_sales - AVG(total_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above average'
	 WHEN total_sales - AVG(total_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below average'
	 ELSE 'Average'
END avg_change,
LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS py_sale,
total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_py_sale,
CASE WHEN total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Better than last year'
	 WHEN total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Worse than last year'
	ELSE 'Same as last year'
END py_change
FROM yearly_product_sales
ORDER BY product_name, order_year

-- PART TO WHOLE 
-- Which categories contribute the most to overall sales 
WITH total_sales_category AS(
	SELECT 
	category,
	SUM(sales_amount) as total_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_product p ON s.product_key = p.product_key
	GROUP BY category
)

SELECT 
category,
total_sales,
SUM(total_sales) OVER() overall_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT)/ SUM(total_sales) OVER())*100, 2), '%') AS percentage_of_total 
FROM total_sales_category

-- DATA SEGMENTATION 
-- segment products into cost ranges and count how many products fall into each segment 
WITH product_sales_rank AS (
	SELECT 
	product_name,
	product_key,
	cost,
	CASE 
		WHEN cost < 100 THEN 'Below 100'
		WHEN cost BETWEEN 100 AND 500 THEN '100-500'
		WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
		ELSE 'Above 1000'
	END AS cost_range
	FROM gold.dim_product p
)

SELECT
cost_range,
COUNT(product_key) AS total_products
FROM product_sales_rank
GROUP BY cost_range 
ORDER BY total_products DESC


/*
Group customers into three segments based on their spending behavior
	-VIP: at least 12 months of history and spending more than 5000
	-Regular: at least 12 months of history but spending 5000 or less 
	-New: lifespan less than 12 months 
And find the total number of customers by each group 
*/

WITH customer_segment AS(
	SELECT 
	c.customer_key as customer_key,
	SUM(s.sales_amount) total_spending,
	MIN(order_date) as first_order,
	MAX(order_date) as last_order,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan,
	CASE 
		WHEN SUM(s.sales_amount) > 5000 AND DATEDIFF(month, MIN(order_date), MAX(order_date)) >= 12 THEN 'VIP'
		WHEN SUM(s.sales_amount) <= 5000 AND DATEDIFF(month, MIN(order_date), MAX(order_date)) >= 12 THEN 'Regular'
		ELSE 'New'
	END customer_rank
	FROM gold.dim_customers c
	LEFT JOIN gold.fact_sales s ON c.customer_key = s.customer_key
	GROUP BY c.customer_key
)

SELECT 
customer_rank,
COUNT(customer_key) total
FROM customer_segment
GROUP BY customer_rank
ORDER BY total





