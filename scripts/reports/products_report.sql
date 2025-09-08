
/*
==============================================================================================
Product Report 
==============================================================================================

Purpose: 
	- This report consolidates key product metrics and behaviours.

Hightlights:
	1. gathers essential fields such as product name category, subcategory, cost
	2. segments products by revenue to identify high-performers, mid-range, and low-performers 
	3. aggregates product-level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total customers (unique)
		- lifespan (in months)
	4. Calculates valuables KPIs
		- recency (months)
		- avg order revenue
		- avg monthly revenue
==============================================================================================
*/
CREATE VIEW gold.product_report AS 
WITH base_query AS(
	SELECT 
		s.order_number,
		s.order_date, 
		s.customer_key,
		s.sales_amount,
		s.quantity,
		p.product_key, 
		p.product_name,
		p.category,
		p.sub_category,
		p.cost
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_product p ON p.product_key = s.product_key
	WHERE s.order_date IS NOT NULL),

	aggregated_query AS(
	SELECT 
		product_key, 
		product_name,
		category,
		sub_category,
		cost, 
		DATEDIFF(month, MIN(order_date), MAX(order_date)) as lifespan,
		MAX(order_date) as last_sale_date,
		COUNT(DISTINCT order_number) total_orders,
		COUNT(DISTINCT customer_key) total_customers,
		SUM(sales_amount) as total_sales,
		SUM(quantity) as total_quantity_sold,
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
	FROM base_query
	GROUP BY 	
		product_key, 
		product_name,
		category,
		sub_category,
		cost)

SELECT 
	product_key, 
	product_name,
	category,
	sub_category,
	cost, 
	last_sale_date,
	DATEDIFF(month, last_sale_date, GETDATE()) as recency,
	CASE 
		WHEN total_sales < 100 THEN 'Low-perfomers'
		WHEN total_sales BETWEEN 100 AND 300 THEN 'Mid-range'
		ELSE 'High-performers'
	END product_segment,
	lifespan,
	total_orders,
	total_customers,
	total_sales,
	total_quantity_sold,
	avg_selling_price,

	-- Average order revenue
	CASE 
		WHEN total_orders = 0 THEN 0 
		ELSE total_sales/total_orders
	END avg_order_revenue,

	-- Average monthly revenue
	CASE
		WHEN lifespan = 0 THEN 0
		ELSE total_sales / lifespan
	END avg_monthly_revenue
	FROM aggregated_query

