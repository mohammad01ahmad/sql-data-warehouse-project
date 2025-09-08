/*
===========================================================================

Customer Report

===========================================================================

Purpose: 
	- This report consolidates key customer metrics and behaviors 

Highlights: 
	1. gather essential fields such as name, age, transaction details 
	2. segment customers into categories (vip, regular, new) and age groups 
	3. aggregates customer-level metrics: 
		- total orders 
		- total sales 
		- total quantity purchased
		- total products
		- lifespan (months)
	4. Calculates valuable KPIs
		- recency (months since last order)
		- average order value 
		- average monthly spend 
============================================================================

*/



/*
----------------------------------------------------------------------------
1) Base Query: Fact table 
----------------------------------------------------------------------------
*/
CREATE VIEW gold.report AS
WITH base_query AS(
	SELECT 
	s.order_number, 
	s.product_key,
	s.order_date,
	s.sales_amount,
	s.quantity,
	c.customer_key,
	c.customer_number,
	CONCAT(c.first_name,' ', c.last_name) AS customer_name,
	DATEDIFF(year, c.birth_date, GETDATE()) AS age
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
	WHERE order_date IS NOT NULL)

/*
----------------------------------------------------------------------------
2) Aggregated Query on the fact table
----------------------------------------------------------------------------
*/
, aggregated_query AS(
	SELECT 
	customer_key, 
	customer_number,
	customer_name,
	age,
	COUNT(DISTINCT order_number) as  total_orders,
	SUM(sales_amount) as total_sales,
	SUM(quantity) as total_quantity,
	COUNT(DISTINCT product_key) as total_products,
	MAX(order_date) as last_order_date,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) as lifespan
	FROM base_query
	GROUP BY customer_key, customer_number, customer_name, age)

/*
---------------------------------------------------------------------------
3) Segmentation
---------------------------------------------------------------------------
*/
	SELECT 
	customer_key, 
	customer_number,
	customer_name,
	age,
		CASE 
			WHEN age < 20 THEN 'Under 20'
			WHEN age BETWEEN 20 AND 29 THEN '20-29'
			WHEN age BETWEEN 30 AND 39 THEN '30-39'
			WHEN age BETWEEN 40 AND 49 THEN '40-49'
			ELSE '50 and Above'
		END age_group,
		CASE 
			WHEN total_sales > 5000 AND lifespan >= 12 THEN 'VIP'
			WHEN total_sales <= 5000 AND lifespan >= 12 THEN 'Regular'
			ELSE 'New'
		END customer_segment,	
	last_order_date,
	/*
	-------------------------------------------------------------------------
	4) KPI 
	-------------------------------------------------------------------------
	*/
	DATEDIFF(month, lifespan, GETDATE()) as recency,
	total_orders,
	total_sales,
		CASE 
			WHEN total_orders = 0 THEN 0 
			ELSE total_sales/total_orders
		END as avg_order_value,
		CASE 
			WHEN lifespan = 0 THEN 0 
			ELSE total_sales/lifespan
		END as avg_monthly_spend,	
	total_quantity,
	total_products,
	lifespan
	FROM aggregated_query
