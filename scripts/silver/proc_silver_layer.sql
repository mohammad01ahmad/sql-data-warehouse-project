/*
==============================================
Procedure for loading data into silver layer
==============================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	PRINT '>> Truncating table'
	TRUNCATE TABLE silver.crm_cust_info

	PRINT '>> Inserting into Customers table'
	-- Remove repeated PK rows
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr, 
		cst_create_date
	)
	SELECT 
	cst_id, 
	cst_key, 
	TRIM(cst_firstname) cst_firstname, 
	TRIM(cst_lastname) cst_lastname,
	CASE cst_marital_status
		WHEN TRIM(UPPER('S')) THEN 'Single' -- Data cleaning 
		WHEN TRIM(UPPER('M')) THEN 'Married'
		Else 'n/a'
	END cst_marital_status,
	CASE cst_gndr 
		WHEN TRIM(UPPER('M')) THEN 'Male' -- Data cleaning 

		WHEN TRIM(UPPER('F')) THEN 'Female'
		Else 'n/a'
	END cst_gndr,
	cst_create_date
	FROM(
		-- Ranking repeated rows
		SELECT *, 
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) Flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL 

		-- Filtering repeated rows and getting the latest record
	)t WHERE Flag_last = 1;



	PRINT '>> Truncating table'
	TRUNCATE TABLE silver.crm_prd_info

	PRINT '>> Inserting into products table'
	INSERT INTO silver.crm_prd_info (
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
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id, -- Extract category ID
		SUBSTRING(prd_key, 7,LEN(prd_key)) prd_key, -- Extract Product key
		prd_nm,
		ISNULL(prd_cost, 0) prd_cost, -- Handling NULLS
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'M' THEN 'Touring'
			ELSE 'n/a'
		END prd_line,
		prd_start_dt,
		-- Calculate end date as one before the next start date
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt
	FROM bronze.crm_prd_info;



	PRINT '>> Truncating table'
	TRUNCATE TABLE silver.crm_sales_details

	PRINT '>> Inserting into sales table'

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
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END sls_due_dt,
			-- Recalculate sales if null, zero, or negative
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				 THEN ABS(sls_price) * sls_quantity
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		-- Recalculate price if null or zero
		CASE 
			WHEN sls_price IS NULL OR sls_price = 0 
				 THEN sls_sales / NULLIF(sls_quantity, 0)  -- avoid divide-by-zero
			ELSE ABS(sls_price)   -- make negative price positive
		END AS sls_price
	FROM bronze.crm_sales_details;



	PRINT '>> Truncating table'
	TRUNCATE TABLE silver.erp_cust_az12

	PRINT '>> Inserting into erp customers table'
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LEN(cid)) 
		ELSE cid 
	END cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END bdate, 
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END gen
	FROM bronze.erp_cust_az12;



	PRINT '>> Truncating table'
	TRUNCATE TABLE silver.erp_loc_a101

	PRINT '>> Inserting into customers location table'

	INSERT INTO silver.erp_loc_a101(cid, cntry)

	SELECT 
	CASE 
		WHEN cid LIKE 'AW-%' THEN SUBSTRING(TRIM(cid),1, 2) + SUBSTRING(TRIM(cid),4, LEN(cid))
	END cid,
	CASE
		WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY')  THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('USA', 'US', 'UNITED STATES')  THEN 'United States of America'
		WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
		ELSE TRIM(cntry)
	END cntry
	FROM bronze.erp_loc_a101;




	PRINT '>> Truncating products details table'
	TRUNCATE TABLE silver.erp_px_cat_g1v2

	PRINT '>> Inserting into table'

	INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
	SELECT 
	TRIM(id), 
	TRIM(cat), 
	TRIM(subcat), 
	TRIM(maintenance)
	FROM bronze.erp_px_cat_g1v2;

END
