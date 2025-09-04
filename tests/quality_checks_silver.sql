use DataWarehouse;
/*
==================================================================
These are all the queries to check the quality of the silver layers
===================================================================
*/

-- check for repeated Pks
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- check for unwanted spaces 
SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Data Standarization & consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

-- check repeating pk
SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- check spaces
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- remove negative numbers or nulls
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standarization & consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

SELECT prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

SELECT 
	NULLIF(sls_order_dt, 0)
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 205000101;

SELECT
	sls_sales,
	sls_quantity,
	sls_price,

    -- Recalculate sales if null, zero, or negative
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
             THEN ABS(sls_price) * sls_quantity
        ELSE sls_sales
    END AS sls_sales_new,

    -- Recalculate price if null or zero
    CASE 
        WHEN sls_price IS NULL OR sls_price = 0 
             THEN sls_sales / NULLIF(sls_quantity, 0)  -- avoid divide-by-zero
        ELSE ABS(sls_price)   -- make negative price positive
    END AS sls_price_new

FROM silver.crm_sales_details
WHERE 
sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_price IS NULL OR sls_quantity IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;


SELECT cid,
COUNT(*)
FROM bronze.erp_cust_az12
GROUP BY cid 
HAVING COUNT(*) > 1


SELECT 
CASE 
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LEN(cid)) 
    ELSE cid 
 END cid
FROM bronze.erp_cust_az12

SELECT DISTINCT
CASE WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
END
FROM bronze.erp_cust_az12

SELECT DISTINCT 
CASE UPPER(TRIM(gen))
    WHEN 'F' THEN 'Female'
    WHEN 'M' THEN 'Male'
    ELSE 'n/a'
END
FROM bronze.erp_cust_az12

SELECT DISTINCT
CASE
    WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY')  THEN 'Germany'
    WHEN UPPER(TRIM(cntry)) IN ('USA', 'US', 'UNITED STATES')  THEN 'United States of America'
    WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
    ELSE TRIM(cntry)
END
FROM bronze.erp_loc_a101
 
