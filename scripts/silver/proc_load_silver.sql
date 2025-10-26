/*
========================================================================================
Stored Procedure: Load Silver layer(bronze->silver)
========================================================================================
Script Purpose
This Stored procedure performs ETL(extract, transform,load) process to populate
the 'silver' schema tables from the bronze schema.
Actions performed
-Truncates the silver table
-Insert transformed and cleaned data from bronze into silver tables
Parameters
None
This stored procedure does not accept any parameters or return any values
Usage Example
Exec silver.load_silver
========================================================================================
*/
Create or Alter procedure silver.load_silver as 
Begin
Begin try
Declare @batchstarttime date, @batchendtime date, @startdate date, @enddate date
print '========================================='
print 'Loading Broze Layer'
print '========================================='
print '-----------------------------------------'
print 'Loading CRM Tables'
print '-----------------------------------------'
Print 'Truncate table: silver.crm_customer_info'
Set @batchstarttime = getdate()
Truncate table silver.crm_customer_info
set @startdate = getdate()
Print 'Inserting data into: silver.crm_customer_info '
INSERT INTO silver.crm_customer_info (
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
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS unique_records
    FROM bronze.crm_customer_info
    WHERE cst_id IS NOT NULL
) t WHERE unique_records = 1;
set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

Print 'Truncate table: silver.crm_prd_info'
Truncate table silver.crm_prd_info
Print 'Inserting data into: silver.crm_prd_info '
set @startdate = getdate()
insert into silver.crm_prd_info(
	prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)

SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'OtherSales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
        AS DATE
    ) AS prd_end_dt
FROM bronze.crm_prd_info;
set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

Print 'Truncate table: silver.crm_sales_details'
Truncate table silver.crm_sales_details
Print 'Inserting data into: silver.crm_sales_details '
set @startdate = getdate()
insert into silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,
sls_sales,sls_quantity,sls_price)
Select sls_ord_num, sls_prd_key, sls_cust_id,
Case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
Else cast(cast( sls_order_dt as nvarchar)as date)
End as sls_order_dt,
Case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
Else cast(cast( sls_ship_dt as nvarchar)as date)
End as sls_ship_dt,
Case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
Else cast(cast( sls_due_dt as nvarchar)as date)
End as sls_due_dt,
Case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
then sls_quantity * abs(sls_price) else sls_sales end as sls_sales,sls_quantity,
Case when sls_price is null or sls_price <= 0 then sls_sales/ nullif(sls_quantity,0)
Else sls_price end sls_price
From bronze.crm_sales_details where sls_price is null or sls_sales is null or sls_quantity is null
set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

Print 'Truncate table: silver.erp_cust_az12'
Truncate table silver.erp_cust_az12
Print 'Inserting data into: silver.erp_cust_az12 '
set @startdate = getdate()
Insert into silver.erp_cust_az12(cid,bdate,gen)
Select case when cid  like '%NAS%' then SUBSTRING(cid,4,len(cid)) else cid end as cid,
Case when bdate > getdate() then null else bdate end as bdate,
case when gen in ('F', 'Female') then 'Female' when gen in ('M', 'Male') then 'Male'
Else gen end as gen
from bronze.erp_cust_az12
set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

Print 'Truncate table: silver.erp_px_loc_a101'
Truncate table silver.erp_px_loc_a101
Print 'Inserting data into: silver.erp_px_loc_a101 '
set @startdate = getdate()
Insert into silver.erp_px_loc_a101(cid,cntry)
Select replace(cid, '-', '') as cid, 
Case When Trim(cntry) = 'DE' then 'Germany' 
When Trim(cntry) in ('US', 'USA') then 'United States'
When Trim(cntry) = '' or cntry is null then 'n/a'
else Trim(cntry) end as cntry
from bronze.erp_px_loc_a101
set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

Print 'Truncate table: silver.erp_px_cat_g1v2'
Truncate table silver.erp_px_cat_g1v2
Print 'Inserting data into: silver.erp_px_cat_g1v2 '
set @startdate = getdate()
insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintence)
Select id,cat,subcat,maintence from bronze.erp_px_cat_g1v2
set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'
Set @batchendtime = GETDATE()
Print 'Total Loading Duration:' + cast(datediff(second,@batchstarttime,@batchendtime)as nvarchar) + 'seconds'
End Try
Begin Catch
print '========================================='
print 'Error Occured during loading Silver Layer'
Print 'Error Message:' + Error_message()
Print 'Error Number:' + cast(Error_number() as nvarchar)
Print 'Error Number:' + cast(Error_state() as nvarchar)	
print '========================================='
End Catch
End
Exec silver.load_silver
