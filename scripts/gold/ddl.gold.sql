use Datawarehouse
/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
Create view  gold.dim_customers as
	select 
		ROW_NUMBER() over(order by ci.cst_id) as Customer_Key,
		ci.cst_id as Customer_id,
		ci.cst_key as Customer_number,
		ci.cst_firstname as First_name,
		ci.cst_lastname as Last_name,
		pl.cntry as Country,
		ci.cst_marital_status as Marital_Status,
		Case when ci.cst_gndr != 'n/a' then ci.cst_gndr
		Else coalesce(ca.gen, 'n/a') end as Gender,
		ca.bdate as Birthdate,
		ci.cst_create_date as Create_Date
	from silver.crm_customer_info ci
	left join silver.erp_cust_az12 ca 
		on ci.cst_key = ca.cid
	left join silver.erp_px_loc_a101 pl 
		on ci.cst_key = pl.cid


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
	Create view gold.dim_products as
	Select row_number() over(order by prd_start_dt, prd_key) as Product_key,
		pn.prd_id as Product_id,
		pn.prd_key as Product_number,
		pn.prd_nm as Product_name,
		pn.cat_id as Category_id,
		pc.cat as Category,
		pc.subcat as Subcategory,
		pc.maintence as Maintence,
		prd_cost as Product_cost,
		prd_line as Product_line,
		prd_start_dt as Start_date
	from silver.crm_prd_info pn
	left join silver.erp_px_cat_g1v2 pc 
		on pn.cat_id = pc.id where pn.prd_end_dt is null

	 =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
 IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
 Create View gold.fact_sales as
 Select 
	sd.sls_ord_num  as Order_number, 
	dp.Product_key as Product_key,
	dc.Customer_Key as Customer_key,
	sd.sls_order_dt as Order_date,
	sd.sls_ship_dt as Ship_date,
	sd.sls_due_dt as Due_date,
	sd.sls_sales as Sales,
	sd.sls_quantity as Quantity,
	sd.sls_price as Price
from silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products dp 
	on sd.sls_prd_key= dp.Product_number 
left join gold.dim_customers dc 
	on sd.sls_cust_id = dc.Customer_id
