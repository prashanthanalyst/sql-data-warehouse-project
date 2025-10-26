/*
======================================================
DDL Scripts: Create Bronze Tables
======================================================
Script Purpose:
This Script create tables in the 'bronze' Schema, dropping existing tables
if they already exits
Run this script to re-define DDL structure of 'bronze' tables.
======================================================
*/


if OBJECT_ID('bronze.crm_customer_info','U') is not null
Drop table bronze.crm_customer_info;

Create table bronze.crm_customer_info (cst_id int,	cst_key nvarchar(50),	cst_firstname nvarchar(50),
cst_lastname nvarchar(50),	cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),	cst_create_date date)

if OBJECT_ID('bronze.crm_prd_info(','U') is not null
Drop table bronze.crm_prd_info;
Create table bronze.crm_prd_info(prd_id int,	prd_key nvarchar(50),
prd_nm nvarchar(50), 	prd_cost int,	prd_line nvarchar(50),	
prd_start_dt date,	prd_end_dt date)

if OBJECT_ID('bronze.crm_sales_details(','U') is not null
Drop table bronze.crm_sales_details;
Create table bronze.crm_sales_details(sls_ord_num nvarchar(50),	sls_prd_key nvarchar(50),
sls_cust_id	int, sls_order_dt int,	sls_ship_dt	int,
sls_due_dt int,	sls_sales int,	sls_quantity int, sls_price int)

if OBJECT_ID('bronze.erp_px_loc_a101(','U') is not null
Drop table bronze.erp_px_loc_a101;
Create table bronze.erp_px_loc_a101( cid nvarchar(50), cntry nvarchar(50))
if OBJECT_ID('bronze.erp_cust_az12(','U') is not null
Drop table bronze.erp_cust_az12;
Create table bronze.erp_cust_az12(cid nvarchar(50),	bdate date,	gen nvarchar(50))
if OBJECT_ID('bronze.erp_px_cat_g1v2(','U') is not null
Drop table bronze.erp_px_cat_g1v2;
Create table bronze.erp_px_cat_g1v2(id nvarchar(50), cat nvarchar(50), subcat nvarchar(50), maintence nvarchar(50))
