/*
========================================================================================
Stored Procedure: Load Bronze layer(Source->bronze)
========================================================================================
Script Purpose
This Stored procedure stores data load into the 'bronze' schema from external CSV files
It performs following actions
-Truncates the bronze table before loading table
-Used bulk insert command to load data from csv to bronze tables
Parameters
None
This stored procedure does not accept any parameters or return any values
Usage Example
Exec bronze.load_bronze
========================================================================================
*/


Create or Alter procedure bronze.DataWarehouse as 
Begin
Declare @batchstarttime date, @batchendtime date
print '========================================='
print 'Loading Broze Layer'
print '========================================='
print '-----------------------------------------'
print 'Loading CRM Tables'
print '-----------------------------------------'

Declare @startdate date, @enddate date
Begin Try
Set @batchstarttime = GETDATE()
print '>>Truncate table crm_customer_info'
Set @startdate = getdate()
truncate table bronze.crm_customer_info 
print '>>Inserting Data into crm_customer_info'

Bulk insert bronze.crm_customer_info from 'C:\Users\Prashanth\Downloads\CRM\cust_info.csv'
with( firstrow = 2, fieldterminator= ',',  TABLOCK);
set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

print '>>Truncate table crm_prd_info'
set @startdate = getdate()
truncate table bronze.crm_prd_info 
Bulk insert bronze.crm_prd_info from 'C:\Users\Prashanth\Downloads\CRM\prd_info.csv'
with( firstrow = 2, fieldterminator= ',',  TABLOCK);
set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

print '>>Truncate table crm_sales_details'
set @startdate = getdate()
truncate table bronze.crm_sales_details 
Bulk insert bronze.crm_sales_details from 'C:\Users\Prashanth\Downloads\CRM\sales_details.csv'
with( firstrow = 2, fieldterminator= ',',
  TABLOCK);

  set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

print '-----------------------------------------'
print 'Loading ERM Tables'
print '-----------------------------------------'

print '>>Truncate table crm_sales_details'
set @startdate = getdate()
truncate table bronze.erp_cust_az12 
Bulk insert bronze.erp_cust_az12 from 'C:\Users\Prashanth\Downloads\ERP\CUST_AZ12.csv'
with( firstrow = 2, fieldterminator= ',',  TABLOCK);
  set @enddate = getdate()
Print 'Loading Difference:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

print '>>Truncate table crm_sales_details'
set @startdate = getdate()

truncate table bronze.erp_px_loc_a101 
Bulk insert bronze.erp_px_loc_a101 from 'C:\Users\Prashanth\Downloads\ERP\LOC_A101.csv'
with( firstrow = 2, fieldterminator= ',',  TABLOCK);
set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'

print '>>Truncate table crm_sales_details'
set @startdate = getdate()

truncate table bronze.erp_px_cat_g1v2 
Bulk insert bronze.erp_px_cat_g1v2 from 'C:\Users\Prashanth\Downloads\ERP\PX_CAT_G1V2.csv'
with( firstrow = 2, fieldterminator= ',',  TABLOCK)
Set @enddate = getdate()
Print 'Loading Duration:' + cast(datediff(second,@startdate,@enddate)as nvarchar) + 'seconds'
Set @batchendtime = GETDATE()
Print 'Total Loading Duration:' + cast(datediff(second,@batchstarttime,@batchendtime)as nvarchar) + 'seconds'
end try
Begin catch
print '========================================='
print 'Error Occured during loading Broze Layer'
Print 'Error Message:' + Error_message()
Print 'Error Number:' + cast(Error_number() as nvarchar)
Print 'Error Number:' + cast(Error_state() as nvarchar)	
print '========================================='

End catch
END
Exec bronze.DataWarehouse

