


/*
=================================================
Create Database and Schemas
=================================================

Script Purpose:
This script create a new database name 'Datawarehouse' after checking if it already exists. 
if the database exits, it is dropped and recreated. Additionally script setup three schemas
within the database: 'bronze','silver','gold'.

Warning: a
Running this script drop the entire 'Datawarehouse' database if it exits.
All data in database will be permanently deleted. Proceed with caution and
ensure you have proper backup before running this script.
*/
use master
Go
-- Drop and execute the 'Datawarehouse' database
IF Exists(select 1 from sys.databases where name = 'Datawarehouse')
Begin
Alter database Datawarehouse set single_user with rollback immediate;
Drop database Datawarehouse
End
Go
--Create Database
Create database Datawarehouse
GO
--Use Database
use Datawarehouse
GO
--Create Schemas
Create schema bronze;
GO
Create schema silver;
GO
Create schema gold;
