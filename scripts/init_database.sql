/*
=================================
Create Database and Schemas
=================================

script purpose:
	This script create a new database named DataWarehouse after checking if it already exists.
	If the database already exist , it is dropped and recreated.
	Additionally , the scripts setup three schemas bronze, silver and gold within the database

WARNING:
	Running this script will drop entire 'DataWarehouse' database if exists. All data will be deleted
	permanently so proceed with caution and ensure you have proper backup before running this scripts.


*/




use master;
Go

If EXISTS (select 1 from sys.databases where name='DataWarehouse')
Begin
	ALter DATABASE DataWarehouse SET  SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO


-- creating Database
create database DataWarehouse;
Go 

use DataWarehouse;
Go

--creating schema
CREATE SCHEMA bronze;
Go

CREATE SCHEMA silver;
Go

CREATE SCHEMA gold;
