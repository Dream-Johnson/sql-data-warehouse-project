/*
===========================================
Create Database and Schema
===========================================
Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exisits.
	If the database exisits, it is dropped and recreated. Aditionaly, the scripts sets up three schemas within the database: 'bronze', 'silver' and 'gold'

	**WARNING**
		Running this script will delete any database having the name 'DataWarehouse' if the database exists.
		All data in the database will be permanently deleted. Procced with caution and ensure you have proper backups before running the scripts.
*/
use master;
GO

--Drop and create the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO


--CREATE the database 'DataWarehouse'
CREATE DATABASE DataWarehouse;

use DataWarehouse;

--Createing the schemas - bronze, silver, gold
create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO



