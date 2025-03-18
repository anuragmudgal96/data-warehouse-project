/*
==========================================
CREATE DATABASE AND SCHEMAS
==========================================

Script Purpose:
	This Script create new database named 'DataWarehouse' after checking if it already exisits.
	If the database exists, it is droped and recreated. Additionally, the script sets up three schemas
	within the database: 'bronze','silver','gold'.

WARNING:
	Running it will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Ensure u have proper backup before running this script.
*/

USE master;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse
END;
GO

-- Create Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
