USE master;
-- Drop and recreate the database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWareHouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLES_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--Create database 

CREATE DATABASE DataWarehouse;
USE DataWarehouse;
GO

--Create schemas for each layer
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
