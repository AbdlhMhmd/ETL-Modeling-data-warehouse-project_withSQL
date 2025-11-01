-- Create a new data warehouse named 'ETL_DataWareHouse'

USE MASTER;  
GO

-- Drop and recreate the 'ETL_DataWareHouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'ETL_DataWareHouse')
BEGIN
    ALTER DATABASE ETL_DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ETL_DataWareHouse;
END;
GO

-- Create the 'DataWarehouse' database  
CREATE database ETL_DataWareHouse;
GO

-- Use the data warehouse to create the schema of the medallion architecture
USE ETL_DataWareHouse;
GO

-- Create Schema
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold
GO

-- End of code
