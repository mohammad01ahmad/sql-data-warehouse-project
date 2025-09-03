/*
=================================
Create database and schemas 
=================================

Script purpose: 
	This creates a new database and creates its schemas. 

*/


USE master;
GO

-- create database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

-- create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
