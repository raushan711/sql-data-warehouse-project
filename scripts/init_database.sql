/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Database: Datawarehouse

-- DROP DATABASE IF EXISTS "Datawarehouse";

CREATE DATABASE "Datawarehouse"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_India.1252'
    LC_CTYPE = 'English_India.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- SCHEMA: bronze

-- DROP SCHEMA IF EXISTS bronze ;

CREATE SCHEMA IF NOT EXISTS bronze
    AUTHORIZATION postgres;


-- SCHEMA: silver

-- DROP SCHEMA IF EXISTS silver ;

CREATE SCHEMA IF NOT EXISTS silver
    AUTHORIZATION postgres;

-- SCHEMA: gold

-- DROP SCHEMA IF EXISTS gold ;

CREATE SCHEMA IF NOT EXISTS gold
    AUTHORIZATION postgres;
