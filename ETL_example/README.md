# ETL Process in SQL (Northwind -> Data Warehouse)

This repository contains a complete, SQL-based ETL process that builds a small data warehouse from the classic Northwind sample database. It creates a dimensional model (dimensions and a fact table), loads data via a stored procedure, and applies basic transformations and data quality handling.

**Important:** Install the Northwind sample database before running any of the scripts in this repository.

## Goals

- Separate the operational source (Northwind) from the reporting layer using a DW.
- Provide a clear, re-runnable full-refresh ETL (truncate-and-load).
- Apply simple data quality rules (default values for NULLs, consistent data types).
- Offer an easy starting point for BI/reporting scenarios.

## Repository Structure

`1.Create Database Norhwind_DW.sql`  
Creates the `Northwind_DW` database, user-defined functions, and all DW tables:
- Dimensions: `Dim_Date`, `Dim_Products`, `Dim_Employees`, `Dim_Customers`, `Dim_Orders`
- Fact: `Fact_Sales`
- Functions:
  - `fn_prodact_type(@proid int)` - classifies products as "Expensive" or "Cheap" relative to the average `UnitPrice` in the source.
  - `fn_Dim_Date(@StartDate date, @EndDate date)` - generates a date dimension (`DateKey`, `Date`, `Year`, `Quarter`, `Month`, `MonthName`).

`2.Insert data procedure.sql`  
Defines and executes the ETL stored procedure `dbo.InsertData`:
- Truncates all DW tables.
- Populates `Dim_Date` for 1996-1999 via `fn_Dim_Date`.
- Loads dimensions from the source (`Northwnd`/`NORTHWND`) with cleaning rules.
- Loads the `Fact_Sales` fact table by joining source order details to the dimension keys.

`0.DataBase/`  
Contains `Northwind.bak`, a backup of the source OLTP database. Restore this backup into SQL Server (e.g., as `Northwnd`) before running `1.Create Database Norhwind_DW.sql`.

Note: The target DW is consistently named `Northwind_DW`. The source is referenced as `Northwnd`/`NORTHWND` (the classic Northwind sample). Adjust the source database name if your environment uses a different name.

## Data Model Overview

- `Dim_Date`: calendar attributes; `DateKey` is `YYYYMMDD` as `int`.
- `Dim_Products`: product, category, supplier, derived `ProductType` ("Expensive"/"Cheap").
- `Dim_Employees`: basic HR fields, computed `FullName`, `Age`, `Seniority`.
- `Dim_Customers`: customer name and geography.
- `Dim_Orders`: order-level shipping geography (subset of order attributes).
- `Fact_Sales`: grain is order line; foreign keys to all dimensions plus measures `UnitPrice`, `Quantity`, `Discount`.

## ETL Flow (Stored Procedure: `dbo.InsertData`)

Full refresh:
- `TRUNCATE` all dimension and fact tables.

Date dimension:
- Insert dates from `1996-01-01` through `1999-12-31` using `fn_Dim_Date`.

Products:
- Source: `Northwnd.dbo.Products` joined with categories and suppliers.
- Derive `ProductType` via `dbo.fn_prodact_type(ProductID)`; default to "Unknown" if `NULL`.

Employees:
- Source: `Northwnd.dbo.Employees`.
- Compute `FullName`, `Age = year(getdate()) - year(BirthDate)`, `Seniority = year(getdate()) - year(HireDate)`.
- Fill missing values with defaults (for example, "Unknown", `-1`, or placeholder dates like `3000-01-01`).

Customers:
- Source: `Northwnd.dbo.Customers`.
- Default `NULL` `City`/`Region`/`Country` to "Unknown".

Orders:
- Source: `Northwnd.dbo.Orders` filtered to `1996-01-01` through `1999-12-31`.
- Default `NULL` `ShipCity`/`ShipRegion`/`ShipCountry` to "Unknown".

Fact_Sales:
- Source: `Northwnd.dbo.[Order Details]` joined to:
  - `Dim_Orders` (by `OrderBK`),
  - `Northwnd.dbo.Orders` (for dates),
  - `Dim_Products` (by `ProductBK`),
  - `Dim_Date` (by exact `OrderDate` match),
  - `Dim_Customers` (by `CustomerBK`),
  - `Dim_Employees` (by `EmployeeBK`).
- Filtered to the same date range (1996-1999).

This design favors simplicity and reproducibility. It is a full-refresh approach rather than incremental upserts.

## Prerequisites

- Microsoft SQL Server (local or cloud) with permissions to create databases, functions, and procedures.
- The Northwind sample database installed and accessible as `Northwnd`/`NORTHWND` (install this first, or update the scripts to your source database name).
- Sufficient permissions to execute stored procedures.

## Setup and Run

1. Restore the source database:  
   Use SQL Server Management Studio (SSMS) or T-SQL to restore `0.DataBase\Northwind.bak` and ensure it is accessible as `Northwnd`/`NORTHWND`.
2. Create the DW and objects:  
   Run `1.Create Database Norhwind_DW.sql`
3. Create and execute the ETL procedure:  
   Run `2.Insert data procedure.sql`  
   This script both defines and executes `dbo.InsertData`. To run it again later:
   ```
   USE Northwind_DW;
   EXEC dbo.InsertData;
   ```
4. Scheduling (optional):  
   Use SQL Server Agent to run `EXEC dbo.InsertData;` on your desired cadence.

## Validation Examples

Quick row checks:
```
SELECT COUNT(*) AS cnt FROM Northwind_DW.dbo.Dim_Date;
SELECT COUNT(*) AS cnt FROM Northwind_DW.dbo.Dim_Products;
SELECT COUNT(*) AS cnt FROM Northwind_DW.dbo.Dim_Employees;
SELECT COUNT(*) AS cnt FROM Northwind_DW.dbo.Dim_Customers;
SELECT COUNT(*) AS cnt FROM Northwind_DW.dbo.Dim_Orders;
SELECT COUNT(*) AS cnt FROM Northwind_DW.dbo.Fact_Sales;
```

Date consistency (only 1996-1999 in the DW):
```
SELECT MIN([Date]) AS min_date, MAX([Date]) AS max_date
FROM Northwind_DW.dbo.Dim_Date;
```

Fact to date linkage:
```
SELECT TOP (10) fs.*, d.[Date], d.[Year], d.[MonthName]
FROM Northwind_DW.dbo.Fact_Sales fs
JOIN Northwind_DW.dbo.Dim_Date d ON fs.DateKey = d.DateKey;
```

## Notes and Extensions

- Scope: The ETL intentionally limits data to 1996-1999 to match the Northwind sample period and keep the model small.
- Type defaults and NULL handling are simple and transparent to make the pipeline easy to follow.
- Potential enhancements:
  - Switch to incremental loads (for example, based on dates or change data capture).
  - Add surrogate key constraints and foreign keys between fact and dimension tables.
  - Expand the `Dim_Date` table to a wider range.
  - Add more business logic to `ProductType` or additional derived attributes.

Built with T-SQL as a straightforward, maintainable full-refresh ETL suitable for learning, demos, and baseline reporting.
