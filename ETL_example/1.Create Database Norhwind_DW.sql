
----1

use master


go


IF EXISTS(select * from sys.databases where name='Northwind_DW')
DROP DATABASE Northwind_DW
go

Create database Northwind_DW
COLLATE SQL_Latin1_General_CP1_CI_AS
go

Use Northwind_DW
go



CREATE TABLE [dbo].[Dim_Products](
	[ProductSK] [int] identity(100,1) PRIMARY KEY NOT NULL,
	[ProductBK] [int] NOT NULL,
	[ProductName] [nvarchar](40) NOT NULL,
	[ProductUnitPrice] [money] NULL,
	ProductType nvarchar(20),
	
	[CategoryName] [nvarchar](15) NOT NULL,
	[SupplierName] [nvarchar](40) NOT NULL,

	[Discontinued] [bit] NOT NULL
)



CREATE TABLE [dbo].[Dim_Employees](
	[EmployeeSK] [int] identity(100,1) PRIMARY KEY NOT NULL,
	[EmployeeBK] [int] NOT NULL,

	[LastName] [nvarchar](20) NOT NULL,
	[FirstName] [nvarchar](10) NOT NULL,
	[FullName] [nvarchar](32) NOT NULL,
	[Title] [nvarchar](30) NULL,
	[BirthDate] [datetime] NULL,
	Age int null,
	[HireDate] [datetime] NULL,
	Seniority int null,
	[City] [nvarchar](15) NULL,
	[Country] [nvarchar](15) NULL,
	[Photo] [image] NULL,
	[ReportsTo] [int] NULL
)




CREATE TABLE [dbo].[Dim_Customers](
	[CustomerSK] int identity(100,1) PRIMARY KEY NOT NULL,
	[CustomerBK] [nchar](5) NOT NULL,
	[CustomerName] [nvarchar](40) NOT NULL,
	[City] [nvarchar](15) NULL,
	[Region] [nvarchar](15) NULL,
	[Country] [nvarchar](15) NULL
)




CREATE TABLE [dbo].[Dim_Orders](
	[OrderSK] [int] identity(100,1) PRIMARY KEY NOT NULL,
	[OrderBK] [int] NOT NULL,
	[ShipCity] [nvarchar](15) NULL,
	[ShipRegion] [nvarchar](15) NULL,
	[ShipCountry] [nvarchar](15) NULL
 )
 





 CREATE TABLE [dbo].[Fact_Sales](
	SalesSK int identity(100,1) PRIMARY KEY not null,
	[OrderSK] [int] NOT NULL,
	[ProductSK] [int] NOT NULL,
	[DateKey] [int] NOT NULL,
	[CustomerSK] [int] NOT NULL,
	[EmployeeSK] [int] NOT NULL,
	[UnitPrice] [money] NOT NULL,
	[Quantity] [smallint] NOT NULL,
	[Discount] [real] NOT NULL
)

create table [dbo].[Dim_Date] (
	DateKey int,
	[Date] date,
	[Year] int,
	[Quarter] int,
	[Month] int,
	[MonthName] 
	nvarchar(20))


go


create function fn_prodact_type(@proid int)
Returns nvarchar (30)
as
begin
declare 
@unitprice int,
@ex nvarchar (30)

set @unitprice = (select UnitPrice from NORTHWND.dbo.products
					where ProductID = @proid )
if
@unitprice > (select avg(UnitPrice) from NORTHWND.dbo.products)
set @ex = 'Expensive'
else
set @ex ='Cheap'
return @ex

end

go
-----------------------------------------------------------------------

create function fn_Dim_Date(@StartDate date, @EndDate Date)

returns @Dim_Date table(
		DateKey int,
		[Date] date, 
		[Year] int,
		[Quarter] int,
		[Month] int,
		[MonthName] nvarchar(20))

as
begin
declare
@i int,
@date date,
@datekey int
set @i =1

set @date = @StartDate
while @i<= (select datediff(day,@StartDate,@EndDate))
begin

set @datekey = (SELECT CONVERT(INT, CONVERT(VARCHAR(8), @date, 112)))

insert into @Dim_Date
values(
@datekey,
@date,
(select year(@date)),
(select DATEPART(QUARTER,@date)),
(select month(@date)),
(SELECT datename(MONTH,@date))
)

set @date = (select dateadd(day,1,(select max([date]) from @Dim_Date)))

set @i =@i + 1

end

return 
end

-----------------------------------------------------
----end

