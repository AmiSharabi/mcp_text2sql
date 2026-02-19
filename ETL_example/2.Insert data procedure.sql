

--1

use Northwind_DW

GO


--------------------------------------------------------------------------
--2
create PROCEDURE dbo.InsertData 

as
truncate table Dim_Date
truncate table Dim_Products
truncate table Dim_Employees
truncate table Dim_Customers		
truncate table Dim_Orders
truncate table Fact_Sales


insert into Dim_Date
			select * from fn_Dim_Date('1996-01-01','1999-12-31')





insert into Dim_Products([ProductBK],[ProductName],[ProductUnitPrice],[ProductType],[CategoryName],[SupplierName],[Discontinued])
			select ProductID, ProductName, UnitPrice,(select  dbo.fn_prodact_type(ProductID)), CategoryName,CompanyName,Discontinued
			from Northwnd.dbo.Products P
			join Northwnd.dbo.Categories c
			on c.CategoryID = p.CategoryID
			join Northwnd.dbo.Suppliers s
			on s.SupplierID = p.SupplierID

update Dim_Products
set ProductType = 'Unknown'
where ProductType is null





insert into Dim_Employees (EmployeeBK, LastName, FirstName,FullName ,Title, BirthDate,age,HireDate, Seniority, City, Country, Photo, ReportsTo)

			select EmployeeID,LastName,FirstName,FirstName +' '+ LastName, Title, BirthDate, year(GETDATE())- year(BirthDate),
			HireDate,year(GETDATE())- year(HireDate), City,Country, Photo, ReportsTo  
			from Northwnd.dbo.Employees

							update Dim_Employees
							set Title ='Unknown'
							where Title is null

					

							update Dim_Employees
							set BirthDate ='3000-01-01'
							where BirthDate is null

							update Dim_Employees
							set Age ='-1'
							where age is null

							update Dim_Employees
							set HireDate ='3000-01-01'
							where HireDate is null

							update Dim_Employees
							set Seniority ='-1'
							where Seniority is null

							update Dim_Employees
							set City ='Unknown'
							where City is null

							update Dim_Employees
							set ReportsTo ='-1'
							where ReportsTo is null

							update Dim_Employees
							set Age ='-1'
							where age is null

							update Dim_Employees
							set Country ='Unknown'
							where Country is null

							update Dim_Employees
							set Photo ='Unknown'
							where Photo is null



insert into Dim_Customers (CustomerBK, CustomerName, City, Region, Country)
			
			select CustomerID, CompanyName, City, Region, Country from Northwnd.dbo.Customers


							update Dim_Customers
							set City ='Unknown'
							where City is null

							update Dim_Customers
							set Region ='Unknown'
							where Region is null

							update Dim_Customers
							set Country ='Unknown'
							where Country is null
					
		


insert into Dim_Orders (OrderBK, ShipCity, ShipRegion, ShipCountry)	

			select OrderID, ShipCity, ShipRegion, ShipCountry from Northwnd.dbo.Orders
			where OrderDate between '1996-01-01'and '1999-12-31'


							update Dim_Orders
							set ShipCity ='Unknown'
							where ShipCity is null

							update Dim_Orders
							set ShipRegion ='Unknown'
							where ShipRegion is null
							
							update Dim_Orders
							set ShipCountry ='Unknown'
							where ShipCountry is null




insert into Fact_Sales (OrderSK, ProductSK, DateKey, CustomerSK, EmployeeSK, UnitPrice, Quantity, Discount)

			select OrderSK ,ProductSK,DateKey ,CustomerSK,EmployeeSK ,od.UnitPrice, od.Quantity, od.Discount from Northwnd.dbo.[Order Details] od
			join Dim_Orders do on do.OrderBK = od.OrderID
			join Northwnd.dbo.Orders o on do.OrderBK = o.OrderID
			join Dim_Products p on p.ProductBK = od.ProductID
			join Dim_Date d on d.[Date] = o.OrderDate
			join Dim_Customers c on c.CustomerBK = o.CustomerID
			join Dim_Employees e on e.EmployeeBK = o.EmployeeID
			where OrderDate between '1996-01-01'and '1999-12-31'


--------------------------------------------------------------------------------------------------
--4
go



exec InsertData


--5
--check

			select * from Dim_Date
			select * from Dim_Products
			select * from Dim_Employees
		    select * from Dim_Customers
			select * from Dim_Orders
			select * from Fact_Sales

--drop procedure InsertData