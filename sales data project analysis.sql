/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouseAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'DataWarehouseAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouseAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseAnalytics')
BEGIN
    ALTER DATABASE DataWarehouseAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseAnalytics;
END;
GO

-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
GO

USE DataWarehouseAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'C:\Users\rajha\Desktop\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

GO


TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'C:\Users\rajha\Desktop\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM 'C:\Users\rajha\Desktop\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

-- grouping by date and year to find the total slaes accordingly


select 
DATETRUNC(month,order_date) as order_month,
sum(sales_amount) as total_sales 
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month,order_date)
order by  DATETRUNC(month,order_date)


-- finding the running total 
select 
order_month,
order_year,
total_sales,
SUM(total_sales) over (partition by order_year order by order_month ) as running_total_per_year
from
(
select 
YEAR(order_date) as order_year,
DATETRUNC(MONTH,order_date) as order_month,
SUM(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by DATETRUNC(MONTH,order_date) , YEAR(order_date)

) t ;

-- Analyze the yearly performance of products by comapring sales to both abg sales and previous year sales 


-- we write a cte that joins the fact table and product table , after that it finds the sum of sale per year 
-- per products 
with yearly_product_sales as(
select 
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from 
gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null
group by 
year(f.order_date),
p.product_name
)


-- we find the average sales - current sales of each year per product to compare them 

select 
order_year,
product_name,
current_sales,

-- year over year analysis
LAG(current_sales) over ( partition by product_name order by order_year) as py_sales,
current_sales - LAG(current_sales) over ( partition by product_name order by order_year) as py_diff,
-- yoy 

AVG(current_sales ) over (partition by product_name ) as average_sales ,

current_sales - AVG(current_sales ) over (partition by product_name ) as diff_avg ,
case when current_sales - AVG(current_sales ) over (partition by product_name )> 0 then 'above_average'
	 when current_sales - AVG(current_sales ) over (partition by product_name )<0 then 'below_average'
	 else 'avg'
end avg_change,
case when current_sales - LAG(current_sales) over ( partition by product_name order by order_year)> 0 then 'above'
	 when current_sales - LAG(current_sales) over ( partition by product_name order by order_year)<0 then 'below'
	 else 'same'
end py_change 
from 
yearly_product_sales
order by product_name, order_year

-- we are categorizing the prodcuts and finding which category contribute how much to the total sales 

with category_sales as ( 
select 
category,
sum(sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key=f.product_key
group by category

)

select 
category,
total_sales,
SUM (total_sales) OVER () as overall_sales,
concat (Round((cast(total_sales as float)/SUM (total_sales) OVER () )*100,2), '%') as percentage_of_total
from category_sales
order by total_sales desc


-- segment products into cost ranges and count how many products fall into each segment 
with product_segments as(
select 
product_key,
cost,
product_name,
case when cost <100 then 'below 100'
	 when cost between 100 and 500 then '100-500'
	 when cost between 500 and 1000 then '500-1000'
	 else 'aobve 1000'
end cost_range 
from gold.dim_products

)

select 
cost_range,
COUNT(product_key) as total_products
from product_segments 
group by cost_range
order by total_products

/*
Group customers into three segments baised on their spending behaviour :
 vip :Customers with atleast 12 months of spending more than 5000 euro
 regular: history of 12 months but spending 5000 or less
 new : Customers with a lifespan less than 12 months 
and find total number of customres by each group 
*/
with customer_spending as (
select 
c.customer_key,
sum (f.sales_amount) as total_sale,
MIN(f.order_date) as first_order,
Max(f.order_date) as last_order,
DATEDIFF(MONTH,MIN(f.order_date),Max(f.order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c .customer_key
group by c.customer_key 
)

select 
count(customer_key) as total_customres,
customer_segment
from
(
select
customer_key,
case when lifespan>=12 and total_sale>5000 then 'VIP'
	 when lifespan>=12 and total_sale <= 5000 then 'regular'
	 else 'new'
end as customer_segment
from 
customer_spending
) t
group by customer_segment
order by total_customres desc

