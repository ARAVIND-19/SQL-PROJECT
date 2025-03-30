-- Advance Data Analysis 
--Change- over time Trends 
-- Sales Performance 
-- by dates
select order_date,sum(sales_amount) total_sales
from gold.fact_sales 
WHERE order_date is not NULL
group by order_date
order by order_date

-- by year
select year(order_date),sum(sales_amount) total_sales,count(distinct(customer_key)) total_customer,sum(quantity) Total_quantity
from gold.fact_sales 
WHERE order_date is not NULL
group by year(order_date)
order by year(order_date)

-- by month
select month(order_date),sum(sales_amount) total_sales,count(distinct(customer_key)) total_customer,sum(quantity) Total_quantity
from gold.fact_sales 
WHERE order_date is not NULL
group by month(order_date)
order by month(order_date)

--by year and month and year 
select year(order_date) order_year,month(order_date) order_month ,sum(sales_amount) total_sales,count(distinct(customer_key)) total_customer,
sum(quantity) Total_quantity
from gold.fact_sales 
WHERE order_date is not NULL
group by year(order_date),month(order_date)
order by year(order_date),month(order_date)

-- using datetrun
select datetrunc(month,order_date) order_month_year,sum(sales_amount) total_sales,count(distinct(customer_key)) total_customer,
sum(quantity) Total_quantity
from gold.fact_sales 
WHERE order_date is not NULL
group by datetrunc(month,order_date)
order by datetrunc(month,order_date)
-- using Format
select format(order_date,'yyyy-MMM') order_month_year,sum(sales_amount) total_sales,count(distinct(customer_key)) total_customer,
sum(quantity) Total_quantity
from gold.fact_sales 
WHERE order_date is not NULL
group by format(order_date,'yyyy-MMM')
order by format(order_date,'yyyy-MMM')

-- How Many Customer where add each year 
select DATETRUNC(year,create_date) as create_year,
count(customer_key) as total_customer from gold.dim_customers 
group by DATETRUNC(year,create_date)
order by DATETRUNC(year,create_date)

--Cumulative Analysis
--- Calculate the total sales per month
-- and the running Total of slaes over time
select 
order_date,
total_sales,
sum(total_sales) over(order by order_date) as running_total_sales
from ( select 
datetrunc(month,order_date) as  order_date,
sum(sales_amount) as total_sales from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)) as ms 

--Performance Analysis
--TASK Analyze the yearly Performance of the product by comparing the each product sales to 
--both its average sales Performance  and previous year sales
--using cte 
with yearly_product_sales as (
select year(f.order_date) order_year,d.product_name,sum(f.sales_amount) current_sales 
from gold.fact_sales f left join gold.dim_products d on f.product_key=d.product_key
where f.order_date is not null 
group by year(f.order_date),d.product_name)

select order_year,product_name,current_sales,avg(current_sales) over(partition by product_name) avg_sales,
current_sales-avg(current_sales) over(partition by product_name) diff_avg,
case when current_sales-avg(current_sales) over(partition by product_name)  > 0 then 'Above Avg'
	 when current_sales-avg(current_sales) over(partition by product_name) < 0 then 'Below Avg'
	 else 'avg'
end avg_change,
lag(current_sales) over (partition by product_name order by order_year) prev_year_sales,
current_sales-lag(current_sales) over (partition by product_name order by order_year) as py_sales,
case when current_sales-lag(current_sales) over (partition by product_name order by order_year)  > 0 then 'Increase'
	 when current_sales-lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
	 else 'No_change'
end indicater
from yearly_product_sales
order by product_name,order_year

--Proportional Analysis
-- Which categories contribute the most to overall sales 
with categories_sales as (
select d.category,sum(f.sales_amount) as total_Sales
from gold.fact_sales f  left join gold.dim_products d on f.product_key=d.product_key
group by d.category)

select category,total_Sales,SUM(total_sales) over() overallsales,
ROUND((CAST(total_Sales AS FLOAT)/SUM(total_sales) over())*100,2) as percentage_of_total
from categories_sales ORDER BY total_Sales DESC

-- Data Segmentation

--- Segment products into cost ranges and count how many products fall into each segment.
with cost_segment as (
SELECT
product_key,
product_name,
cost,
CASE WHEN COST <100 THEN 'Below 100'
When cost between 100 and 500 then '100-500'
when cost between 500 and 1000 then '500-1000'
else 'Above 1000' 
end cost_range
FROM gold.dim_products)

select cost_range,COUNT(product_key) total_product from cost_segment group by cost_range order by total_product desc

--Group customers into three segments based on their spending behavior: 
--VIP: at least 12 months of history and spending more than €5,000. 
--Regular: at least 12 months of history but spending €5,000 or less. 
--New: lifespan less than 12 months.
--And find the total number of customers by each group.
with customer_group as(
select c.customer_key,sum(f.sales_amount) total_sales ,min(f.order_date) first_date,max(f.order_date) last_date,DATEDIFF(month,min(f.order_date),max(f.order_date)) life_span
from gold.fact_sales f left join gold.dim_customers c on f.customer_key=c.customer_key
group by c.customer_key)

select segment,COUNT(segment) from (
select Case when life_span >=12 and total_sales >5000 then 'VIP' 
when life_span>=12 and total_sales<=5000 then 'Regular'
else 'NEW'
end segment from customer_group) t group by segment order by  COUNT(segment) desc

/*
==========================================================================================================================================================
Customer Report
==========================================================================================================================================================

Purpose: 
	-This report consolidates key customer metrics and behaviors 

Highlights: 
	1. Gathers essential fields such as names, ages, and transaction details. 
	2. Segments customers into categories (VIP, Regular, New) and age groups. 
	3. Aggregates customer-level metrics: 
		total orders 
		total sales 
		total quantity purchased 
		total products 
		lifespan (in months) 
	4. Calculates valuable KPIs: 
		recency (months since last order) 
		average order value 
		average monthly spend

===================================================================================================================================================================
*/

/*----------------------------------------------------------------------------------------------------------------------------------------------------------- 
1) Base Query : retireves Core  Columns  from tables
----------------------------------------------------------------------------------------------------------------------------------------------------------------*/
with base_query as (
select f.order_number,f.order_date,f.sales_amount,f.product_key,
f.quantity,c.customer_key,c.customer_number,c.first_name+' '+c.last_name Full_name ,datediff(year,c.birthdate,GETDATE()) as age
from gold.dim_customers c 
left join gold.fact_sales f on c.customer_key=f.customer_key
where order_date is not null and birthdate is not null)  
/*	3. Aggregates customer-level metrics: 
		total orders 
		total sales 
		total quantity purchased 
		total products 
		lifespan (in months) */
,customer_aggregation as (
select 
customer_key,
customer_number,
Full_name,
age,
COUNT(distinct(order_number)) as Total_order,
sum(sales_amount) Total_sales,sum(quantity) Total_quantity,
count(distinct(product_key)) total_product,max(order_date) last_order_date,
min(order_date) first_order_date,
DATEDIFF(month,min(order_date),max(order_date)) Lifespan
from base_query 
group by customer_key,
customer_number,
Full_name,
age)


select 
customer_key,customer_number,
Full_name,age,
Case 
	when age<20 then 'under 20'
	when age between 20 and 29 then 'under 30'
	when age between 30 and 39 then 'under 40'
	when age between 40 and 49 then 'under 50'
	else '50 and above'
end age_group,
Case 
	when Lifespan >=12 and Total_sales >5000 then 'VIP' 
	when Lifespan>=12 and Total_sales<=5000 then 'Regular'
	else 'NEW' 
end customer_segment,
DATEDIFF(month,last_order_date,getdate()) recency,
Total_order,Total_sales,Total_quantity,
total_product,Lifespan,
Case 
	when total_order=0 then 00
	else (Total_sales/Total_order) 
end avg_order_value,
-- compute avg monthly sales
Case 
	when Lifespan=0 then total_sales
	else (Total_sales/Lifespan) 
	end avg_monthly_spend
from customer_aggregation

-- creating view from above query's and cte for visualaztion 
create view gold.view_report_customer as
with base_query as (
select f.order_number,f.order_date,f.sales_amount,f.product_key,
f.quantity,c.customer_key,c.customer_number,c.first_name+' '+c.last_name Full_name ,datediff(year,c.birthdate,GETDATE()) as age
from gold.dim_customers c 
left join gold.fact_sales f on c.customer_key=f.customer_key
where order_date is not null and birthdate is not null)
,customer_aggregation as (
select 
customer_key,
customer_number,
Full_name,
age,
COUNT(distinct(order_number)) as Total_order,
sum(sales_amount) Total_sales,sum(quantity) Total_quantity,
count(distinct(product_key)) total_product,max(order_date) last_order_date,
min(order_date) first_order_date,
DATEDIFF(month,min(order_date),max(order_date)) Lifespan
from base_query 
group by customer_key,
customer_number,
Full_name,
age)
select 
customer_key,customer_number,
Full_name,age,
Case 
	when age<20 then 'under 20'
	when age between 20 and 29 then 'under 30'
	when age between 30 and 39 then 'under 40'
	when age between 40 and 49 then 'under 50'
	else '50 and above'
end age_group,
Case 
	when Lifespan >=12 and Total_sales >5000 then 'VIP' 
	when Lifespan>=12 and Total_sales<=5000 then 'Regular'
	else 'NEW' 
end customer_segment,
DATEDIFF(month,last_order_date,getdate()) recency,
Total_order,Total_sales,Total_quantity,
total_product,Lifespan,
Case 
	when total_order=0 then 00
	else (Total_sales/Total_order) 
end avg_order_value,
-- compute avg monthly sales
Case 
	when Lifespan=0 then total_sales
	else (Total_sales/Lifespan) 
	end avg_monthly_spend
from customer_aggregation
/* 
=== 
Product Report 
=== 
Purpose: 
This report consolidates key product metrics and behaviors. 
Highlights: 
1. Gathers essential fields such as product name, category, subcategory, and cost. 
2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers. 
3. Aggregates product-level metrics: 
total orders 
total sales 
total quantity sold 
total customers (unique) 
lifespan (in months) 
4. Calculates valuable KPIs: 
recency (months since last sale) 
average order revenue (AOR) 
average monthly revenue
*/

CREATE VIEW gold.report_product AS
WITH B_QUERY AS (
    SELECT 
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.category,
        p.product_name,
        p.subcategory,
        p.cost,
        p.product_key
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
    WHERE f.order_date IS NOT NULL
),
product_aggregation AS (
    SELECT 
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS Lifespan,
        MAX(order_date) AS last_sale_date,
        COUNT(DISTINCT order_number) AS Total_orders,
        COUNT(DISTINCT customer_key) AS Total_customers,
        SUM(sales_amount) AS Total_sales,
        SUM(quantity) AS Total_quantity,
        ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
    FROM B_QUERY
    GROUP BY product_key, product_name, category, subcategory, cost
)

SELECT 
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    DATEDIFF(Month, last_sale_date, GETDATE()) AS recency_in_months,
    CASE 
        WHEN Total_sales > 50000 THEN 'High-performer'
        WHEN Total_sales >= 10000 THEN 'Mid-Range'
        ELSE 'Low-Performer'
    END AS product_segment,
    Lifespan,
    Total_orders,
    Total_customers,
    Total_sales,
    Total_quantity,
    avg_selling_price,
    CASE
        WHEN Total_orders = 0 THEN 0
        ELSE Total_sales / Total_orders
    END AS avg_order_revenue,
    CASE
        WHEN Lifespan = 0 THEN Total_sales
        ELSE Total_sales / Lifespan
    END AS avg_monthly_revenue
FROM product_aggregation;

