/*
================================================================================================================================
DDL SCRIPT: CREATE GOLD VIEWS
================================================================================================================================
SCRIPT PURPOSE:
    This script creates views for the Gold layer in the data warehouse .
    The Gold layer represents the final dimension and fact tables (Star schema) 

Each views performs transformations and combines data from the silver layer to 
produce a clean , enriched , and business-ready dateset .

Usage:
   - These views can be queried directly for analytics and reporting.
===============================================================================================================================
*/
===============================================================================================================================
-- Create Dimension : gold.dim_customers 
==============================================================================================================================
Create View gold.dim_customers as 

    SELECT DISTINCT
   
    ROW_NUMBER() over (Order by cst_id) as Customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstName as FirstName,
    ci.cst_lastName as LastName,
    ci.cst_Marital_status as Marital_Status,
     Case When ci.cst_gndr != 'n/a' then ci.cst_gndr --CRM master for gender info
         Else Coalesce (ca.gen,'n/a')
    End as Gender,
    ci.cst_create_date as CreateDate,
    ca.bdate as BirthDate,
    la.cntry as Country
FROM Silver.crm_cust_info ci
LEFT JOIN (
    SELECT DISTINCT cid, bdate, gen
    FROM Silver.erp_cust_az12
) ca ON ci.cst_key = ca.cid
LEFT JOIN (
    SELECT DISTINCT cid, cntry
    FROM Silver.erp_loc_a101
) la ON ci.cst_key = la.cid;

============================================================================================================================
-- Create Dimesnsion for Products 
============================================================================================================================
Create View Gold.dm_products as 
select 
   Row_Number () over (order by pn.prd_start_dt,pn.prd_key) as Product_key,
   pn.prd_id as product_id,
   pn.prd_key as product_Number,
   pn.prd_nm as product_name,
   pn.cat_id as category_id,
   pc.cat as category,
   pc.subcat sub_category,
   pc.maintenace,
   pn.prd_cost as cost,
   pn.prd_line as product_line ,
   pn.prd_start_dt as start_date
  From Silver.crm_prd_info pn
Left join Silver.erp_px_cat_giv12 pc
ON pn.cat_Id = pc.id
Where prd_end_dt is null --- filter out all historical data
===============================================================================================================================
-- create facts for Sales 
==============================================================================================================================

Create View Gold.fact_Sales as 
Select 
Sd.sls_ord_num as Order_Number,
pr.product_key,
cu.customer_id,
sd.sls_order_dt as Order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as Due_date,
sd.sls_sales as Sales_Amount,
sd.sls_quantity as Quantity,
sd.sls_price as Price 
from silver.crm_sales_detailes sd
Left Join Gold.dim_products pr 
ON sd.sls_prd_key = pr.product_number
Left Join gold.dim_customers cu 
ON sd.Sls_cust_ID = cu.customer_id
