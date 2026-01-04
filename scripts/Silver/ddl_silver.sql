/*
===========================================================================
DDL Script : Create Silver Table 
==========================================================================
Script Purpose 
   This script create tables in the 'silver ' schema , dropping existing tables 
if they are already exist .
Run this script to re-defined the DDL structure of bronze tables .
=============================================================================
*/
If OBJECT_ID ( 'silver.crm_sales_detailes','U') is not Null
   Drop Table  silver.crm_sales_detailes
Create Table  silver.crm_sales_detailes (
Sls_Ord_num Nvarchar(50),
Sls_Prd_key     Nvarchar(50),
Sls_cust_ID      int,
sls_Order_dt    int,
sls_ship_dt     int,
sls_due_dt      int ,
sls_sales       int,
sls_quantity     int,
sls_price       int
)

  If OBJECT_ID ( 'silver.crm_cust_info','U') is not Null
   Drop Table  silver.crm_cust_info
cst
  
  
  If OBJECT_ID ( 'Bronze.crm_sales_detailes','U') is not Null
   Drop Table  Bronze.crm_sales_detailes
