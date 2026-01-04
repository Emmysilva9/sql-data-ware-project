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
cst_Id                   int,
cst_key                  nvarchar(50),
cst_firstname             nvarchar(50),
cst_lastName             nvarchar(50),
cst_marital_status        nvarchar  (50),
cst_gndr                   nvarchar(50),
cst_create_date            nvarchar(50),
dwh_create_date            nvarchar(50)


    If OBJECT_ID ( 'silver.crm_prd_info','U') is not Null
  Drop Table  silver.crm_prd_info

    create table Bronze.crm_prd_info (
    prd_ID                INT,
    prd_key               Nvarchar(50),
    prd_NM                Nvarchar(50),
    prd_cost              int,
    prd_line               nvarchar(50),
    prd_start_dt          datetime,
    prd_end_dt            datetime
    ) 
  
