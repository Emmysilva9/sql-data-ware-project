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
go
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
) go

  If OBJECT_ID ( 'silver.crm_cust_info','U') is not Null
   Drop Table  silver.crm_cust_info 
Go
   Create Table Silver.cust_info(
cst_Id                   int,
cst_key                  nvarchar(50),
cst_firstname             nvarchar(50),
cst_lastName             nvarchar(50),
cst_marital_status        nvarchar  (50),
cst_gndr                   nvarchar(50),
cst_create_date            nvarchar(50),
dwh_create_date            nvarchar(50)
)   go
    
   If OBJECT_ID ( 'silver.crm_prd_info','U') is not Null
  Drop Table  silver.crm_prd_info
Go
    create table silver.crm_prd_info (
    prd_ID                INT,
    prd_key               Nvarchar(50),
    prd_NM                Nvarchar(50),
    prd_cost              int,
    prd_line               nvarchar(50),
    prd_start_dt          date,
    prd_end_dt            date
    ) Go

    
    If OBJECT_ID ( 'silver.erp_cust_az12','U') is not Null
  Drop Table  silver.erp_cust_az12
Go
    Create table silver.erp_cust_az12 (
cid            nvarchar(50),
bdate          date ,
gen            nvarchar(50)
) Go 

   If OBJECT_ID ( ' silver.erp_loc_a101','U') is not Null
  Drop Table  silver.erp_loc_a101
Go
    Create Table silver.erp_loc_a101 (
    cid    nvarchar(50),
    cntry   nvarchar(50)
    ) Go 
  
    If OBJECT_ID ( ' silver.erp_px_cat_giv12','U') is not Null
  Drop Table silver.erp_px_cat_giv12
go
    Create table silver.erp_px_cat_giv12(
    id                    nvarchar(50),
    cat                   nvarchar(50),
    subcat                nvarchar(50),
    maintenace             nvarchar(50)
    ) go

