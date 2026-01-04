/*
--------------------------------------------------------------------------------------
Quality Check 
--------------------------------------------------------------------------------------
Script Purpose 
This script perform various quality check for data consistencey,accuracy and standardization 
across the silver schema . It includes the Checks for :
- Null or duplicate primary keys 
- Unwanted Spaces in string field 
- Data standardization and consistency 
- invalid data range and orders 
-Data consistency between related fields.

Usage Notes 
- Run these checks after data loading  silver layer 
- Investigate and resolve any discrepencies found during tne checks 
*/
----------------------------------------------------------------------------------------
Select 
id,
cat,
Subcat,
maintenace
from Bronze.erp_px_cat_giv2

-- Checking for unwanted space 
select 
cat,Subcat,maintenace 
from Bronze.erp_px_cat_giv2
where subcat != trim(subcat) or Subcat != trim(subcat)
Or maintenace != trim(maintenace)

-- Remove the minus -  Example AW-0011

Select
cid,
Replace(cid,'-','') cid,
cntry
from Bronze.erp_loc_a101

-- Data Standization and Consistency 

select distinct 
cntry ,
Case when Trim(cntry) = 'DE' then 'Germany'
     when Trim (cntry) in ('US','USA') then 'USA'
     When Trim(cntry) = '' then 'n/a'
 Else cntry
 END as Cntry
 from Bronze.erp_loc_a101

-- identify out of range (Rough work)

Select 
cid,
Case when cid like 'NAS%' Then SUBSTRING(cid,4,len(cid))
 Else cid 
 End as CID, 
bdate,
Case when bdate > getdate() then Null
  else Bdate
  End Bdate,
gen 
from Bronze.erp_cust_az12
--where bdate < '1924-01-01' or bdate > getdate()

 -- standardization and consistency 
 
 select distinct 
 gen ,
 Case when Upper(Trim(gen)) in ('F','Female') Then 'Female'
      when Upper(Trim(gen)) in ('M','Male') Then 'Male'
Else 'n/a'
End Gen 
from Bronze.erp_cust_az12

-- Check data consistency : between sales,Quantity and price 
-->> Sales = Quantity * price 
-->> Values must not be null,negative or zero 

Select 
sls_sales,
sls_quantity,
sls_price 
from Bronze.crm_sales_detailes
where Sls_sales != sls_quantity * sls_price

-- To work on Invalid date 

select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
Lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt
from Bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')

--check for negtive numbers or null 
-- Expection : no results 

select prd_cost 
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

select 
cst_firstname 
from Bronze.crm_cust_info
where cst_FirstName != trim(cst_firstName)

-- if origina is not equal after trimming ,there are space
-- Checking for Nulls or Duplicates in Primary key 
-- Expection No reults 

Select 
prd_id,
Count(*)
from Bronze.crm_prd_info
Group by prd_id
Having Count(*) > 1 or prd_id is Null

