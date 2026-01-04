/*
============================================================================
Store procedure : Load Silver Layer (Bronze -> Silver)
============================================================================
Script Purpose : 
  This stored procedure performs the ETL ( Extract ,Transformed,Load) process to 
populate the 'silver' schema tables from the 'bronze' schema .

Action Performed: 
- Truncates Silver Tables
- Insert transformed and clease data from bronze tables  into silver tables .

Parameters :
None.
This stored procedure does not accept any parameters or returned values .

Usage Example:
  Exec Silver.Load_silver ;
*/
Create or Alter procedure Silver.load_Silver As
Begin 
Declare @Start_Time DateTime , @End_Time DateTime,@Batch_Start_Time DateTime,@Batch_End_Time DateTime
    Begin Try
       Print '=============================================='
       Print 'Loading Bronze Layer'
       Print '=============================================='
       Print '----------------------------------------------'
       Print'Loading CRM Tables'
       Print '----------------------------------------------'
-- Load Data into Silver.crm_cust_info
   Set @Start_Time = Getdate()
print '> Truncate table for Silver.crm_cust_info'
Truncate Table Silver.crm_cust_info
print '> Insert table for Silver.crm_cust_info'
Insert Into Silver.crm_cust_info (
  cst_id ,
  cst_key,
  cst_firstName,
  cst_lastname,
  cst_Marital_status,
  cst_gndr,
  cst_create_date)

select 
cst_id,
cst_key,
trim(cst_FirstName) as cst_firstName, 
trim(cst_lastName) as cst_lastname,

Case when Upper(Trim( Cst_Marital_Status)) = 'S' then 'Single'
     when Upper(Trim(Cst_Marital_Status)) = 'M' then 'Married' 
     else 'n/a' 
     end Cst_Marital_Status,

Case when Upper(Trim( cst_gndr)) = 'F' then 'Female'
     when Upper(Trim(cst_gndr)) = 'M' then 'Male' 
     else 'n/a' 
     end Cst_gndr,

cst_create_date

from 
(select 
*,
Row_Number () Over (partition by cst_id Order by cst_create_date desc ) as flag_test
from Bronze.crm_cust_info
--where cst_id = 29466
where cst_id is not Null)t
where flag_test = 1
   set @End_Time = GETDATE()
Print'>> Load Duration:' + Cast(DateDiff (Second,@Start_time,@End_time) as Nvarchar) +'Seconds'
print'-------------------------------------------'
  Set @Start_Time = GETDATE()
Print'>> Truncate Silver.crm_prd_info  '
Truncate Table Silver.crm_prd_info 
Print'>> Insert Silver.crm_prd_info  '
INSERT INTO Silver.crm_prd_info 
(
    prd_ID,
    cat_Id,
    prd_key,
    prd_Nm,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    prd_cost
)
SELECT
    Prd_ID,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS Cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS Prd_key,
    Prd_nm,
    CASE UPPER(TRIM(Prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'School'
        WHEN 'M' THEN 'Mount'
        WHEN 'T' THEN 'Training'
        ELSE 'n/a'
    END AS Prd_line,
    CAST(Prd_start_dt AS DATE) AS Prd_Start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt,
    ISNULL(Prd_cost,0) AS Prd_cost
FROM Bronze.crm_prd_info
     Set @End_Time = GETDATE()
Print'>> Load Duration:' + Cast(DateDiff (Second,@Start_time,@End_time) as Nvarchar) +'Seconds'
Print'----------------------------------------------'
     Set @Start_Time = GETDATE()
 Print' >>>Truncate silver.crm_sales_detailes'
Truncate Table silver.crm_sales_detailes
print' >>>insert  silver.crm_sales_detailes'
INSERT INTO silver.crm_sales_detailes
(
      Sls_Ord_num,
      Sls_Prd_key,
      Sls_cust_ID,
      sls_Order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
)
SELECT
      Sls_Ord_num,
      sls_prd_key,
      sls_cust_id,

      CASE 
            WHEN sls_Order_dt = 0 OR LEN(sls_Order_dt) != 8 
                  THEN NULL
            ELSE CAST(CAST(sls_Order_dt AS varchar) AS date)
      END AS sls_Order_dt,

      CASE 
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
                  THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS varchar) AS date)
      END AS sls_ship_dt,

      CASE 
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
                  THEN NULL
            ELSE CAST(CAST(sls_due_dt AS varchar) AS date)
      END AS sls_due_dt,

      CASE 
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales != sls_quantity * ABS(sls_price)
                  THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
      END AS sls_sales,

      sls_quantity,

      CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 THEN
                  CASE 
                        WHEN sls_quantity = 0 THEN NULL
                        ELSE 
                              CASE 
                                    WHEN sls_sales IS NULL
                                      OR sls_sales <= 0
                                      OR sls_sales != sls_quantity * ABS(sls_price)
                                          THEN (sls_quantity * ABS(sls_price)) / NULLIF(sls_quantity,0)
                                    ELSE sls_sales / NULLIF(sls_quantity,0)
                              END
                  END
            ELSE sls_price
      END AS sls_price

FROM bronze.crm_sales_detailes;
    Set @End_Time = GETDATE()
Print'>> Load Duration:' + Cast(DateDiff (Second,@Start_time,@End_time) as Nvarchar) +'Seconds'
Print'-------------------------------------------'
            Set @Start_Time = GETDATE()
Print'>>>>Truncate  table Silver.erp_cust_az12 '
Truncate Table Silver.erp_cust_az12
Print'>>>>Insert table Silver.erp_cust_az12 '
insert into Silver.erp_cust_az12 
(cid,bdate,gen)

Select 

Case when cid like 'NAS%' Then SUBSTRING(cid,4,len(cid))
 Else cid 
 End as CID, 
Case when bdate > getdate() then Null
  else Bdate
  End as Birthdate,
Case when Upper(Trim(gen)) in ('F','Female') Then 'Female'
     when Upper(Trim(gen)) in ('M','Male') Then 'Male'
Else 'n/a'
End Gender 
from Bronze.erp_cust_az12
    Set @Start_Time = GETDATE()
Print'>> Load Duration:' + Cast(DateDiff (Second,@Start_time,@End_time) as Nvarchar) +'Seconds'
print'----------------------------------------------'
        Set @Start_Time = GETDATE()
Print'>>>>>Truncate Table Silver.erp_loc_a101'
insert into Silver.erp_loc_a101 (cid,cntry)
Select
Replace(cid,'-','') cid,
Case when Trim(cntry) = 'DE' then 'Germany'
     when Trim (cntry) in ('US','USA') then 'United State'
     When Trim(cntry) = '' or cntry is Null then 'n/a'
 Else cntry
 END as Cntry
from Bronze.erp_loc_a101
          Set @End_Time = Getdate() 
Print'>> Load Duration:' + Cast(DateDiff (Second,@Start_time,@End_time) as Nvarchar) +'Seconds'
Print'---------------------------------------------'
          Set @Start_Time = GETDATE()
Truncate table Silver.erp_px_cat_giv12
Print '>>>>>> Truncate Table Silver.erp_px_cat_giv12 '
insert into Silver.erp_px_cat_giv12 (
id,
cat,
Subcat,
maintenace )

Select 
id,
cat,
Subcat,
maintenace
from Bronze.erp_px_cat_giv2
    Set @End_Time = GETDATE()
Print'>> Load Duration:' + Cast(DateDiff (Second,@Start_time,@End_time) as Nvarchar) +'Seconds'

Print'------------------------------------------'
Print 'Load Silver Layer is Completed'
Print'>> Total Load Duration:' + Cast(DateDiff (Second,@Start_time,@End_time) as Nvarchar) +'Seconds'

End Try 
Begin Catch
     Print '==========================================='
     Print'Error Ocurred During Loading Bronze Layer'
     Print'Error Message' + Error_Message()
     Print'Error Message' + Cast(Error_Number() as Nvarchar)
     Print'Error Message' + Cast(Error_State() as Nvarchar)
     Print '==========================================='

End Catch 
End 

 
