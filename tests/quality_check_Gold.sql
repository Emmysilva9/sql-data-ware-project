/*
============================================================================================
Quality Checks 
===========================================================================================
Script purpose :
This script performs quality checks to validate the integrity , consistency , and 
accuracy of Gold layer . These checks ensure :
- Uniqunesss of surrogate keys in dimension tables .
- Referential integrity between facts and dimesion tables.
- validation of relationship in the data model for anaytical purposes.

Usage Notes:
- Run these checks after loading Data Silver layer.
- Investigate and receive any discrepencies found during the checks
==========================================================================================
*/
============================================================================================
-- Checking 'Gold.dim_customers'
========================================================================================
-- checking for the uniqueness of customer_key in gold.dim_customers 
-- Expectation : No result 
Select 
  customer_key,
 count(*) as Duplicate_count
from gold.dim_customers 
Group by customer_key 
Having count(*) > 1

