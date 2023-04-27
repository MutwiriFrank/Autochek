## DDLs -- ddls.sql
I first created DDLS for the tables. I decided to introduce a staging layer. THe staging layer wil contain the data as it is from the source with no transomation done.

## lOADING TO STAGING TABLES -- c insert_records_to_staging.sql 

I first loaded all the tables to staging tables using the copy comand on psql terminal. check insert_records_to_staging.sql for queries

## Loading main tables  --load_tables.sql

I then loaded the data from the staging to the main tables. THe main transformation included changing data types, omitiing some values eg on borrower credit score i omitted the letters by using REGEX.


## LOad Report  -- load_report.sql

Load report.sql has the main transforation logic. 
To create the reports i first had to create 3 CTEs. loan_payment_cte, payment_schedule_cte , amount_at_risk_cte I later joined the CTEs and source tables to create the desired report. 

