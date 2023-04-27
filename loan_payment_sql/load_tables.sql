

insert into public.borrower_table (borrower_id,borrower_state,city, zip_code ,  borrower_credit_score  )
select borrower_id,borrower_state,city, zip_code ,  
case when  borrower_credit_score ~ '^[0-9\.]+$' then borrower_credit_score else null end  as borrower_credit_score
from public.stg_borrower_table;
	
 
--load loan_table

insert into public.loan_table
select 
borrower_id,
loan_id,  
TO_DATE(trim(date_of_release), 'MM/DD/YYYY') as date_of_release, 
term,
interest_rate,
loan_amount,
downpayment,
payment_frequency,
 CASE WHEN maturity_date = '02/29/2023' then TO_DATE('03/01/2023', 'MM/DD/YYYY') 
 else  TO_DATE(trim(maturity_date), 'MM/DD/YYYY') end as maturity_date
from public.stg_loan_table;


-- load payment_schedule

insert into public.payment_schedule
select
loan_id,
schedule_id,
TO_DATE(trim(expected_payment_date), 'MM/DD/YYYY') as  epected_payment_date,
expected_payment_amount
from public.stg_payment_schedule;


-- load loan_payment

insert into public.loan_payment(loan_id, payment_id, date_paid, amount_paid )
select 
loan_id,
payment_id,
TO_DATE(trim(amount_paid), 'MM/DD/YYYY') as   date_paid,
cast(date_paid as numeric) as amount_paid
from public.stg_loan_payment;
