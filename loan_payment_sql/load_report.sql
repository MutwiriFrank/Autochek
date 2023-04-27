with loan_payment_cte as (
select loan_id, amount_paid , date_paid  ,
row_number() over(partition by loan_id order by date_paid desc) as rn
from public.loan_payment
),

payment_schedule_cte as (
select loan_id, expected_payment_date, expected_payment_amount,
row_number() over(partition by loan_id order by expected_payment_date desc) AS rn
from public.payment_schedule
),

amount_at_risk_cte as (
select distinct loan_id, expected_payment_date,total_expected , total_paid ,
total_expected - total_paid as balance
from(
	select *,
		sum(expected_payment_amount) over (partition by loan_id order by expected_payment_date asc) as total_expected,
		sum(amount_paid) over (partition by loan_id order by expected_payment_date asc) as total_paid
	from 
	(
		select ps.loan_id, max(expected_payment_amount) as expected_payment_amount,  
		sum(case when amount_paid is null then 0 else amount_paid end) as amount_paid ,
		 expected_payment_date
		from   payment_schedule ps 
		left join loan_payment lp on lp.loan_id = ps.loan_id 
			and  extract (month from lp.date_paid ) = extract (month from ps.expected_payment_date )
			and extract (year from lp.date_paid ) = extract (year from ps.expected_payment_date )
		group by ps.loan_id,expected_payment_date 	
		)a
)a
order by loan_id
)


SELECT lot.loan_id, lot.borrower_id, date_of_release as loan_date_of_release, term, loan_amount, 
downpayment, borrower_state, city, zip_code, payment_frequency, maturity_date , 
(lpc.date_paid - psc.expected_payment_date ) as current_days_past_due,
psc.expected_payment_date as last_due_date, 
lpc.date_paid as last_repayment_date,
arc.balance as amount_at_risk, 
bot.borrower_credit_score, '' as branch, '' as branch_id, '' as borrower_name ,
arc.total_paid as total_amount_paid, arc.total_expected as total_amount_expected

FROM payment_schedule_cte psc
left join loan_payment_cte lpc on  psc.loan_id =lpc.loan_id and psc.rn = lpc.rn 
inner join public.loan_table lot on lot.loan_id = psc.loan_id
inner join public.borrower_table  bot on lot.borrower_id = bot.borrower_id
inner join amount_at_risk_cte arc on arc.loan_id = psc.loan_id and psc.expected_payment_date = arc.expected_payment_date
 order by psc.loan_id, psc.expected_payment_date


