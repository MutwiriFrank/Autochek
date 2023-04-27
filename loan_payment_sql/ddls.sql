-- staging tables ddls

create table stg_borrower_table(
	borrower_id varchar(50),
	borrower_state varchar(50),
	city varchar(50),
	zip_code  varchar(50),
	borrower_credit_score varchar(50)
);

create table stg_loan_table(
	borrower_id varchar(50),
	loan_id varchar(50),
	date_of_release varchar(50),
	term integer,
	interest_rate  numeric, 
	loan_amount numeric,
	downpayment numeric,
	payment_frequency numeric,
	maturity_date varchar(50)
);

create table stg_payment_schedule(
	loan_id varchar(50),
	schedule_id varchar(50),
	expected_payment_date varchar(50),
	expected_payment_amount numeric
	);
	
create table stg_loan_payment(
	loan_id varchar(50),
	payment_id varchar(50),
	amount_paid varchar(50),
	date_paid  varchar(50)
); 


-- main ddls 
 
create table borrower_table(
	borrower_id varchar(50) PRIMARY KEY,
	borrower_state varchar(50),
	city varchar(50),
	zip_code  varchar(50),
	borrower_credit_score varchar(50)
);
 
create table loan_table(
	borrower_id varchar(50) REFERENCES borrower_table(borrower_id),
	loan_id varchar(50) PRIMARY KEY,
	date_of_release date,
	term integer,
	interest_rate  numeric, 
	loan_amount numeric,
	downpayment numeric,
	payment_frequency numeric,
	maturity_date date
);

create table payment_schedule(
	loan_id varchar(50) REFERENCES loan_table(loan_id) ,
	schedule_id varchar(50) PRIMARY KEY,
	expected_payment_date date,
	expected_payment_amount numeric
	);
	
create table loan_payment(
	loan_id varchar(50) REFERENCES loan_table(loan_id)  , 
	payment_id varchar(50) PRIMARY KEY,
	amount_paid numeric,
	date_paid  date
); 


