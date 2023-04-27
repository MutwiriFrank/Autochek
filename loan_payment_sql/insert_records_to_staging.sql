-- on psql terminal

-- Load borrowers to a staging table first (borrower_temp_table) since we have an extra column

\COPY stg_borrower_table  FROM /home/mutwiri/Desktop/Autochek_Interview/data/Borrower_Data.csv DELIMITERS ',' CSV HEADER;

--load loan table
\COPY stg_loan_table  FROM /home/mutwiri/Desktop/Autochek_Interview/data/Loan_Data.csv  DELIMITERS ',' CSV HEADER;


-- load a staging table(loan_payment_temp) first. amount paid and date paid are swapped
\COPY stg_payment_schedule  FROM /home/mutwiri/Desktop/Autochek_Interview/data/Schedule_Data.csv DELIMITERS ',' CSV HEADER;


-- load payment staging with raw csv data
\COPY stg_loan_payment  FROM /home/mutwiri/Desktop/Autochek_Interview/data/Repayment_Data.csv  DELIMITERS ',' CSV HEADER;





