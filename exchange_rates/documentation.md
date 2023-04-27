## How to run

#### create a virtual Enviroment 
-- virtualenv airflow_env

#### install all the required python packages
-- pip install -r requirements.txt

#### Run Airflow Scheduler
-- airflow scheduler


#### Run Airflow webserver 
-- airflow webserver


## How I solved the problem

1. I created a dag  get_exchange_rates with 4 tasks
-is_xe_api_available
-download_exchange_rates
-download_exchange_rates_frm
-process_exchange_rates

The dag is scheduled to run 2 times a day, first at 1am and second at 11pm.(schedule='0 1,23 * * *')

is_xe_api_available utilises HTTP_Sensor to check if the api exists.
If the api exists,  download_exchange_rates and download_exchange_rates_frm are triggered to run 

 is_xe_api_available >> [download_exchange_rates, download_exchange_rates_frm] 

download_exchange_rates task calls download_exchange_rates which makes a get request to XE.
The response returns a CSV with conversion rate of USD to given currencies which is written to  USD_to_cur.csv.

download_exchange_rates_frm  returns a CSV with conversion rate of USD from the given currencies

process_exchange_rates is then triggered,
[download_exchange_rates, download_exchange_rates_frm]  >> process_exchange_rates
Process_exchange_rates perfoms transormation of the two returned csvs using pandas.
The transformations include joining the two csvs, dropping some columns eg amount
droping unwanted rows and  renaming headers. The function then returns a final csv processed_rates.csv
the function also utilizes os module to check if a file already exist. If processed_rates.csv exist. 
When the dag runs for the second time the new data is appended to  processed_rates.csv.
    
