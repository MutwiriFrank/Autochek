from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
import pandas as pd
from os.path import exists
import requests


default_args = {
    "owner": "Frank",
    "retry" : 3,
    "retry_delay" : timedelta(minutes=4)
}

def download_exchange_rates():
    url = "https://xecdapi.xe.com/v1/convert_to.csv/?to=USD&from=KES,NGN,GHS,UGX,MAD,XOF,EGP&amount=1"
    username = "dataseal766013759"
    password = "270d2ahlgfstrla829u6ur5h9f"
    response = requests.get(url, auth=(username, password))
    response_content = response.content
    csv_file = open('usd_to_cur.csv', 'wb')
    csv_file.write(response_content)
    csv_file.close

def download_exchange_rates_frm():
    url = "https://xecdapi.xe.com/v1/convert_from.csv/?from=USD&to=KES,NGN,GHS,UGX,MAD,XOF,EGP&amount=1"
    username = "dataseal766013759"
    password = "270d2ahlgfstrla829u6ur5h9f"
    response = requests.get(url, auth=(username, password))
    response_content = response.content
    csv_file = open('usd_from_cur.csv', 'wb')
    csv_file.write(response_content)
    csv_file.close

def process_exchange_rates():

    file_exists = exists('/home/mutwiri/airflow/processed_rates.csv')
    if not file_exists:  
        from_usd = pd.read_csv('usd_from_cur.csv', skiprows=[0])
        to_usd = pd.read_csv('usd_to_cur.csv', skiprows=[0])
        from_usd.drop(from_usd.columns[[1]], axis=1, inplace=True)
        from_usd.rename(columns = {'from':'to'}, inplace = True)
        to_usd.drop(to_usd.columns[[1]], axis=1, inplace=True)
        inner_merged = pd.merge(from_usd, to_usd , on=["to", "quotecurrency", "timestamp"] )
        inner_merged.rename(columns = {'to':'currency_to', 'quotecurrency':'currency_from' , 'mid_x': 'USD_to_currency_rate', 'mid_y' : 'USD_from_currency_rate'}, inplace = True)
        inner_merged.to_csv('processed_rates.csv', index=False )

    else :

        from_usd = pd.read_csv('usd_from_cur.csv', skiprows=[0])
        from_usd.rename(columns = {'from':'to'}, inplace = True)
        to_usd = pd.read_csv('usd_to_cur.csv', skiprows=[0])
        from_usd.drop(from_usd.columns[[1]], axis=1, inplace=True)
        to_usd.drop(to_usd.columns[[1]], axis=1, inplace=True)
        incremental = pd.merge(from_usd, to_usd , on=["to", "quotecurrency", "timestamp"] )
        incremental.rename(columns = {'to':'currency_to', 'quotecurrency':'currency_from' , 'mid_x': 'USD_to_currency_rate', 'mid_y' : 'USD_from_currency_rate'}, inplace = True)
    
        incremental.to_csv('processed_rates.csv', mode='a', index=False, header=False)


with DAG (
    dag_id= "get_exchange_rates",
    default_args= default_args,
    start_date= datetime(2023,3,29,2),
    schedule='0 1,23 * * *',
    catchup=True,
) as dag:
    
    '''
    Check if the endpoint exists.  
    '''
    is_xe_api_available = HttpSensor(
        task_id = "is_xe_api_available",
        http_conn_id="xe_exchange_rate_api",
        endpoint='v1/convert_to.json/?to=USD&from=KES,NGN,GHS,UGX,MAD,XOF,EGP&amount=1',
        response_check= lambda response: 200 in [response.status_code],
        poke_interval = 5,
        timeout = 20
    )

    '''
    If the endpoint exists make api get request and get the exchange rates.
    The response returns a CSV we save as USD_to_cur.csv.
    The csv has conversion rate of USD to given currencies
    '''
   
    download_exchange_rates = PythonOperator(
        task_id = "download_exchange_rates",
        python_callable= download_exchange_rates
    )

    '''
    The response returns a CSV we save as USD_from_cur.csv.
    This function gets the exchange rate of USD from given currencies
    
    '''

    download_exchange_rates_frm = PythonOperator(
        task_id = "download_exchange_rates_frm",
        python_callable= download_exchange_rates_frm
    )

    '''
    Process_exchange_rates processes the two returned csvs using pandas.
    The transformations include joining the two csvs, dropping some columns
    droping unwanted rows and  renaming headers 
    
    '''

    process_exchange_rates = PythonOperator(
        task_id = "process_exchange_rates",
        python_callable= process_exchange_rates

    )

    # exchange_rates_spark = spark_submit_operator(
    #     task_id = "exchange_rates_spark",
    #     application = "/home/mutwiri/airflow/dags/scripts/exchange_rates_spark.py",
    #     conn_id = "spark_conn",
    #     verbose = False
    # )




    is_xe_api_available >> [download_exchange_rates, download_exchange_rates_frm]  >> process_exchange_rates