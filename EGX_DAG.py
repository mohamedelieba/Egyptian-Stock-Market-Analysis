from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup, SoupStrainer
import re
import math
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter
import numpy as np
import seaborn as sns
import json


SYMBOLS_PATH = '/home/elieba/airflow/files/symbols.txt'
# Define default arguments that are passed to each task
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 9),  # Start date of the DAG
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

# Define the DAG, passing in the DAG name, default arguments, and schedule interval
with DAG(
    'EGX_DAG',  # Name of the DAG
    default_args=default_args,
    description='EGX Analysis Pipeline',
    schedule=timedelta(days=1),  # Run the DAG every day
    catchup=False,  # Skip backfilling for the DAG
) as dag:

    # Define tasks using operators

    # Task 1: Fetch All EGX Listed Companies
    def fetch_symbols():
        resp = requests.get('https://www.tradingview.com/markets/stocks-egypt/market-movers-large-cap/')
        soup = BeautifulSoup(resp.text)
        symbols = []
        with open(SYMBOLS_PATH, 'w') as file:
            for i, tag in enumerate(soup.findAll('a', attrs={'class': re.compile('^apply-common-tooltip')})): 
                if(i%2==0 and tag.string != 'EGS923M1C017'):
                    file.write(tag.string + '\n')

    fetch_task = PythonOperator(
        task_id='fetch_symbols',  # Unique ID for the task
        python_callable=fetch_symbols,  # Python function to execute
    )

    # Task 2: Wait for the symbols file to be written
    sensor_task = FileSensor( task_id= "my_file_sensor_task",
    poke_interval= 10,  
    timeout = 10 * 60,  
    filepath= SYMBOLS_PATH) 

    
    def has_book_value(tag):
        return tag.string and re.compile("^Book Value").search(tag.string.get_text())

    def has_market_cap(tag):
        return tag.string and re.compile("Market Cap").search(tag.string.get_text())

    def has_total_shares(tag):
        return tag.string and re.compile("Current Total Shares").search(tag.string.get_text())
    def has_eps(tag):
        return tag.string and re.compile("EPS").search(tag.string.get_text())
    
    base_url = 'https://english.mubasher.info/markets/EGX/stocks/'
    
    symbols_list = []
    update_dates = []
    ratios = []
    names = []
    EPS = []
    prices = []
    book_values =[]

    def parse(**kwargs):
        with open(SYMBOLS_PATH, 'r') as file:
            lines = file.readlines()
        symbols = [line.strip() for line in lines]

        for symbol in symbols:
            resp = requests.get(base_url+symbol)
            div_tags = SoupStrainer(attrs={'class': re.compile('^stock-overview')})
            soup = BeautifulSoup(resp.text, 'lxml', parse_only=div_tags)
            book_value_span = soup.find_all(has_book_value)[0]
            
            eps_span = soup.find_all(has_eps)[0]
            quarter = book_value_span.find_next_sibling('span').find('span', class_='market-summary__date').string.get_text()
            
            book_value = float(book_value_span.find_next_sibling('span').find('span', class_=re.compile('^number')).string.replace(',',''))
            
            currency = book_value_span.find_next_sibling('span').find('span', string='Egyptian Pound')

                                                                    
            eps = float(eps_span.find_next_sibling('span').find('span', class_=re.compile('^number')).string.get_text())
            
            h1_tags = SoupStrainer('h1')
            soup = BeautifulSoup(resp.text, 'lxml', parse_only=h1_tags)
            name = soup.find('h1').string
        

            
            price_tags = SoupStrainer('div', attrs={'class': re.compile('^market-summary__last-price')})
            soup = BeautifulSoup(resp.text, 'lxml', parse_only=price_tags)
            last_price = float(soup.find().string)
            if book_value != 0 and currency != None:
                p2B = round(last_price/book_value,2)
                if p2B < 1 and p2B > 0:
                    symbols_list.append(symbol)
                    update_dates.append(quarter.replace('\n', ' ').strip("Based on: "))
                    ratios.append(p2B)
                    names.append(name)
                    EPS.append(eps)
                    prices.append(last_price)
                    book_values.append(book_value)

        data = {'Company': names, 'Symbol': symbols_list,'EPS': EPS, 'Price':prices,'Book Value': book_values, 'P/B': ratios, 'Date of Book Value Update': update_dates}

        df = pd.DataFrame(data)
        df['year'] = df['Date of Book Value Update'].str[-4:]
        df['Quarter'] = df['Date of Book Value Update'].str.split().str[0]
        df['Company'] = df['Company'].str.split().str[:-1].str.join(' ')
        quarter_order = ['First', 'Second', 'Third', 'Fourth']
        df['Quarter'] = pd.Categorical(df['Quarter'], categories=quarter_order, ordered=True)
        df['Diff_PCT %'] = round(((df['Book Value'] - df ['Price'] )/ df['Price']) * 100,2)
        df = df.sort_values(by=['Diff_PCT %', 'year','Quarter'], ascending=False)
        df = df[['Company', 'Symbol', 'P/B','Price','Book Value','Date of Book Value Update', 'Diff_PCT %']]
        df = df.reset_index(drop=True)
        ti = kwargs['ti']
        compnies_dict = df.to_dict(orient='records')
        companies_json = json.dumps(compnies_dict)
        ti.xcom_push(key='companies_json', value = companies_json)

    parse_compnies = PythonOperator(
        task_id = 'parse_companies',
        python_callable=parse
    )

    def draw(**kwargs):

        ti = kwargs['ti']
        companies_json = ti.xcom_pull(task_ids='parse_companies', key='companies_json')
        compnaies_dict = json.loads(companies_json)
        df = pd.DataFrame(compnaies_dict)

        fig, ax = plt.subplots(figsize=(10,6))

        x = np.arange(len(df))
        ax.set_xticks(x)
        ax.set_xticklabels(df['Company'], rotation='vertical')

        ax.bar(x-0.2, df['Price'], width=0.4, label='Actual Price')
        ax.bar(x+0.2, df['Book Value'], width=0.4, label='Book Value')

        ax.set_ylabel('Price')

        ax.set_title('Actual Price vs. Book Value')

        plt.legend()
        plt.yscale('log')

        formatter = ScalarFormatter()
        formatter.set_scientific(False)
        ax.yaxis.set_major_formatter(formatter)
            
        plt.savefig('/home/elieba/airflow/files/companies_chart.png', dpi=300)
    

    draw_task = PythonOperator(
        task_id = 'draw_task',
        python_callable = draw
    )

    # Set task dependencies (task1 runs before task2)
    fetch_task >> sensor_task >> parse_compnies >> draw_task  # task1 must run successfully before task2 starts
