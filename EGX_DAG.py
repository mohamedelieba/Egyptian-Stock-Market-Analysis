from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task, dag
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

@task
def fetch():
        resp = requests.get('https://www.tradingview.com/markets/stocks-egypt/market-movers-large-cap/')
        soup = BeautifulSoup(resp.text)
        symbols = []
        for i, tag in enumerate(soup.findAll('a', attrs={'class': re.compile('^apply-common-tooltip')})): 
            if(i%2==0 and tag.string != 'EGS923M1C017'):
                 symbols.append(tag.string)
        return symbols



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

@task
def parse(symbols: list):

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

        return df.to_json()

@task
def draw(companies_json: str):

        df = pd.read_json(companies_json)
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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 9), 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),  
}

@dag(dag_id='EGX_DAG',
     default_args=default_args,
     description='EGX Analysis Pipeline',
     schedule=timedelta(days=1),
     catchup=False)
def taskflow_api_dag():
    symbols = fetch()
    companies_json = parse(symbols)
    draw(companies_json)

dag = taskflow_api_dag()