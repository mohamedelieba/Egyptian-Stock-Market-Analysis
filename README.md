# Egyptian-Stock-Market-Analysis

This code is a Python script that scrapes financial data from the Mubasher website for the Egyptian stock market. The script compares the book value of each company to its last closing price and extracts a list of companies with a P/B value less than 1, which usually indicates good investment opportunities.

### Data
The data used in this notebook is collected from the Mubasher website.a financial news website that provides real-time market data and analysis for the MENA region
### Code Description
The requests and BeautifulSoup libraries are used to send HTTP requests to Mubasher and parse the HTML content. The script iterates over a list of stock symbols, sends a GET request for each symbol, and extracts the required financial data.

The following financial data is extracted for each stock symbol:

* Book value: The total value of a company's assets minus its liabilities, which represents the value that shareholders would theoretically receive if the company were to liquidate all of its assets and pay off all of its debts.
* EPS: Earnings per share, which represents a company's profit divided by the number of outstanding shares.
* Last price: The closing price of the stock on the day the script was run.
* Company name: The name of the company.
* Quarter: The quarter for which the book value and EPS were reported.
The extracted data is stored in lists, and only companies with a P/B value less than 1 are added to the final list. The final list includes the company symbol, name, book value, EPS, last price, and P/B ratio.

### Requirements
* Python 3.x
* requests library
* BeautifulSoup library
* pandas 
* numpy
* matplotlib
* seaborn
### Usage
To use the script, simply run it on as a Jupyter notebook on your browser.

### Contributing
Contributions to this script are welcome. If you find any issues or have suggestions for improvements, feel free to open a GitHub issue or submit a pull request.
