# The etl pipeline.
# extract the csvs as pandas dataframes from their folder
# transform them into the values we are analysing
# load them into csv files representing the tables they will be put into
# save these completed files into a folder
# run the azure_load.py script to load them

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
from pandas import merge_ordered, merge
import numpy as np
from datetime import datetime,date as dt

from sql_load import run_sql_load

pd.options.mode.chained_assignment = None

stock_data = (
    pd.read_csv("./csv_raw/stocks.csv")
    .assign(date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    #.sort_values(by="Date")
)

econ_data = (
    pd.read_csv("./csv_raw/econ.csv")
    .assign(date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    #.sort_values(by="date")
)

#--------------------
# fact_stock_analysis
#--------------------
def find_growth():
    share_price = pd.DataFrame({
        'stock_name': stock_data['stock_name'],
        'share_price': stock_data['share_price'], 
        'date': stock_data['date']
    })
    share_price["fact_stock_growth"] = 0
    temp = share_price

    for i in share_price.index:
        if i == 0:
            x = 1
        else:
            current = share_price["share_price"].iloc[i]
            last = share_price["share_price"].iloc[i-1]
            diff = current - last
            growth = (diff/last) * 100
            temp["fact_stock_growth"].iloc[i] = round(growth, 3)

    return temp

def find_dividend_price_ratio():
    temp = pd.DataFrame({
        'stock_name': stock_data['stock_name'],
        'share_price': stock_data['share_price'],
        'dividend':stock_data['dividend'],
        'date': stock_data['date']
    })
    temp['fact_stock_dividend_price_ratio'] = 0

    for i in temp.index:
            price = temp['share_price'].iloc[i]
            div = temp['dividend'].iloc[i]
            ratio = div/price
            temp['fact_stock_dividend_price_ratio'].iloc[i] = round(ratio, 3)

    return temp

def find_market_cap():
    temp = pd.DataFrame({
        'stock_name': stock_data['stock_name'],
        'share_price': stock_data['share_price'],
        'share_quantity':stock_data['share_quantity'],
        'date': stock_data['date']
    })
    temp['fact_stock_market_cap'] = 0

    for i in temp.index:
            price = temp['share_price'].iloc[i]
            shares = temp['share_quantity'].iloc[i]
            market_cap = price * shares
            temp['fact_stock_market_cap'].iloc[i] = market_cap

    return temp

def find_earnings_per_share():
    temp = pd.DataFrame({
        'stock_name': stock_data['stock_name'],
        'earnings': stock_data['earnings'],
        'share_quantity':stock_data['share_quantity'],
        'date': stock_data['date']
    })

    temp['fact_stock_earnings_per_share'] = 0

    for i in temp.index:
            earnings = temp['earnings'].iloc[i]
            shares = temp['share_quantity'].iloc[i]
            eps = earnings/shares
            temp['fact_stock_earnings_per_share'].iloc[i] = eps

    return temp

def find_earnings_price_ratio():
    #finding = find_growth()
    temp = pd.DataFrame({
        'stock_name': stock_data['stock_name'],
        'earnings': stock_data['earnings'],
        'share_price':stock_data['share_price'],
        'date': stock_data['date']
    })
    temp['fact_stock_earnings_price_ratio'] = 0

    for i in temp.index:
        if i == 0:
            x = 1
        else:
            earnings = temp['earnings'].iloc[i]
            share_price = temp['share_price'].iloc[i]
            ratio = earnings/share_price
            temp['fact_stock_earnings_price_ratio'].iloc[i] = round(ratio, 3)

    return temp

def get_fact_stock():
    temp = find_growth()
    temp = temp.sort_values(by=['stock_name', 'date'])
    temp = temp.drop(columns=['share_price'])
    b = find_dividend_price_ratio()
    b = b.sort_values(by=['stock_name', 'date'])
    b = b.drop(columns=['share_price', 'dividend', 'stock_name'])
    temp = merge(temp, b, on='date')
    temp = temp.sort_values(by=['stock_name', 'date'])

    b = find_market_cap()
    b = b.sort_values(by=['stock_name', 'date'])
    b = b.drop(columns=['share_price', 'share_quantity', 'stock_name'])
    temp = merge(temp, b, on='date')
    temp = temp.sort_values(by=['stock_name', 'date'])

    b = find_earnings_per_share()
    b = b.sort_values(by=['stock_name', 'date'])
    b = b.drop(columns=['earnings', 'share_quantity', 'stock_name'])
    temp = merge(temp, b, on='date',)
    temp = temp.sort_values(by=['stock_name', 'date'])

    b = find_earnings_price_ratio()
    b = b.sort_values(by=['stock_name', 'date'])
    b = b.drop(columns=['earnings', 'share_price', 'stock_name'])
    temp = merge(temp, b, on='date')
    temp = temp.sort_values(by=['stock_name', 'date'])

    temp = temp.drop_duplicates(subset=['stock_name', 'date'])

    temp = temp.reset_index(drop=True)

    return temp




#FINDING THE VALUES

##fact_stock_growth = find_growth()
#drop share_price

#fact_stock_dividend_price_ratio = find_dividend_price_ratio()
#drop share_price & dividend

#fact_stock_market_cap = find_market_cap()
#drop share_price & share_quantity

#fact_stock_earnings_per_share = find_earnings_per_share()
#drop earnings & share_quantity

#fact_stock_earnings_growth_ratio = find_earnings_growth_ratio()
#drop earnings & growth

#print(fact_stock_earnings_growth_ratio)

#----------------
# fact_comparison
#----------------

def find_correlation(name, country, x, y):

    temp = econ_data[econ_data['country_name'] == country]
    temp['date'] = pd.to_datetime(temp['date']).dt.normalize()
    #temp['date'] = pd.to_datetime(temp['date'])
    #temp['date'] = dt.normalize(temp['date'])

    a = stock_data[stock_data['stock_name'] == name]

    a = a.drop(columns=['share_quantity', 'dividend', 'earnings'])

    a['date'] = pd.to_datetime(a['date']).dt.normalize()

    temp = merge(temp, a, on='date')

    c = temp.corr()

    d = c[x][y]

    #c.to_csv('./csv_processed/c.csv', index=False,date_format="%d/%m/%Y")

    return d

def get_fact_corr():

    fact = pd.DataFrame(columns=[
        'country_name',
        'stock_name',
        'fact_stock_price_country_gdp_growth_coefficient',
        'fact_stock_price_country_inflation_coefficient',
        'fact_stock_price_country_unemployment_coefficient',
        'fact_stock_price_country_debt_gdp_coefficient',
        'fact_stock_price_country_household_savings_coefficient'
    ])

    #i = 1
    for x in stock_data['stock_name'].unique():
        for y in econ_data['country_name'].unique():
            a = find_correlation(x, y, 'share_price', 'quarterly_GDP_growth')
            b = find_correlation(x, y, 'share_price', 'quarterly_inflation')
            c = find_correlation(x, y, 'share_price', 'quarterly_unemployment')
            d = find_correlation(x, y, 'share_price', 'quarterly_debt_GDP')
            e = find_correlation(x, y, 'share_price', 'household_saving_ratio')
            
            fact = fact.append({
                'country_name': y,
                'stock_name': x,
                'fact_stock_price_country_gdp_growth_coefficient': a,
                'fact_stock_price_country_inflation_coefficient': b,
                'fact_stock_price_country_unemployment_coefficient': c,
                'fact_stock_price_country_debt_gdp_coefficient': d,
                'fact_stock_price_country_household_savings_coefficient': e
                }, ignore_index=True)  




    return fact


#FINDING THE VALUES & SAVING
fact_comparison = get_fact_corr()
fact_comparison.to_csv('./csv_processed/fact_comparison.csv', index=False)

fact_stock_analysis = get_fact_stock()
fact_stock_analysis.to_csv('./csv_processed/fact_stock_analysis.csv', date_format="%d/%m/%Y %H:%M:%S", index=False)

stock_data.to_csv('./csv_processed/dim_stock.csv', date_format="%d/%m/%Y %H:%M:%S", index=False)
econ_data.to_csv('./csv_processed/dim_econ.csv', date_format="%d/%m/%Y %H:%M:%S", index=False)

#Loading into Azure & running azure_load.py

#f.to_csv('./csv_processed/out.csv')
x = run_sql_load()
print(x)