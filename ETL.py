import pandas as pd
#import import_ipynb
import data_aggregation_tools as da
import streamlit as st

def format_money(value):
    if abs(value) >= 1e6:
        return '{:.2f}M'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '{:.2f}K'.format(value / 1e3)
    else:
        return '{:.2f}'.format(value)


def read_clean_data():
    """read clean data from csv files"""
    dtypes = { 'UAH': 'float64', 'Category': 'str' }

    large_donations_by_category = pd.read_csv('data/large_donations_by_category.csv', dtype=dtypes, parse_dates=['Date'])
    large_spending_by_category = pd.read_csv('data/large_spending_by_category.csv', dtype=dtypes, parse_dates=['Date'])

    donations_below_large_by_category = pd.read_csv('data/donations_below_large_by_category.csv', dtype=dtypes, parse_dates=['Date'])
    spending_below_large_by_category = pd.read_csv('data/spending_below_large_by_category.csv', dtype=dtypes, parse_dates=['Date'])

    donations_total = pd.read_csv('data/donations_total.csv', dtype=dtypes, parse_dates=['Date'])
    spending_total =  pd.read_csv('data/spending_total.csv', dtype=dtypes, parse_dates=['Date'])

    donations_total_by_category = pd.read_csv('data/donations_total_by_category.csv', dtype=dtypes, parse_dates=['Date'])
    spending_total_by_category = pd.read_csv('data/spending_total_by_category.csv', dtype=dtypes, parse_dates=['Date'])

    donations_total_by_category = pd.read_csv('data/donations_total_by_category.csv', dtype=dtypes, parse_dates=['Date'])
    spending_total_by_category = pd.read_csv('data/spending_total_by_category.csv', dtype=dtypes, parse_dates=['Date'])

    return large_donations_by_category, large_spending_by_category, donations_below_large_by_category, spending_below_large_by_category, \
            donations_total, spending_total, donations_total_by_category, spending_total_by_category

@st.cache_data(ttl=24*60*60)
def ETL_raw_data(nrows = None):
    '''Extract and transform data and save it to csv files'''
    if nrows == None:
            df = pd.read_csv('./data/DIGNITAS UKRAINE INC_Transactions for Dashboard.csv',
                        index_col=None,
                        usecols = ['Account', 'Date', 'Transaction type', 'Name', 'Amount'],
                        dtype={ 'Transaction type': 'category', 'Name': 'string', 'Account': 'string'}, parse_dates=['Date'])
    else:
        return None

    df['Amount'] = df['Amount'].replace({'\$': '', ',': '', '\(': '', '\)': ''}, regex=True).astype(float)
    df = df[df['Transaction type'] != 'Transfer']
    df = df[df['Account'] != 'FIVE % FOR ADMIN EXPENSES']

    # in-kind donations
    #df = df[df['Account name'] != 'IN-KIND DONATION DISTRIBUTIONS']
    #df_inkind = df[df['Account name'] == 'In-kind donations']
    #df = df[df['Account name'] != 'In-kind donations']

    # investments
    #df_inv = df[df['Account name'] == 'DIVIDENT RECEIVED FIDELITY']

    #df = df[df['Account name'] != 'DIVIDENT RECEIVED FIDELITY']
    #df = df[df['Account name'] != 'FIDELITY INVESTMENTS']

    # spending
    ds = df[df['Transaction type'] != 'Deposit']
    ds = ds[ds['Transaction type'] != 'Journal Entry']
    ds = ds[ds['Account'] != 'Donations directed by individuals']
    ds = ds[ds['Account'] != 'CHASE ENDING IN 5315']
    ds = ds[ds['Account'] != 'PayPal Bank 3']
    ds = ds[ds['Account'] != 'DIVIDENT RECEIVED FIDELITY']

    # donations
    df = df[df['Account'] == 'Donations directed by individuals']
    df = df[df['Transaction type'] == 'Deposit']

    # Define the mapping of old column names to new column names
    column_mapping = {'Name': 'Category', 'Amount': 'UAH'}
    # Use the mapping to rename the columns
    df.rename(columns=column_mapping, inplace=True)
    #df_inkind.rename(columns=column_mapping, inplace=True)

    column_mapping = {'Account': 'Category', 'Amount': 'UAH'}
    ds.rename(columns=column_mapping, inplace=True)

    column_mapping = {'Name': 'Category', 'Amount line': 'UAH'}
    df.rename(columns=column_mapping, inplace=True)
    df = df[['Date', 'Category', 'UAH']]

    column_mapping = {'Account name': 'Category', 'Amount line': 'UAH'}
    ds.rename(columns=column_mapping, inplace=True)
    ds = ds[['Date', 'Category', 'UAH']]

    value_mapping = {'02 GENERAL BANK URESTR': 'General donations', 'PAY PALL UNRESTRICTED': 'General donations',
                '03 FUNDRASING EVENTS UNRESTR': 'General donations', '04 PAYPAL GENERAL UNRESTR': 'General donations',
                '05 VENMO UNRESTR': 'General donations', '06 SQUARE UNRESTR': 'General donations',
                '07 1000 DRONES RESTR': '1000 Drones for Ukraine', '09 MSU RESTR': 'Mobile Shower Units',
                '11 VETERANIUS RESTR' : 'Veteranius', '08 VICTORY DRONES RESTR': 'Victory Drones', '10 Flight to Recovery RESTR': 'Flight to Recovery'}
    df['Category'] = df['Category'].replace(value_mapping)
    df['Category'] = df['Category'].str.title()

    ds['Category'] = ds['Category'].str.title()
    value_mapping = {'Car Purchases': 'Admin', 'Lodging': 'Admin',
                'Transportation And Parking': 'Admin', 'Events Participation Expenses': 'Admin',
                'Legal Fees': 'Admin', 'Software & Apps': 'Admin',
                'Maling And Delivery': 'Admin', 'Bank Fees & Service Charges': 'Admin', 'Chase Bank': 'Admin',
                '01 Administrative Account': 'Admin', 'Quickbooks Payments': 'Admin',
                'Ngoptics' : 'Drone purchases', 'Collegiate Productions' : 'Admin', 'Supplies & Materials': 'Admin'}

    ds['Category'] = ds['Category'].replace(value_mapping)
    ds['UAH'] = ds['UAH'].abs()

    donations_total_by_category = df.groupby(['Date', 'Category']).sum().reset_index()
    donations_total_by_category.to_csv('data/donations_total_by_category.csv', index=False)

    spending_total_by_category = ds.groupby(['Date', 'Category']).sum().reset_index()
    spending_total_by_category.to_csv('data/spending_total_by_category.csv', index=False)

    donations_total = df.drop('Category', axis=1).groupby('Date').sum().reset_index()

    donations_total.to_csv('data/donations_total.csv', index=False)

    spending_total = ds.drop('Category', axis=1).groupby('Date').sum().reset_index()
    spending_total.to_csv('data/spending_total.csv', index=False)

    # above 10K
    amount = 10000
    large_donations = df[df['UAH'] >= amount].fillna('')
    large_donations_by_category = large_donations.groupby(['Date', 'Category']).sum().reset_index()
    large_donations_by_category.to_csv('data/large_donations_by_category.csv', index=False)

    large_spending = ds[ds['UAH'] >= amount].fillna('')
    large_spending_by_category = large_spending.groupby(['Date', 'Category']).sum().reset_index()
    large_spending_by_category.to_csv('data/large_spending_by_category.csv', index=False)

    # below 10K
    donations_below_large_by_category = df[df.UAH < amount]
    spending_below_large_by_category = ds[ds.UAH < amount]

    donations_below_large_by_category = donations_below_large_by_category.groupby(['Date', 'Category']).sum().reset_index()
    spending_below_large_by_category = spending_below_large_by_category.groupby(['Date', 'Category']).sum().reset_index()

    donations_below_large_by_category.to_csv('data/donations_below_large_by_category.csv', index=False)
    spending_below_large_by_category.to_csv('data/spending_below_large_by_category.csv', index=False)

    #return df, ds
