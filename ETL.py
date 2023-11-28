import pandas as pd
#import import_ipynb
import data_aggregation_tools as da

def format_money(value):
    if abs(value) >= 1e6:
        return '{:.2f}M'.format(value / 1e6)
    elif abs(value) >= 1e3:
        return '{:.2f}K'.format(value / 1e3)
    else:
        return '{:.2f}'.format(value)


def read_txs():
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


def read_txs_from_csv(nrows = None):
    '''Extract and transform data from csv file'''
    if nrows == None:
            #df = pd.read_csv('./data/DIGNITAS UKRAINE INC_Transaction Drilldown Report.csv',
            df = pd.read_csv('./data/DIGNITAS UKRAINE INC_Transactions for Dashboard.csv',
                         index_col=None,
                         usecols = ['Account name', 'Date', 'Transaction type', 'Name', 'Amount line'],
                         dtype={ 'Transaction type': 'category', 'Name': 'string', 'Account name': 'string'}, parse_dates=['Date'])
    else:
            return None

    df['Amount line'] = df['Amount line'].replace({'\$': '', ',': ''}, regex=True).astype(float)
    df = df[df['Transaction type'] != 'Transfer']
    df = df[df['Account name'] != 'FIVE % FOR ADMIN EXPENSES']

    # in-kind donations
    df = df[df['Account name'] != 'IN-KIND DONATION DISTRIBUTIONS']
    df_inkind = df[df['Account name'] == 'In-kind donations']
    df = df[df['Account name'] != 'In-kind donations']

    # investments
    df_inv = df[df['Account name'] == 'DIVIDENT RECEIVED FIDELITY']

    df = df[df['Account name'] != 'DIVIDENT RECEIVED FIDELITY']
    df = df[df['Account name'] != 'FIDELITY INVESTMENTS']
    df = df[df['Account name'] != 'In-kind donations']

    # spending
    ds = df[df['Transaction type'] != 'Deposit']
    ds = ds[ds['Transaction type'] != 'Journal Entry']
    ds = ds[ds['Account name'] != 'Donations directed by individuals']
    ds = ds[ds['Account name'] != 'CHASE ENDING IN 5315']
    ds = ds[ds['Account name'] != 'PayPal Bank 3']
    ds = ds[ds['Account name'] != 'DIVIDENT RECEIVED FIDELITY']

    # donations
    df = df[df['Account name'] == 'Donations directed by individuals']
    df = df[df['Transaction type'] == 'Deposit']

    # Define the mapping of old column names to new column names
    column_mapping = {'Name': 'Category', 'Amount line': 'UAH'}
    # Use the mapping to rename the columns
    df.rename(columns=column_mapping, inplace=True)
    df_inkind.rename(columns=column_mapping, inplace=True)

    column_mapping = {'Account name': 'Category', 'Amount line': 'UAH'}
    ds.rename(columns=column_mapping, inplace=True)
    return df, df_inkind, df_inv, ds


def replace_category_values(data, category_column, val1, val2):
    """Mapping of Category values"""
    df_copy = data.copy()
    df_copy[category_column] = df_copy[category_column].replace(val1, val2)
    return df_copy

def extract_relevant_txs(df, ds, start_date, end_date):
    """Sum txs by date and save to csv files"""
    if (start_date != None) | (end_date != None):
        df = df[df['Date'] >= start_date]
        ds = ds[ds['Date'] >= start_date]
        df = df[df['Date'] <= end_date]
        ds = ds[ds['Date'] <= end_date]

    value_mapping = {'02 GENERAL BANK URESTR': 'General donations', 'PAY PALL UNRESTRICTED': 'General donations',
                '03 FUNDRASING EVENTS UNRESTR': 'General donations', '04 PAYPAL GENERAL UNRESTR': 'General donations',
                '05 VENMO UNRESTR': 'General donations', '06 SQUARE UNRESTR': 'General donations',
                '07 1000 DRONES RESTR': '1000 Drones for Ukraine', '09 MSU RESTR': 'Mobile Shower Units',
                '11 VETERANIUS RESTR' : 'Veteranius', '08 VICTORY DRONES RESTR': 'Victory Drones', '10 Flight to Recovery RESTR': 'Flight to Recovery'}
    df['Category'] = df['Category'].replace(value_mapping)

    ds['UAH'] = ds['UAH'].abs()

    column_mapping = {'Name': 'Category', 'Amount line': 'UAH'}
    df.rename(columns=column_mapping, inplace=True)
    df = df[['Date', 'Category', 'UAH']]
    column_mapping = {'Account name': 'Category', 'Amount line': 'UAH'}
    ds.rename(columns=column_mapping, inplace=True)

    ds = ds[['Date', 'Category', 'UAH']]
    mask = ds['Category'].str.contains('software', case=False, na=False)
    ds.loc[mask, 'Category'] = 'Admin'
    mask = ds['Category'].str.contains('transportation', case=False, na=False)
    ds.loc[mask, 'Category'] = 'Admin'
    mask = ds['Category'].str.contains('events', case=False, na=False)
    ds.loc[mask, 'Category'] = 'Admin'
    mask = ds['Category'].str.contains('lodging', case=False, na=False)
    ds.loc[mask, 'Category'] = 'Admin'
    mask = ds['Category'].str.contains('car purchases', case=False, na=False)
    ds.loc[mask, 'Category'] = 'Admin'
    mask = ds['Category'].str.contains('Legal fees', case=False, na=False)
    ds.loc[mask, 'Category'] = 'Admin'
    mask = ds['Category'].str.contains('delivery', case=False, na=False)
    ds.loc[mask, 'Category'] = 'Admin'
    mask = ds['Category'].str.contains('bank fees', case=False, na=False)
    ds.loc[mask, 'Category'] = 'Admin'
    mask = ds['Category'].str.contains('supplies', case=False, na=False)
    ds.loc[mask, 'Category'] = 'Admin'

    #ds['Category'] = ds['Category'].str.replace('software & apps', 'Admin')
    #ds['Category'] = ds['Category'].str.replace('events participation expenses', 'Admin')
    # ds['Category'] = ds['Category'].str.replace('transportation and parking', 'Admin')
    # ds['Category'] = ds['Category'].str.replace('lodging', 'Admin')
    # ds['Category'] = ds['Category'].str.replace('car purchases', 'Admin')
    # ds['Category'] = ds['Category'].str.lower()

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