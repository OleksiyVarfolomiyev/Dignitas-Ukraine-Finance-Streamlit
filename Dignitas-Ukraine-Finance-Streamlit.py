import streamlit as st; st.set_page_config(layout="wide")

import ETL as etl
import data_aggregation_tools as da
import charting_tools

import pandas as pd
import datetime as dt

import plotly.express as px
from plotly.offline import iplot
import plotly.figure_factory as ff
import plotly.io as pio
from plotly.subplots import make_subplots

@st.cache_data(ttl=24*60*60)
def data_prep():
    """ prep data before running the app"""
    df, df_inkind, df_inv, ds = etl.read_txs_from_csv()
    start_date = df['Date'].min()
    end_date = df['Date'].max()
    #end_date = dt.date.today()
    #start_date = dt.date(2023, 2, 15)
    #end_date = dt.date(2023, 10, 24)
    #end_date = dt.date.today()
    #end_date = dt.date.today() - dt.timedelta(days=1)
    etl.extract_relevant_txs(df, ds, start_date, end_date)

#data_prep()

# app code
large_donations_by_category, large_spending_by_category, donations_below_large_by_category, spending_below_large_by_category, \
donations_total, spending_total, donations_total_by_category, spending_total_by_category = etl.read_txs()

st.title("Dignitas Ukraine **Financials**")

def show_metrics(donations_total, spending_total):
    """ Show metrics"""
    #end_date = df['Date'].max()
    starting_date = donations_total['Date'].min().date()
    #starting_date = dt.date(2023, 2, 15)
    end_date = dt.date.today()
    #dt.date(2023, 10, 31)

    #donations_today = etl.format_money(donations_total[donations_total['Date'] == donations_total['Date'].max()]['UAH'].iloc[0])
    spending_latest = etl.format_money(spending_total[spending_total['Date'] == spending_total['Date'].max()]['UAH'].iloc[0])
    yesterday = donations_total['Date'].max()# - pd.Timedelta(days=1)
    donations_yesterday = etl.format_money(donations_total[donations_total['Date'] == yesterday]['UAH'].iloc[0])

    col1, col2, col3 = st.columns(3)
    col1.metric("Days", (end_date - starting_date).days, "1", delta_color="normal")
    col2.metric("Donations", etl.format_money(donations_total.UAH.sum()), donations_yesterday, delta_color="normal")
    col3.metric("Spending",  etl.format_money(spending_total.UAH.sum()),  spending_latest, delta_color="normal")

show_metrics(donations_total, spending_total)

def show_donations_spending(spending_total, donations_total):
    """ Show donations and spending by time period"""
    col0, col1, col2 = st.columns(3)
    with col1:
        timeperiod = st.selectbox(' ', ['Monthly ', 'Weekly ', 'Daily ', 'Yearly '])

    spending = da.sum_by_period(spending_total, timeperiod[0])
    donations = da.sum_by_period(donations_total, timeperiod[0])

    if timeperiod == 'Monthly':
        donations.index = donations.index.strftime("%Y-%m-%d")
        spending.index = spending.index.strftime("%Y-%m-%d")
        donations.index = pd.date_range(start=donations.index[0], periods=len(donations), freq='M')
        spending.index = pd.date_range(start=spending.index[0], periods=len(spending), freq='M')

    elif timeperiod == 'Yearly':
        donations.index = donations.index.strftime("%Y")
        spending.index = spending.index.strftime("%Y")
    else:
        donations.index = donations.index.strftime("%Y-%m-%d")
        spending.index = spending.index.strftime("%Y-%m-%d")


    donations_and_spending = pd.merge(donations, spending, left_index=True, right_index=True, how = 'left')
    donations_and_spending.columns = ['Donations', 'Spending']
    #donations_and_spending.index = donations_and_spending.index.start_time

    fig = charting_tools.bar_plot_grouped(donations_and_spending, 'Donations', 'Spending', '', False)
    st.plotly_chart(fig, use_container_width=True)

show_donations_spending(spending_total, donations_total)

# Ring plot - Donations and Spending by Category
def show_donations_spending_by_category(large_donations_by_category, large_spending_by_category,
                                        donations_below_large_by_category, spending_below_large_by_category,
                                        donations_total_by_category, spending_total_by_category):
    """ Show donations and spending by category"""
    donations_by_category = donations_total_by_category
    spending_by_category = spending_total_by_category

    col0, col1, col2, col3 = st.columns(4)
    with col1:
        over_below_all = st.selectbox(' ',['all txs', 'over 10K', 'below 10K'])
    with col2:
        period = st.selectbox(' ', ['Month', 'Week', 'Day', 'Year'])

    if period == 'Month':
        donations = donations_by_category[donations_by_category['Date'] >= pd.Timestamp.now().floor('D') - pd.DateOffset(months=1)]
        spending =  spending_by_category[spending_by_category['Date'] >= pd.Timestamp.now().floor('D') - pd.DateOffset(months=1)]

    elif period == 'Week':
        donations = donations_by_category[donations_by_category['Date'] >= pd.Timestamp.now().floor('D') - pd.DateOffset(weeks=1)]
        spending =  spending_by_category[spending_by_category['Date'] >= pd.Timestamp.now().floor('D') - pd.DateOffset(weeks=1)]
    elif period == 'Day':
        day = donations_by_category['Date'].max()
        donations = donations_by_category[donations_by_category['Date'] == donations_by_category.Date.max()]
        spending =  spending_by_category[spending_by_category['Date'] == spending_by_category.Date.max()]
    elif period == 'Year':
        donations = donations_by_category[donations_by_category['Date'] >= pd.Timestamp.now().floor('D') - pd.DateOffset(years=1)]
        spending =  spending_by_category[spending_by_category['Date'] >= pd.Timestamp.now().floor('D') - pd.DateOffset(years=1)]
    else:
        donations = donations_by_category
        spending = spending_by_category

    if over_below_all == 'over 10K':
        donations = donations[donations['UAH'] >= 10000]
        spending = spending[spending['UAH'] >= 10000]
    elif over_below_all == 'below 10K':
        donations = donations[donations['UAH'] < 10000]
        spending = spending[spending['UAH'] < 10000]
    # else:
    #     donations = donations_total_by_category
    #     spending = spending_total_by_category

    donations_by_cat = pd.DataFrame(donations.groupby('Category')['UAH'].sum())
    spending_by_cat =  pd.DataFrame(spending.groupby('Category')['UAH'].sum())

    fig1 = charting_tools.pie_plot(donations_by_cat, 'UAH', 'Donations by category', False)
    fig2 = charting_tools.pie_plot(spending_by_cat, 'UAH', "Spending by Category", False)
    fig = charting_tools.subplot_horizontal(fig1, fig2, 1, 2, 'domain', 'domain', 'Donations by Category', 'Spending by Category', False)
    st.plotly_chart(fig, use_container_width=True)

show_donations_spending_by_category(large_donations_by_category, large_spending_by_category,
                                    donations_below_large_by_category, spending_below_large_by_category,
                                    donations_total_by_category, spending_total_by_category)

def donations_spending_by_period_by_category(donations_total_by_category, spending_total_by_category,
                                            large_donations_by_category, large_spending_by_category,
                                            donations_below_large_by_category, spending_below_large_by_category):
    """Donations/Spending by time period (d, w, m) and large/regular amounts"""
    #main_donation_categories = donations_total_by_category.groupby('Category')['UAH'].sum().sort_values(ascending = False).index[:4].tolist()
    main_donation_categories = donations_total_by_category.groupby('Category')['UAH'].sum().sort_values(ascending=False).index.tolist()
    #main_spending_categories = spending_total_by_category.groupby('Category')['UAH'].sum().sort_values(ascending = False).index[:4].tolist()
    main_spending_categories = spending_total_by_category.groupby('Category')['UAH'].sum().sort_values(ascending = False).index.tolist()
    col0, col1, col2 = st.columns(3)
    with col0:
        amount = st.selectbox(' ',[' all ', '>10K', '<10K'])
    with col1:
        selected_period = st.selectbox(' ',['monthly', 'weekly', 'daily', 'yearly'])
    with col2:
        donations_spending = st.selectbox(' ', ['donations', 'spending'])

    if amount == '>10K':
        donations_by_category = large_donations_by_category
        spending_by_category = large_spending_by_category
    elif amount == '<10K':
        donations_by_category = donations_below_large_by_category
        spending_by_category = spending_below_large_by_category
    else:
        donations_by_category = donations_total_by_category
        spending_by_category = spending_total_by_category

    if donations_spending == 'donations':
        main_categories = main_donation_categories
        tx_by_category = donations_by_category
    else:
        main_categories = main_spending_categories
        tx_by_category = spending_by_category

    fig = charting_tools.chart_by_period(tx_by_category, main_categories, selected_period[0],
                                        f'{selected_period} {amount} {donations_spending}')
    st.plotly_chart(fig, use_container_width=True)

donations_spending_by_period_by_category(donations_total_by_category, spending_total_by_category,
                                                                        large_donations_by_category, large_spending_by_category,
                                                                        donations_below_large_by_category, spending_below_large_by_category)

#st.write(spending_below_large_by_category)

st.markdown("Visit [Dignitas Fund](https://dignitas.fund/)")
