import pandas as pd
import pyodbc as odbc

import helper_functions as hf

def upload_transactions(data_file_dir:str):
    transactions = pd.read_csv(hf.create_dir(data_file_dir, 'transactions.csv'))
    categories = hf.get_data('select * from categories_DIM', 'budget')
    spending_groups = hf.get_data('select * from spending_groups_DIM', 'budget')
    accounts = hf.get_data('select * from accounts_DIM', 'budget')
    dates = hf.get_data('select * from Date_DIM', 'budget')

    spending_group_mapping = dict(spending_groups.iloc[:, [1, 0]].values)
    category_mapping = dict(categories.iloc[:, [1, 0]].values)
    account_mapping = dict(accounts.iloc[:, [2, 0]].values)
    transactions['spending_group_key'] = transactions['Spending Group'].map(spending_group_mapping)
    transactions['category_key'] = transactions['Category'].map(category_mapping)
    transactions['account_key'] = transactions['Account'].map(account_mapping)
    transactions['date_key'] = [x.replace('/', '') for x in transactions['Date']]
    transactions.rename(columns={'Amount':'amount', 'Description':'description'}, inplace=True)
    transactions = transactions[['date_key', 'category_key', 'spending_group_key', 'account_key', 'description', 'amount']].dropna()

    con, cur = hf.create_database_connection('budget')

    for i, v in transactions.iterrows():
        cur.execute('INSERT INTO [dbo].[transactions_Fact] ([date_key], [category_key], [spending_group_key], [account_key], [description], [amount]) VALUES(?, ?, ?, ?, ?, ?)', v.date_key, v.category_key, v.spending_group_key, v.account_key, v.description, v.amount)
    cur.commit()
    con.commit()
    cur.close()

upload_transactions(r'G:\My Drive\DANIELS ECOSYSTEM\extracurricularActivities\DataScience\Projects\budget_warehouse')