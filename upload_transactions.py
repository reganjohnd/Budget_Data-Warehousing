import pandas as pd
import pyodbc as odbc

from dotenv import load_dotenv
from pathlib import Path
import os
env_path = Path.home()/'.env'
load_dotenv(dotenv_path= env_path, override=True)
DIR = os.getenv('budget_dir')

import helper_functions as hf

def upload_transactions(data_file_dir:str):
    con, cur = hf.create_database_connection('budget')

    transactions = pd.read_csv(hf.create_dir(data_file_dir, 'transactions.csv'))
    categories = hf.get_data('select * from categories_DIM', cur)
    spending_groups = hf.get_data('select * from spending_groups_DIM', cur)
    accounts = hf.get_data('select * from accounts_DIM', cur)

    ### update dimension tables
    

    dimension_update_variables = {'categories':{'trxs_column_name': 'Category',
                                                'db_column_name': 'category_name',
                                                'db_tbl_name': 'categories_DIM'},
                                'spending_groups': {'trxs_column_name': 'Spending Group',
                                            'db_column_name': 'spending_group_name',
                                            'db_tbl_name': 'spending_groups_DIM'},
                                'accounts': {'trxs_column_name': 'Account',
                                            'db_column_name': 'account_detail',
                                            'db_tbl_name': 'accounts_DIM'}}

    dimension_data = [categories, spending_groups, accounts]

    # for the respective dimension
    for i, v in enumerate(dimension_data):
        # unique list of categories from the new transactions
        trxs_values = list(transactions[list(dimension_update_variables.values())[i].get('trxs_column_name')].unique())

        # unique list of categories from the DB dimension table
        db_values = list(v[list(dimension_update_variables.values())[i].get('db_column_name')])

        # extract categories that are in the new transactions but not in the DB dimension table
        values_to_add = [x for x in trxs_values if x not in db_values]
        
        if len(values_to_add) > 0:
            for j, k in enumerate(values_to_add):
                query = f'''
                    INSERT INTO [dbo].[{list(dimension_update_variables.values())[i].get('db_tbl_name')}] ([{list(dimension_update_variables.values())[i].get('db_column_name')}]) VALUES ('{k}');
                    '''
                print(query)
                cur.execute(query)
                cur.commit()
                con.commit()

    ### update transactions table
    categories = hf.get_data('select * from categories_DIM', cur)
    spending_groups = hf.get_data('select * from spending_groups_DIM', cur)
    accounts = hf.get_data('select * from accounts_DIM', cur)

    spending_group_mapping = dict(spending_groups.iloc[:, [1, 0]].values)
    category_mapping = dict(categories.iloc[:, [1, 0]].values)
    account_mapping = dict(accounts.iloc[:, [2, 0]].values)

    transactions['spending_group_key'] = transactions['Spending Group'].map(spending_group_mapping)
    transactions['category_key'] = transactions['Category'].map(category_mapping)
    transactions['account_key'] = transactions['Account'].map(account_mapping)

    transactions['date_key'] = [x.replace('/', '') for x in transactions['Date']]
    
    transactions.rename(columns={'Amount':'amount', 'Description':'description'}, inplace=True)
    transactions = transactions[['date_key', 'category_key', 'spending_group_key', 'account_key', 'description', 'amount']].dropna()

    for i, v in transactions.iterrows():
        cur.execute('INSERT INTO [dbo].[transactions_Fact] ([date_key], [category_key], [spending_group_key], [account_key], [description], [amount]) VALUES(?, ?, ?, ?, ?, ?)', v.date_key, v.category_key, v.spending_group_key, v.account_key, v.description, v.amount)
        cur.commit()
        con.commit()

    cur.close()
    con.close()

upload_transactions(DIR)