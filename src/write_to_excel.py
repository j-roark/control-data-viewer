'''

Version : Python 3.7
Dependencies : MySQLdb
Written by : John Roark 
Purpose : To write pandas dataframe objects to an excel spreadsheet

'''

import pandas as pd
import random
from datetime import date

def write_to_excel(final_data):
 
    today = date.today()
    filename = f'E:/Backup/{random.randint(1000, 9999)}_{today.strftime("%b-%d-%Y")}_package_stats_by_order_number.xlsx'
    
    # Filename and directory to write the .xlsx file too.
    
    with pd.ExcelWriter(filename) as writer:
        final_data.to_excel(writer, sheet_name="Pack. Stats. by Order Num", index=False)

    print(f'Saved as: {filename}')
    return
