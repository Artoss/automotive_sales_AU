# -*- coding: utf-8 -*-
"""
Created on Sat May 20 22:45:33 2023

Car sales data from www.marklines.com

Australia - automotive sales volumes by month and make


@author: Artoss
"""

import pandas as pd


# Latest data - monthly data for current year and previous two years

df2 = pd.read_html('https://www.marklines.com/en/statistics/flash_sales/automotive-sales-in-australia-by-month')



# Historical data by month from 2018 onwards (older data may be available)

url = 'https://www.marklines.com/en/statistics/flash_sales/salesfig_australia_2019#aug'

df = pd.read_html(url)

df_data = pd.DataFrame()

for table in range(1, 22, 2):
    print(table)
    df_data = pd.concat([df_data, df[table]], axis=1)


