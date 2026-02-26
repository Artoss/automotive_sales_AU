# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 16:06:24 2019

Get files from FCAI website on car sales across Australia
www.fcai.com.au

Metadata: https://www.fcai.com.au/sales/segmentation-criteria

@author: stuart.mcknight
"""

import requests

years = ['2017', '2018', '2019']
months = ['january', 'february', 'march',
          'april', 'may', 'june',
          'july', 'august', 'september',
          'october', 'november', 'december']

publication = "july_2019_vfacts_media_release_and_industry_summary.pdf"
url = "https://www.fcai.com.au/library/publication/"

r = requests.get(url+publication, allow_redirects=True)

open("Outputs/" + publication, 'wb').write(r.content)


