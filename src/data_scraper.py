#%%
from bs4 import BeautifulSoup
import requests
import pandas as pd
import csv
import sys
from tqdm import tqdm
sys.setrecursionlimit(100000)
#%%
url = 'https://jobs.undp.org/cj_view_job.cfm?cur_job_id='
#Generate list of decreasing numbers to artificially generate the URLS:
numbers = list(range(106779, 1150, -1))
numbers_df = pd.DataFrame(numbers, columns = ['numbers'])
numbers_df['numbers'] = numbers_df['numbers'].astype(str)
#Append numbers to url
numbers_df['url'] = url + numbers_df['numbers'].astype(str)
url = numbers_df.url.tolist()
#Check if it is right

def get_soup(url):
    response = requests.get(url, timeout=1)
    soup = BeautifulSoup(response.content, "html.parser")

    return soup

def get_content(url):
  soup = get_soup(url)
  try:
    content = []
    for i in soup.find_all('tr'):
      content.append(i.getText().lower())  
  except: 
    content = 'NaN'
  
  return content
#%%
d = {}
for i, r in enumerate(tqdm(url)):
    if i%20000 == 0:
        with open('undp_jobs.csv', 'w') as f:  # You will need 'wb' mode in Python 2.x
            w = csv.DictWriter(f, d.keys())
            w.writeheader()
            w.writerow(d)
    try:
        content = get_content(r)
        d[i] = {}
        d[i]['content'] =  content
    except: 
        continue
# %%
