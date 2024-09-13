#%%
from bs4 import BeautifulSoup
import requests
import pandas as pd
import csv
import sys 
from tqdm import tqdm
sys.setrecursionlimit(100000)
import os 

#%%
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

def get_url_list(base_url = "https://jobs.undp.org/cj_view_job.cfm?cur_job_id=", from_last_id = True):

  if from_last_id == True:
    with open('../data/last_id.txt') as f:
       first_id = [ line.rstrip('\n') for line in f ]
  else:
    first_id = 1150

  base_url = base_url + first_id

  return base_url


def scrape(url="https://jobs.undp.org/cj_view_job.cfm?cur_job_id=", scrape_all = False):
  
  if scrape_all == False:
    with open('../data/last_id.txt') as f:
       first_id = [ line.rstrip('\n') for line in f ]
  else:
    first_id = 1150

  

  """
  define scraper
  - perhaps first function that only populates the list of URLs to scrape
  - second function that follows the old scraper (worked well)
  """
  return data 



