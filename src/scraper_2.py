import requests
from bs4 import BeautifulSoup
import csv
import logging
from tqdm import tqdm
import time
import os
import sys

sys.setrecursionlimit(100000)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_URL = 'https://jobs.undp.org/cj_view_job.cfm?cur_job_id='
CHECKPOINT_FILE = 'checkpoint.txt'
OUTPUT_FILE = 'undp_jobs.csv'
MIN_JOB_ID = 112000
MAX_RETRIES = 3
RETRY_DELAY = 5

def get_soup(url, retries=MAX_RETRIES):
    for _ in range(retries):
        try:
            response = requests.get(url, timeout=10)
            return BeautifulSoup(response.content, "html.parser")
        except requests.RequestException as e:
            logging.warning(f"Error fetching {url}: {e}. Retrying...")
            time.sleep(RETRY_DELAY)
    logging.error(f"Failed to fetch {url} after {MAX_RETRIES} retries")
    return None

def get_content(soup):
    if not soup:
        return 'NaN'
    try:
        content = []
        for i in soup.find_all('tr'):
            content.append(i.getText().lower())
        return content
    except:
        return 'NaN'

def get_checkpoint_data():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            data = f.read().strip().split(',')
            return int(data[0]), int(data[1])
    return MIN_JOB_ID - 1, MIN_JOB_ID - 1  # Start from MIN_JOB_ID if no checkpoint exists

def save_checkpoint(last_scraped_id, max_id):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(f"{last_scraped_id},{max_id}")

def is_valid_job_posting(url):
    try:
        response = requests.get(url, allow_redirects=True, timeout=10)
        final_url = response.url
        
        # Check if the final URL is not the careers page
        return "https://www.undp.org/careers" not in final_url
    except requests.RequestException:
        return False

def find_max_job_id(start_id=MIN_JOB_ID):
    current_id = start_id
    with tqdm(desc="Finding max job ID", unit="job") as pbar:
        while True:
            url = f"{BASE_URL}{current_id}"
            if is_valid_job_posting(url):
                current_id += 1
                pbar.update(1)
                print(url)
            else:
                break
    
    max_id = current_id - 1  # Subtract 1 to get the last valid ID
    logging.info(f"Max job ID found: {max_id}")
    return max_id

def scrape_jobs():
    last_scraped_id, prev_max_id = get_checkpoint_data()
    
    print("Finding the maximum job ID...")
    current_max_id = find_max_job_id(max(MIN_JOB_ID, last_scraped_id))
    
    if last_scraped_id < MIN_JOB_ID:
        min_id = MIN_JOB_ID
    else:
        min_id = last_scraped_id + 1

    max_id = current_max_id

    logging.info(f"Starting scrape from job ID: {max_id} to {min_id}")
    jobs_data = {}

    with tqdm(total=max_id-min_id+1, desc="Scraping jobs", unit="job") as pbar:
        for current_id in range(max_id, min_id - 1, -1):
            url = f"{BASE_URL}{current_id}"
            
            if is_valid_job_posting(url):
                try:
                    soup = get_soup(url)
                    content = get_content(soup)
                    jobs_data[current_id] = {'content': content}
                    save_checkpoint(current_id, max_id)
                except Exception as e:
                    logging.warning(f"Error processing job ID {current_id}: {e}")
            else:
                logging.debug(f"Skipping invalid job posting at ID {current_id}")
            
            pbar.update(1)

            if len(jobs_data) % 1000 == 0:
                save_to_csv(jobs_data)
                jobs_data.clear()

    if jobs_data:
        save_to_csv(jobs_data)

def save_to_csv(data):
    mode = 'a' if os.path.exists(OUTPUT_FILE) else 'w'
    with open(OUTPUT_FILE, mode, newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['job_id', 'content'])
        if mode == 'w':
            writer.writeheader()
        for job_id, job_data in data.items():
            writer.writerow({'job_id': job_id, 'content': job_data['content']})
    logging.info(f"Saved {len(data)} jobs to {OUTPUT_FILE}")

if __name__ == "__main__":
    logging.info("Starting job scraping")
    scrape_jobs()
    logging.info("Scraping completed")