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
OUTPUT_FILE = '../data/undp_jobs.csv'
MIN_JOB_ID = 1150
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
        for i in soup.find_all('main'):
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

def find_max_job_id(start_id=MIN_JOB_ID, threshold=100000):
    current_id = start_id
    with tqdm(desc="Finding max job ID", unit="job") as pbar:
        while True:
            url = f"{BASE_URL}{current_id}"
            if current_id < threshold or is_valid_job_posting(url):
                current_id += 1
                pbar.update(1)
            else:
                break
    
    max_id = current_id - 1  # Subtract 1 to get the last valid ID
    logging.info(f"Max job ID found: {max_id}")
    print("Max job ID:", max_id)
    return max_id

def scrape_jobs(threshold=100000):
    last_scraped_id, prev_max_id = get_checkpoint_data()
    
    print("Finding the maximum job ID...")
    current_max_id = find_max_job_id(max(MIN_JOB_ID, last_scraped_id), threshold)
    
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
            
            if current_id >= threshold and not is_valid_job_posting(url):
                logging.debug(f"Skipping invalid job posting at ID {current_id}")
                pbar.update(1)
                continue
            
            try:
                soup = get_soup(url)
                content = get_content(soup)
                jobs_data[current_id] = {'content': content}
                save_checkpoint(current_id, max_id)
            except Exception as e:
                logging.warning(f"Error processing job ID {current_id}: {e}")
            
            pbar.update(1)
            """
            Batch Processing:

            Instead of writing to the CSV file after every single job is scraped (which would be inefficient due to frequent disk I/O operations), we accumulate data in the jobs_data dictionary.
            When we've collected 1000 jobs, we write them all at once to the CSV file.

            Memory Management:
            After writing every 1000 jobs, we clear the jobs_data dictionary. This prevents the dictionary from growing too large and consuming too much memory, especially when scraping a large number of jobs.

            Handling Remainders:
            The final if jobs_data: check is to handle any remaining jobs that didn't make it to a full batch of 1000. For example, if we scraped 3500 jobs total, this would write the last 500 jobs to the CSV.

            Data Safety:
            By writing to the file periodically, we ensure that if the script crashes or is interrupted, we don't lose all the data we've scraped. At most, we'd lose the jobs in the current batch (up to 999 jobs).

            Appending, Not Overwriting:
            It's important to note that in the save_to_csv function, we're using mode 'a' (append), not 'w' (write/overwrite). This means each batch of data is appended to the end of the file, not overwriting previous data.
            """
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
    scrape_jobs(threshold=100000)
    logging.info("Scraping completed")