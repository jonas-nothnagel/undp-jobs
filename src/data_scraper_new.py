import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from tqdm import tqdm
import time
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_URL = 'https://jobs.undp.org/cj_view_job.cfm?cur_job_id='
CHECKPOINT_FILE = 'checkpoint.txt'
OUTPUT_FILE = 'undp_jobs.csv'
INITIAL_JOB_ID = 1150
MAX_JOB_ID = 2000  # Set a reasonable upper limit to prevent infinite loops
MAX_RETRIES = 3
RETRY_DELAY = 5
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
                  'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                  'Chrome/58.0.3029.110 Safari/537.3'
}

def get_soup(session, url, retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            logging.debug(f"Successfully fetched {url}")
            return BeautifulSoup(response.content, "html.parser"), response.url
        except requests.RequestException as e:
            logging.warning(f"Attempt {attempt} - Error fetching {url}: {e}")
            if attempt < retries:
                sleep_time = RETRY_DELAY * attempt  # Exponential backoff
                logging.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logging.error(f"Failed to fetch {url} after {retries} attempts")
    return None, url  # Return None if all retries fail

def get_content(soup):
    logging.debug("Extracting content from soup")
    job_details = {}
    try:
        # Example extraction logic - adjust based on actual page structure
        title_tag = soup.find('h1', class_='job-title')  # Adjust class as needed
        job_details['title'] = title_tag.get_text(strip=True) if title_tag else 'N/A'

        description_tag = soup.find('div', class_='job-description')  # Adjust class as needed
        job_details['description'] = description_tag.get_text(strip=True) if description_tag else 'N/A'

        # Add more fields as necessary
        # For example:
        # location_tag = soup.find('span', class_='job-location')
        # job_details['location'] = location_tag.get_text(strip=True) if location_tag else 'N/A'

        # Check if meaningful content was extracted
        if job_details['title'] != 'N/A' or job_details['description'] != 'N/A':
            logging.debug(f"Extracted job details: {job_details}")
            return job_details
        else:
            logging.debug("No meaningful content found in job details")
            return None
    except Exception as e:
        logging.error(f"Error extracting content: {e}")
        return None

def is_valid_job_posting(final_url):
    # Adjust the validation based on the actual redirect behavior
    is_valid = "cj_view_job.cfm" in final_url
    logging.debug(f"URL {final_url} is {'valid' if is_valid else 'invalid'} job posting")
    return is_valid

def get_last_job_id():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            try:
                return int(f.read().strip())
            except ValueError:
                logging.warning("Checkpoint file is corrupted. Starting from INITIAL_JOB_ID.")
    return INITIAL_JOB_ID

def save_checkpoint(job_id):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(str(job_id))

def scrape_jobs(start_id, max_id=MAX_JOB_ID):
    current_id = start_id
    jobs_data = {}
    total_jobs_scraped = 0
    failed_jobs = 0

    with requests.Session() as session:
        session.headers.update(HEADERS)

        with tqdm(total=max_id - start_id + 1, desc="Scraping jobs", unit="job") as pbar:
            while current_id <= max_id:
                url = f"{BASE_URL}{current_id}"
                soup, final_url = get_soup(session, url)

                if soup is None:
                    logging.error(f"Failed to retrieve content for job ID {current_id}. Skipping.")
                    failed_jobs += 1
                elif is_valid_job_posting(final_url):
                    content = get_content(soup)
                    if content:
                        jobs_data[current_id] = content
                        save_checkpoint(current_id)
                        total_jobs_scraped += 1
                        logging.info(f"Successfully scraped job ID {current_id}")
                    else:
                        logging.warning(f"No content found for job ID {current_id}")
                else:
                    logging.info(f"No valid job posting found at ID {current_id}. Skipping.")

                current_id += 1
                pbar.update(1)

                # Save progress every 1000 jobs or at the end
                if len(jobs_data) >= 1000:
                    save_to_csv(jobs_data)
                    jobs_data.clear()

            # Save any remaining data
            if jobs_data:
                save_to_csv(jobs_data)

    logging.info(f"Scraping completed. Total jobs scraped: {total_jobs_scraped}, Failed jobs: {failed_jobs}")

def save_to_csv(data):
    df = pd.DataFrame.from_dict(data, orient='index')
    df.index.name = 'job_id'
    # Ensure the output directory exists
    output_dir = os.path.dirname(OUTPUT_FILE)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    df.to_csv(OUTPUT_FILE, mode='a', header=not os.path.exists(OUTPUT_FILE), index=True)
    logging.info(f"Saved {len(data)} jobs to {OUTPUT_FILE}")

if __name__ == "__main__":
    last_job_id = get_last_job_id()
    logging.info(f"Starting scrape from job ID: {last_job_id}")
    scrape_jobs(last_job_id)
    logging.info("Scraping completed")
