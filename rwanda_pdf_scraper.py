from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import requests

def scrape_amategeko():
    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode (no browser window)
    driver = webdriver.Chrome(options=options)
    
    # Open the main page
    base_url = 'https://www.amategeko.gov.rw/laws/in-force/1?child=1.1'
    driver.get(base_url)
    
    # Wait for the main page to load
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.ID, 'root')))
    
    # Initialize data structures
    processed_elements = set()
    pdf_data = []

    def process_element(element):
        # Find all clickable items (expandable elements)
        # Adjust the selector based on the website's HTML structure
        try:
            # Example: clickable elements have class 'clickable-item'
            clickable_items = element.find_elements(By.CSS_SELECTOR, '.clickable-item')
            print(clickable_items)
            for item in clickable_items:
                # Get a unique identifier for the item to avoid reprocessing
                item_id = item.get_attribute('data-id') or item.text

                if item_id not in processed_elements:
                    processed_elements.add(item_id)
                    
                    # Click to expand the item
                    driver.execute_script("arguments[0].click();", item)
                    time.sleep(0.5)  # Wait for content to load

                    # Recursively process the new content
                    process_element(element)
        except Exception as e:
            print(f"Error processing clickable items: {e}")

        # Find all links to sub-pages with PDFs
        try:
            # Adjust the selector based on the website's HTML structure
            pdf_links = element.find_elements(By.CSS_SELECTOR, 'a[href^="/view/toc/doc/"]')

            for link in pdf_links:
                href = link.get_attribute('href')
                name = link.text.strip()
                date_element = link.find_element(By.XPATH, './following-sibling::div[@class="date"]')
                date = date_element.text.strip() if date_element else 'Unknown'

                pdf_data.append({'href': href, 'name': name, 'date': date})
        except Exception as e:
            print(f"Error finding PDF links: {e}")

    try:
        # Start processing from the root element
        root_element = driver.find_element(By.ID, 'root')
        process_element(root_element)
    except Exception as e:
        print(f"An error occurred during scraping: {e}")
    finally:
        driver.quit()

    # Output the collected data
    print("Collected PDF Data:")
    for item in pdf_data:
        print(item)

    # Download the PDFs
    download_pdfs(pdf_data)

def download_pdfs(pdf_data):
    # Set up Selenium WebDriver for downloading PDFs
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)

    download_folder = 'pdf_downloads'
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    for item in pdf_data:
        href = item['href']
        name = item['name']
        print(f"Processing PDF: {name}")

        try:
            # Open the sub-page where the PDF is located
            driver.get(href)
            wait = WebDriverWait(driver, 10)

            # Find the PDF link on the sub-page
            pdf_link_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href$=".pdf"]')))
            pdf_url = pdf_link_element.get_attribute('href')

            # Download the PDF using requests
            response = requests.get(pdf_url)
            response.raise_for_status()

            # Save the PDF file
            pdf_filename = f"{name}.pdf".replace('/', '_').replace('\\', '_')
            pdf_path = os.path.join(download_folder, pdf_filename)
            with open(pdf_path, 'wb') as f:
                f.write(response.content)

            print(f"Downloaded PDF: {pdf_filename}")
        except Exception as e:
            print(f"Failed to download PDF for {name}: {e}")

    driver.quit()

if __name__ == "__main__":
    scrape_amategeko()
