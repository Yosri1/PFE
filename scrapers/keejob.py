import requests
from bs4 import BeautifulSoup
import dateparser
from utils.text_utils import remove_extra_spaces
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_keejob(logger):
    """Scrape job postings from Keejob."""
    parent_url = "https://www.keejob.com/offres-emploi/?keywords=agroalimentaire&page={i}"
    logger.info("Starting Keejob scraping process")
    
    try:
        logger.debug(f"Fetching first page: {parent_url.format(i=1)}")
        response = requests.get(parent_url.format(i=1), timeout=10)
        response.raise_for_status()
        parent_soup = BeautifulSoup(response.text, 'html5lib')
        
        pagination_nav = parent_soup.find("nav", class_="nav-pagination")
        num_pages = 1
        if pagination_nav:
            page_links = pagination_nav.find('a', class_='page-link')
            if page_links:
                last_page_text = page_links.get("aria-label", "").split()[-1]
                try:
                    num_pages = int(last_page_text)
                    logger.info(f"Found {num_pages} pages to scrape")
                except ValueError:
                    logger.warning("Could not extract number of pages from Keejob")
        
        job_data = []
        for i in range(1, num_pages + 1):
            url = parent_url.format(i=i)
            logger.info(f"Scraping page {i}/{num_pages}: {url}")
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html5lib')
                job_container = soup.find("div", class_="block_b row-fluid")
                
                if job_container:
                    job_links = job_container.find_all("a", style="color: #005593;")
                    logger.debug(f"Found {len(job_links)} job links on page {i}")
                    for anchor in job_links:
                        abs_url = 'https://www.keejob.com' + anchor['href']
                        logger.debug(f"Fetching job posting: {abs_url}")
                        try:
                            job_response = requests.get(abs_url, timeout=10)
                            job_response.raise_for_status()
                            job_soup = BeautifulSoup(job_response.text, 'html5lib')
                            meta = extract_keejob_meta(job_soup)
                            meta['Source'] = 'Keejob'
                            job_data.append(meta)
                            logger.info(f"Successfully scraped job posting: {meta.get('JobTitle', 'Unknown')}")
                        except requests.RequestException as e:
                            logger.error(f"Error fetching job URL {abs_url}: {str(e)}")
                            continue
                else:
                    logger.warning(f"No job container found on page {i}")
            except requests.RequestException as e:
                logger.error(f"Error fetching page {url}: {str(e)}")
                continue
        
        logger.info(f"Scraping completed. Total jobs collected: {len(job_data)}")
        return job_data
    
    except requests.RequestException as e:
        logger.error(f"Error fetching initial page: {str(e)}")
        return []

def extract_keejob_meta(soup):
    """Extract meta information from Keejob job posting."""
    logger.debug("Extracting meta information from job posting")
    meta_info = {}
    
    try:
        content_div = soup.find('div', class_='span9 content')
        if content_div:
            entreprise = content_div.find('b').find('a') if content_div.find('b') else None
            meta_info['Entreprise'] = remove_extra_spaces(entreprise.text if entreprise else None)
            logger.debug(f"Extracted entreprise: {meta_info.get('Entreprise')}")
            
            sector_b = content_div.find("b", text=re.compile('Secteur:')) if content_div else None
            meta_info['Sector'] = remove_extra_spaces(sector_b.next_sibling.strip() if sector_b else None)
            logger.debug(f"Extracted sector: {meta_info.get('Sector')}")
            
            size_b = content_div.find("b", text=re.compile('Taille:')) if content_div else None
            meta_info['Size'] = remove_extra_spaces(size_b.next_sibling.strip() if size_b else None)
            logger.debug(f"Extracted size: {meta_info.get('Size')}")
        
        description_div = soup.find('div', class_="block_a span12 no-margin-left")
        meta_info['Description'] = remove_extra_spaces(description_div.get_text().replace('\xa0', ' ') if description_div else None)
        logger.debug(f"Extracted description length: {len(meta_info.get('Description', ''))}")
        
        meta_divs = soup.find_all("div", class_="meta")
        for meta_div in meta_divs:
            b_tag = meta_div.find("b")
            if b_tag:
                label = b_tag.text.strip().replace(":", "")
                value = remove_extra_spaces(meta_div.text.replace(b_tag.text, "").replace(":", "").strip().split(">")[-1])
                meta_info[label] = value
                logger.debug(f"Extracted meta {label}: {value}")
        
        la_date = meta_info.get("Publiée le")
        meta_info['Published'] = dateparser.parse(la_date) if la_date else None
        logger.debug(f"Extracted published date: {meta_info.get('Published')}")
        
        job_title = soup.find('h1')
        meta_info['JobTitle'] = remove_extra_spaces(job_title.text if job_title else None)
        logger.debug(f"Extracted job title: {meta_info.get('JobTitle')}")
        
        logger.info("Meta information extraction completed")
        return meta_info
    
    except Exception as e:
        logger.error(f"Error extracting meta information: {str(e)}")
        return meta_info
