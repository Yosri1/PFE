import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import dateparser
from sqlalchemy import create_engine
import uuid
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = "postgresql://neondb_owner:npg_DKdOBoM1nZ2G@ep-floral-waterfall-a8767ul3-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
engine = create_engine(DATABASE_URL)

def remove_extra_spaces(text):
    """Remove extra spaces from text."""
    if text:
        return re.sub(r'\s{2,}', ' ', text.strip())
    return None

def find_number_of_pages(parent_url):
    """Find the number of pages to scrape."""
    page_number = 1
    while True:
        url = parent_url.format(i=page_number)
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            parent_soup = BeautifulSoup(response.text, 'html5lib')
            no_results = parent_soup.find('p', class_='mb-2', string='Aucun résultat. Veuillez modifier votre recherche.')
            if no_results or not parent_soup.find_all('article', class_='job clicky'):
                return page_number - 1
            page_number += 1
            logger.info(f"Checking page {page_number}...")
        except requests.RequestException as e:
            logger.error(f"Error fetching page {page_number}: {e}")
            return page_number - 1

def extract_optioncarriere_meta(soup):
    """Extract meta information from Optioncarriere job posting."""
    meta_info = {}
    meta_info['Entreprise'] = remove_extra_spaces(soup.find('p', class_='company').text if soup.find('p', class_='company') else None)
    meta_info['Sector'] = None
    meta_info['Size'] = None
    description = soup.find('section', class_="content").get_text()
    meta_info['Description'] = remove_extra_spaces(description.replace('\xa0', ' '))
    meta_info['JobTitle'] = remove_extra_spaces(soup.find('h1').text if soup.find('h1') else None)
    
    details_ul = soup.find('ul', class_='details')
    if details_ul:
        location_tag = details_ul.find('li')
        meta_info['WorkLocation'] = remove_extra_spaces(location_tag.find('span').text if location_tag.find('svg', {'xlink:href': '#icon-location'}) else location_tag.text)
        job_type_tag = location_tag.find_next_sibling('li')
        meta_info['JobType'] = remove_extra_spaces(job_type_tag.text if job_type_tag else None)
        availability_tag = job_type_tag.find_next_sibling('li') if job_type_tag else None
        meta_info['Availability'] = remove_extra_spaces(availability_tag.text if availability_tag else None)
    
    tags_ul = soup.find('ul', class_='tags')
    if tags_ul:
        published_text = tags_ul.find('span', class_='badge badge-r badge-s').text.strip() if tags_ul.find('span', class_='badge badge-r badge-s') else None
        if published_text and "Il y a" in published_text:
            days_match = re.search(r'Il y a (\d+) jours?', published_text)
            months_match = re.search(r'Il y a (\d+) mois', published_text)
            if days_match:
                days = int(days_match.group(1))
                meta_info['Published'] = datetime.now().date() - timedelta(days=days)
            elif months_match:
                months = int(months_match.group(1))
                meta_info['Published'] = datetime.now().date() - timedelta(days=months * 30)
    
    meta_info['Reference'] = None
    meta_info['Experience'] = None
    meta_info['Education'] = None
    meta_info['Proposed_remuneration'] = None
    meta_info['Langues'] = None
    return meta_info

def extract_keejob_meta(soup):
    """Extract meta information from Keejob job posting."""
    meta_info = {}
    content_div = soup.find('div', class_='span9 content')
    meta_info['Entreprise'] = remove_extra_spaces(content_div.find('b').find('a').text if content_div and content_div.find('b') and content_div.find('b').find('a') else None)
    sector_b = content_div.find("b", text=re.compile('Secteur:')) if content_div else None
    meta_info['Sector'] = remove_extra_spaces(sector_b.next_sibling.strip() if sector_b else None)
    size_b = content_div.find("b", text=re.compile('Taille:')) if content_div else None
    meta_info['Size'] = remove_extra_spaces(size_b.next_sibling.strip() if size_b else None)
    description = soup.find('div', class_="block_a span12 no-margin-left").get_text()
    meta_info['Description'] = remove_extra_spaces(description.replace('\xa0', ' '))
    
    meta_divs = soup.find_all("div", class_="meta")
    for meta_div in meta_divs:
        b_tag = meta_div.find("b")
        if b_tag:
            label = b_tag.text.strip().replace(":", "")
            value = remove_extra_spaces(meta_div.text.replace(b_tag.text, "").replace(":", "").strip().split(">")[-1])
            meta_info[label] = value
    
    la_date = meta_info.get("Publiée le")
    meta_info['Published'] = dateparser.parse(la_date) if la_date else None
    meta_info['JobTitle'] = remove_extra_spaces(soup.find('h1').text if soup.find('h1') else None)
    return meta_info

def scrape_optioncarriere():
    """Scrape job postings from Optioncarriere."""
    parent_url = "https://www.optioncarriere.tn/emploi?s=Business&l=Tunisie&p={i}"
    num_pages = find_number_of_pages(parent_url)
    logger.info(f"Number of pages to scrape from Optioncarriere: {num_pages}")
    
    job_data = []
    for i in range(1, num_pages + 1):
        url = parent_url.format(i=i)
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html5lib')
            job_containers = soup.find_all("article", class_="job clicky")
            for anchor in job_containers:
                abs_url = 'https://www.optioncarriere.tn' + anchor['data-url']
                job_response = requests.get(abs_url, timeout=10)
                job_response.raise_for_status()
                job_soup = BeautifulSoup(job_response.text, 'html5lib')
                meta = extract_optioncarriere_meta(job_soup)
                meta['Source'] = 'Optioncarriere'
                job_data.append(meta)
        except requests.RequestException as e:
            logger.error(f"Error fetching URL {url}: {e}")
            continue
    return job_data

def scrape_keejob():
    """Scrape job postings from Keejob."""
    parent_url = "https://www.keejob.com/offres-emploi/?keywords=commerce&page={i}"
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
            except ValueError:
                logger.warning("Could not extract number of pages from Keejob.")
    
    logger.info(f"Number of pages to scrape from Keejob: {num_pages}")
    job_data = []
    for i in range(1, num_pages + 1):
        url = parent_url.format(i=i)
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html5lib')
            job_container = soup.find("div", class_="block_b row-fluid")
            if job_container:
                for anchor in job_container.find_all("a", style="color: #005593;"):
                    abs_url = 'https://www.keejob.com' + anchor['href']
                    job_response = requests.get(abs_url, timeout=10)
                    job_response.raise_for_status()
                    job_soup = BeautifulSoup(job_response.text, 'html5lib')
                    meta = extract_keejob_meta(job_soup)
                    meta['Source'] = 'Keejob'
                    job_data.append(meta)
        except requests.RequestException as e:
            logger.error(f"Error fetching URL {url}: {e}")
            continue
    return job_data

def check_existing_postings(job_data):
    """Check for existing postings in the database to avoid duplicates."""
    existing_refs = set()
    existing_combinations = set()
    query = "SELECT Reference, JobTitle, Published FROM job_postings"
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        for row in result:
            ref, title, published = row
            if ref:
                existing_refs.add(ref)
            if title and published:
                existing_combinations.add((title, str(published)))
    
    new_jobs = []
    for job in job_data:
        ref = job.get('Reference')
        title = job.get('JobTitle')
        published = job.get('Published')
        if ref and ref in existing_refs:
            continue
        if title and published and (title, str(published)) in existing_combinations:
            continue
        new_jobs.append(job)
    return new_jobs

def save_to_db(job_data):
    """Save job postings to PostgreSQL database."""
    if not job_data:
        logger.info("No new job postings to save.")
        return
    
    df = pd.DataFrame(job_data)
    df['ID'] = [str(uuid.uuid4()) for _ in range(len(df))]
    columns = ["ID", "Reference", "JobTitle", "Published", "JobType", "WorkLocation", "Experience", 
               "Education", "Proposed_remuneration", "Availability", "Langues", "Entreprise", 
               "Sector", "Size", "Description", "Source"]
    df = df.reindex(columns=columns)
    
    with engine.connect() as conn:
        df.to_sql('job_postings', conn, if_exists='append', index=False)
    logger.info(f"Successfully saved {len(df)} new job postings to database.")

def main():
    """Main function to run the scraper."""
    try:
        optioncarriere_jobs = scrape_optioncarriere()
        keejob_jobs = scrape_keejob()
        all_jobs = optioncarriere_jobs + keejob_jobs
        new_jobs = check_existing_postings(all_jobs)
        save_to_db(new_jobs)
        return {"status": "success", "new_jobs_added": len(new_jobs)}
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    result = main()
    print(json.dumps(result, default=str))
