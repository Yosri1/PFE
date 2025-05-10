# scrapers/optioncarriere.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
from utils.text_utils import remove_extra_spaces
from datetime import datetime

today_date = datetime.now()
def find_number_of_pages(parent_url, logger):
    """Find the number of pages to scrape."""
    page_number = 1
    while True:
        url = parent_url.format(i=page_number)
        try:
            response = requests.get(url, timeout=10,  )
            response.raise_for_status()
            parent_soup = BeautifulSoup(response.text, 'html5lib')
            no_results = parent_soup.find('p', class_='mb-2', string='Aucun résultat. Veuillez modifier votre recherche.')
            if no_results or not parent_soup.find_all('article', class_='job clicky'):
                return page_number - 1
            page_number += 1
            logger.info(f"Checking page {page_number}..   ")
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

def scrape_optioncarriere(logger,Major):
    """Scrape job postings from Optioncarriere."""
    parent_url = f"https://www.optioncarriere.tn/emploi?s={Major}&l=Tunisie&p={{i}}"
    num_pages = find_number_of_pages(parent_url, logger)
    logger.info(f"Number of pages to scrape from Optioncarriere: {num_pages}")
    
    job_data = []
    for i in range(1, num_pages + 1):
        url = parent_url.format(i=i)
        try:
            response = requests.get(url, timeout=10,   )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html5lib')
            job_containers = soup.find_all("article", class_="job clicky")
            for anchor in job_containers:
                abs_url = 'https://www.optioncarriere.tn' + anchor['data-url']
                job_response = requests.get(abs_url, timeout=10,  )
                job_response.raise_for_status()
                job_soup = BeautifulSoup(job_response.text, 'html5lib')
                meta = extract_optioncarriere_meta(job_soup)
                meta['Source'] = 'Optioncarriere'
                meta['Major'] = Major

                job_data.append(meta)
        except requests.RequestException as e:
            logger.error(f"Error fetching URL {url}: {e}")
            continue
    return job_data
