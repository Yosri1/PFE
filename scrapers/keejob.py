# scrapers/keejob.py
import requests
from bs4 import BeautifulSoup
import dateparser
from utils.text_utils import remove_extra_spaces
import re 
def scrape_keejob(logger):
    """Scrape job postings from Keejob."""
    parent_url = "https://www.keejob.com/offres-emploi/?keywords=agroalimentaire&page={i}"
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
