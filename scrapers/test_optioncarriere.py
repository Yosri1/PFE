import unittest
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
from scrapers.optioncarriere import scrape_optioncarriere, find_number_of_pages, extract_optioncarriere_meta

class TestOptioncarriereScraper(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.sample_html = """
        <html>
            <body>
                <article class="job clicky" data-url="/job/123"></article>
            </body>
        </html>
        """
        self.sample_job_html = """
        <html>
            <body>
                <h1>Food Scientist</h1>
                <p class="company">Business</p>
                <section class="content">Job description text</section>
                <ul class="details">
                    <li><svg xlink:href="#icon-location"></svg><span>Tunis</span></li>
                    <li>Full-time</li>
                    <li>Immediate</li>
                </ul>
                <ul class="tags">
                    <span class="badge badge-r badge-s">Il y a 5 jours</span>
                </ul>
            </body>
        </html>
        """

    @patch('scrapers.optioncarriere.requests.get')
    def test_find_number_of_pages(self, mock_get):
        mock_response = MagicMock()
        mock_response.text = self.sample_html
        mock_response.raise_for_status.return_value = None
        
        mock_empty_response = MagicMock()
        mock_empty_response.text = '<html><body><p class="mb-2">Aucun résultat. Veuillez modifier votre recherche.</p></body></html>'
        mock_empty_response.raise_for_status.return_value = None
        
        mock_get.side_effect = [mock_response, mock_empty_response]
        
        parent_url = "https://www.optioncarriere.tn/emploi?s=Business&l=Tunisie&p={i}"
        result = find_number_of_pages(parent_url, self.logger)
        
        self.assertEqual(result, 1)

    @patch('scrapers.optioncarriere.requests.get')
    def test_scrape_optioncarriere(self, mock_get):
        mock_response = MagicMock()
        mock_response.text = self.sample_html
        mock_response.raise_for_status.return_value = None
        
        mock_job_response = MagicMock()
        mock_job_response.text = self.sample_job_html
        mock_job_response.raise_for_status.return_value = None
        
        mock_get.side_effect = [mock_response, mock_job_response, mock_response]
        
        result = scrape_optioncarriere(self.logger)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['Source'], 'Optioncarriere')
        self.assertEqual(result[0]['JobTitle'], 'Food Scientist')
        self.assertEqual(result[0]['Entreprise'], 'Business')
        self.assertEqual(result[0]['WorkLocation'], 'Tunis')

    def test_extract_optioncarriere_meta(self):
        soup = BeautifulSoup(self.sample_job_html, 'html5lib')
        result = extract_optioncarriere_meta(soup)
        
        expected = {
            'JobTitle': 'Food Scientist',
            'Entreprise': 'Business',
            'Sector': None,
            'Size': None,
            'Description': 'Job description text',
            'WorkLocation': 'Tunis',
            'JobType': 'Full-time',
            'Availability': 'Immediate',
            'Published': datetime.now().date() - timedelta(days=5),
            'Reference': None,
            'Experience': None,
            'Education': None,
            'Proposed_remuneration': None,
            'Langues': None
        }
        
        for key in expected:
            if key == 'Published':
                self.assertAlmostEqual((result[key] - expected[key]).days, 0, delta=1)
            else:
                self.assertEqual(result[key], expected[key])

    def test_extract_optioncarriere_meta_empty(self):
        soup = BeautifulSoup('<html><body></body></html>', 'html5lib')
        result = extract_optioncarriere_meta(soup)
        
        self.assertEqual(result.get('JobTitle'), None)
        self.assertEqual(result.get('Entreprise'), None)
        self.assertEqual(result.get('Description'), None)
        self.assertEqual(result.get('WorkLocation'), None)

    @patch('scrapers.optioncarriere.requests.get')
    def test_scrape_optioncarriere_request_error(self, mock_get):
        mock_get.side_effect = requests.RequestException("Connection error")
        result = scrape_optioncarriere(self.logger)
        
        self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()
