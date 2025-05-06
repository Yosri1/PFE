import unittest
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from scrapers.keejob import scrape_keejob, extract_keejob_meta

class TestKeejobScraper(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.sample_html = """
        <html>
            <body>
                <nav class="nav-pagination">
                    <a class="page-link" aria-label="Page 2">2</a>
                </nav>
                <div class="block_b row-fluid">
                    <a href="/job/123" style="color: #005593;">Job Link</a>
                </div>
            </body>
        </html>
        """
        self.sample_job_html = """
        <html>
            <body>
                <h1>Software Engineer</h1>
                <div class="span9 content">
                    <b><a>TechCorp</a></b>
                    <b>Secteur:</b> Technology
                    <b>Taille:</b> Large
                </div>
                <div class="block_a span12 no-margin-left">
                    Job description text
                </div>
                <div class="meta">
                    <b>Publiée le:</b> 2025-05-01
                </div>
            </body>
        </html>
        """

    @patch('scrapers.keejob.requests.get')
    def test_scrape_keejob(self, mock_get):
        mock_response = MagicMock()
        mock_response.text = self.sample_html
        mock_response.raise_for_status.return_value = None
        
        mock_job_response = MagicMock()
        mock_job_response.text = self.sample_job_html
        mock_job_response.raise_for_status.return_value = None
        
        mock_get.side_effect = [mock_response, mock_job_response]
        
        result = scrape_keejob(self.logger)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['Source'], 'Keejob')
        self.assertEqual(result[0]['JobTitle'], 'Software Engineer')
        self.assertEqual(result[0]['Entreprise'], 'TechCorp')

    @patch('scrapers.keejob.requests.get')
    def test_scrape_keejob_no_pagination(self, mock_get):
        mock_response = MagicMock()
        mock_response.text = '<html><body><div class="block_b row-fluid"></div></body></html>'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = scrape_keejob(self.logger)
        
        self.assertEqual(len(result), 0)

    def test_extract_keejob_meta(self):
        soup = BeautifulSoup(self.sample_job_html, 'html5lib')
        result = extract_keejob_meta(soup)
        
        expected = {
            'JobTitle': 'Software Engineer',
            'Entreprise': 'TechCorp',
            'Sector': 'Technology',
            'Size': 'Large',
            'Description': 'Job description text',
            'Publiée le': '2025-05-01',
            'Published': datetime(2025, 5, 1)
        }
        
        for key in expected:
            self.assertEqual(result[key], expected[key])

    def test_extract_keejob_meta_empty(self):
        soup = BeautifulSoup('<html><body></body></html>', 'html5lib')
        result = extract_keejob_meta(soup)
        
        self.assertEqual(result.get('JobTitle'), None)
        self.assertEqual(result.get('Entreprise'), None)
        self.assertEqual(result.get('Description'), None)

    @patch('scrapers.keejob.requests.get')
    def test_scrape_keejob_request_error(self, mock_get):
        mock_get.side_effect = requests.RequestException("Connection error")
        result = scrape_keejob(self.logger)
        
        self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()
