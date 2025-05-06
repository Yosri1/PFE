import unittest
import json
from unittest.mock import patch, MagicMock
from gemini_nlp import setup_gemini, job_analysis, process_json_list

class TestGeminiNLP(unittest.TestCase):
    def setUp(self):
        self.api_key = "test_api_key"
        self.mock_model = MagicMock()
        self.sample_job_description = """
        Poste d'ingénieur logiciel chez TechCorp, une entreprise technologique.
        Exigences : 3-5 ans d'expérience, diplôme en informatique,
        compétences en Python, Java, communication, travail d'équipe.
        Certifications : AWS préférée. Langues : Français, Anglais.
        """
        self.sample_response = '''```json
        {
            "company_sector": "Technology",
            "company_size": "null",
            "years_of_experience": "3-5",
            "educational_qualifications": "Computer Science",
            "technical_skills": ["Python", "Java"],
            "certifications": ["AWS"],
            "behavioral_skills": ["Communication", "Teamwork"],
            "languages": ["French", "English"]
        }
        ```'''

    @patch('gemini_nlp.genai')
    def test_setup_gemini(self, mock_genai):
        mock_genai.GenerativeModel.return_value = self.mock_model
        result = setup_gemini(self.api_key)
        mock_genai.configure.assert_called_with(api_key=self.api_key)
        self.assertEqual(result, self.mock_model)

    def test_job_analysis(self):
        self.mock_model.generate_content.return_value = MagicMock(text=self.sample_response)
        result = job_analysis(self.sample_job_description, self.mock_model)
        self.assertEqual(result, self.sample_response)

    def test_process_json_list_valid(self):
        analysis_results = [self.sample_response]
        expected_output = [{
            "company_sector": "Technology",
            "company_size": "null",
            "years_of_experience": "3-5",
            "educational_qualifications": "Computer Science",
            "technical_skills": ["Python", "Java"],
            "certifications": ["AWS"],
            "behavioral_skills": ["Communication", "Teamwork"],
            "languages": ["French", "English"]
        }]
        result = process_json_list(analysis_results)
        self.assertEqual(result, expected_output)

    def test_process_json_list_invalid_json(self):
        analysis_results = ['invalid json']
        result = process_json_list(analysis_results)
        self.assertEqual(result, [])

    def test_process_json_list_empty(self):
        analysis_results = []
        result = process_json_list(analysis_results)
        self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()
