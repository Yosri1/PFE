# nlp/gemini_nlp.py
import google.generativeai as genai
import time
import json
import re

def setup_gemini(api_key):
    """Set up Gemini API client."""
    genai.configure(api_key=api_key)
    return genai.GenerativeModel('gemini-2.0-flash')

def job_analysis(job_description, model):
    """Analyze job description using Gemini API."""
    prompt = f"""Analyze the following French job description and extract the specified information. Return the results in a structured JSON format. If a piece of information is not found in the description, use "null" as its value.

    **Guidelines:**
    - Keep "company_sector", "company_size", and "educational_qualifications" concise (one or two words max).
    - Extract technical and behavioral skills as lists (each skill 1 or two words max).
    - Output should always be in English.
    **Job Description:**
    {job_description}

    **Expected JSON Output Format:**
    {{
      "company_sector": "...",
      "company_size": "...",
      "years_of_experience": "...",
      "educational_qualifications": "...",
      "technical_skills": [...],
      "certifications": [...],
      "behavioral_skills": [...],
      "languages": [...]
    }}
    """
    response = model.generate_content(prompt)
    time.sleep(3.5)  # Rate limiting
    return response.text

def process_json_list(analysis_results):
    """Process a list of JSON strings and clean/load them."""
    cleaned_json_data = []
    for json_str in analysis_results:
        try:
            clean_str = json_str.replace('```json', '').replace('```', '').strip()
            clean_str = re.sub(r'\n(?=(?:[^"]*"[^"]*")*[^"]*$)', '', clean_str)
            json_data = json.loads(clean_str)
            cleaned_json_data.append(json_data)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            print(f"Problematic string: {json_str}")
            continue
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return []
    return cleaned_json_data
