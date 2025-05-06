import google.generativeai as genai
import time
import json
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_gemini(api_key):
    """Set up Gemini API client."""
    logger.info("Setting up Gemini API client")
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')
        logger.info("Gemini API client setup successful")
        return model
    except Exception as e:
        logger.error(f"Failed to setup Gemini API client: {str(e)}")
        raise

def job_analysis(job_description, model):
    """Analyze job description using Gemini API."""
    logger.info(f"Starting job analysis for description: {job_description[:50]}...")
    try:
        prompt = f"""Analyze the following French job description and extract the specified information. Return the results in a structured JSON format. If a piece of information is not found in the description, use "null" as its value.

        **Guidelines:**
        - Keep "company_sector", "company_size", and "educational_qualifications" concise (one or two words max).
        - Extract technical and behavioral skills as lists (each skill 1 or two words max).
        - Output should always be in English.
        **Job Description:**
        {job_description}

        **Expected JSON Output Format:**
        {{
          "company Sector": "...",
          "company_size": "...",
          "years_of_experience": "...",
          "educational_qualifications": "...",
          "technical_skills": [...],
          "certifications": [...],
          "behavioral_skills": [...],
          "languages": [...]
        }}
        """
        logger.debug("Sending prompt to Gemini API")
        response = model.generate_content(prompt)
        logger.info("Received response from Gemini API")
        time.sleep(3.5)  # Rate limiting
        logger.debug(f"Response text: {response.text[:100]}...")
        return response.text
    except Exception as e:
        logger.error(f"Error in job analysis: {str(e)}")
        raise

def process_json_list(analysis_results):
    """Process a list of JSON strings and clean/load them."""
    logger.info(f"Processing {len(analysis_results)} JSON analysis results")
    cleaned_json_data = []
    for i, json_str in enumerate(analysis_results, 1):
        logger.debug(f"Processing JSON string {i}/{len(analysis_results)}")
        try:
            clean_str = json_str.replace('```json', '').replace('```', '').strip()
            clean_str = re.sub(r'\n(?=(?:[^"]*"[^"]*")*[^"]*$)', '', clean_str)
            json_data = json.loads(clean_str)
            logger.info(f"Successfully parsed JSON string {i}")
            cleaned_json_data.append(json_data)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in string {i}: {str(e)}")
            logger.debug(f"Problematic string: {json_str}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error in string {i}: {str(e)}")
            return []
    logger.info(f"Successfully processed {len(cleaned_json_data)} JSON objects")
    return cleaned_json_data
