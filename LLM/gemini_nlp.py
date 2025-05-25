import google.generativeai as genai
import time
import json
import re
import logging
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Job category dictionary
JOB_CATEGORIES = {
    "Accounting": [
        "auditor", "financial accountant", "management accountant", "tax accountant",
        "budget analyst", "public accountant", "Other"
    ],
    "BUSINESS ANALYTICS": [
        "Data analysis", "Business intelligence", "Business Analyst", "Operations research",
        "Project manager", "Analytics Manager", "Consultant", "Database Administrator",
        "Supply chain management", "Chief Data Officer", "Management Consultant",
        "Management Analyst", "Business Development Manager", "Other"
    ],
    "Finance": [
        "financial services", "corporate financial management", "commercial banking",
        "investment banking", "capital markets", "Other"
    ],
    "INFORMATION TECHNOLOGY": [
        "Quality assurance manager", "Online content specialist",
        "Digital marketing and communication consultant", "Business process manager",
        "Information management specialist", "Project manager", "ICT manager", "Other"
    ],
    "MARKETING": [
        "marketing managers", "brand managers", "product managers",
        "customer relationship managers", "sales managers", "marketing consultants", "Other"
    ]
}

def setup_gemini(api_key: str, model_name: str = 'gemma-3-27b-it') -> genai.GenerativeModel:
    """Set up Gemini API client."""
    logger.info("Setting up Gemini API client")
    if not api_key:
        logger.error("API key is empty")
        raise ValueError("API key cannot be empty")
    
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)
        logger.info(f"Gemini API client setup successful with model {model_name}")
        return model
    except Exception as e:
        logger.error(f"Failed to setup Gemini API client: {str(e)}")
        raise

def job_analysis(job_description: str, model: genai.GenerativeModel) -> str:
    """Analyze job description using Gemini API."""
    logger.info(f"Starting job analysis for description: {job_description[:50]}...")
    if not job_description.strip():
        logger.error("Job description is empty")
        raise ValueError("Job description cannot be empty")

    try:
        prompt = f"""Analyze the provided job description and extract the specified information. Return the results in a structured JSON format. If a piece of information is not explicitly mentioned, use "null" as its value. For 'job_category', classify the job as one specific job role from the provided reference dictionary: {json.dumps(JOB_CATEGORIES, indent=2)}. The dictionary maps major categories (e.g., "Accounting") to lists of specific job roles (e.g., "auditor", "financial accountant"). Select the most relevant job role based on the job description. If no specific job role matches, use "Other".

**Guidelines:**
- Ensure all extracted information is in English, regardless of the job description's language.
- For 'job_category', return only the specific job role (e.g., "auditor", "Business Analyst"), not the major category (e.g., "Accounting").
- Keep "company_sector" concise max 2 words,
- Keep  "company_size" a choice between 4 options ["Startup", "Small", "Meduim","Large"]
- Keep "Contract_type" a choice between 3 options ["Full-time", "Part-time","Internship]
- Keep educational_qualifications" choice between 3 options []"Bachelors" ,"Masters" , "Other"]
- Extract "technical_skills" and "behavioral_skills" as lists, with each skill described in 1-2 words (e.g., "Python", "Teamwork").
- For "years_of_experience", extract a specific number (float in years) if provided; otherwise, use "null".
- For "certifications" and "languages", list specific entries (e.g., "AWS Certified", "French") or use "null" if none are mentioned.
- If the job description is empty, vague, or missing details, return "null" for all fields except "job_category", which should be "Other".
- Ensure the output is valid JSON without code block markers.

**Job Description:**
{job_description}

**Expected JSON Output Format:**
{{
  "company_sector": "...",
  "company_size": "...",
  "Contract_type": "...",
  "job_category": "...",
  "years_of_experience": "...",
  "educational_qualifications": "...",
  "technical_skills": ["...", "..."],
  "certifications": ["...", "..."],
  "behavioral_skills": ["...", "..."],
  "languages": ["...", "..."]
}}
"""
        logger.debug("Sending prompt to Gemini API")
        response = model.generate_content(prompt)
        logger.info("Received response from Gemini API")
        return response.text
    except Exception as e:
        logger.error(f"Error in job analysis: {str(e)}")
        raise

def process_json_list(analysis_results: List[str]) -> List[Dict]:
    """Process a list of JSON strings and clean/load them."""
    logger.info(f"Processing {len(analysis_results)} JSON analysis results")
    cleaned_json_data = []
    
    for i, json_str in enumerate(analysis_results, 1):
        logger.debug(f"Processing JSON string {i}")
        try:
            # Remove code block markers and extra whitespace
            clean_str = re.sub(r'```(?:json)?\n?', '', json_str).strip()
            # Remove trailing commas before closing braces/brackets
            clean_str = re.sub(r',\s*([\]\}])', r'\1', clean_str)
            json_data = json.loads(clean_str)
            logger.info(f"Successfully parsed JSON string {i}")
            cleaned_json_data.append(json_data)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in string {i}: {str(e)}")
            logger.debug(f"Problematic string: {json_str}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error in string {i}: {str(e)}")
            continue
    
    logger.info(f"Successfully processed {len(cleaned_json_data)} JSON objects")
    return cleaned_json_data