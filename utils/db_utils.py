# utils/db_utils.py
from sqlalchemy import create_engine
import pandas as pd
import uuid
import logging
from datetime import datetime
logger = logging.getLogger(__name__)

def get_engine(database_url):
    """Create and return a SQLAlchemy engine."""
    return create_engine(database_url)

def save_to_db_non_dupe(job_data, engine):
    """Save job postings to PostgreSQL database."""
    if not job_data:
        logger.info("No new job postings to save.")
        return
    
    df = pd.DataFrame(job_data)
    df['ID'] = [str(uuid.uuid4()) for _ in range(len(df))]
    df['Scraped'] = datetime.now().date()

    columns = ["ID","Major","JobTitle", "Published", "JobType", "WorkLocation", "Experience", 
               "Education", "Availability", "Langues", "Entreprise", 
               "Sector", "Size", "Description", "Source","Scraped"]
    df = df.reindex(columns=columns)
    
    with engine.connect() as conn:
        df.to_sql('job_postings', conn, if_exists='replace', index=False)
    logger.info(f"Successfully saved {len(df)} non duped job postings to database.")
def save_to_db(job_data, engine):
    """Save job postings to PostgreSQL database."""
    if not job_data:
        logger.info("No new job postings to save.")
        return
    
    df = pd.DataFrame(job_data)
    df['ID'] = [str(uuid.uuid4()) for _ in range(len(df))]

    columns = ["ID","Major","JobTitle", "Published", "JobType", "WorkLocation", "Experience", 
               "Education", "Availability", "Langues", "Entreprise", 
               "Sector", "Size", "Description", "Source","Scraped",]
    df = df.reindex(columns=columns)
    
    with engine.connect() as conn:
        df.to_sql('job_postings', conn, if_exists='append', index=False)
    logger.info(f"Successfully saved {len(df)} new job postings to database.")
