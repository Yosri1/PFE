# main.py
import json
import pandas as pd 
from config.config import DATABASE_URL
from utils.logging_utils import setup_logging
from utils.db_utils import get_engine, save_to_db , save_to_db_non_dupe
from scrapers.optioncarriere import scrape_optioncarriere
from scrapers.keejob import scrape_keejob
from utils.deduplicate_jobs import deduplicate_jobs_by_description
def main():
    """Main function to run the scraper."""
    logger = setup_logging()
    engine = get_engine(DATABASE_URL)
    
    try:
        optioncarriere_jobs = scrape_optioncarriere(logger)
        keejob_jobs = scrape_keejob(logger)
        all_jobs = optioncarriere_jobs + keejob_jobs
        save_to_db(all_jobs, engine)
        df = pd.read_sql("SELECT * FROM job_postings", engine)
        non_dupe_jobs = deduplicate_jobs_by_description(pd.read_sql("SELECT * FROM job_postings", engine))
        save_to_db_non_dupe(non_dupe_jobs, engine)

        return {"status": "success", "new_jobs_added": "z"}
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    result = main()
    print(json.dumps(result, default=str))
