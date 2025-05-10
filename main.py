# main.py
import json
import pandas as pd 
from config.config import DATABASE_URL
from utils.logging_utils import setup_logging
from utils.db_utils import get_engine, save_to_db
from scrapers.optioncarriere import scrape_optioncarriere
from scrapers.keejob import scrape_keejob
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

        return {"status": "success", "new_jobs_added": len(all_jobs)}
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    result = main()
    print(json.dumps(result, default=str))
