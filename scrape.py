import json
import pandas as pd
from config.config import DATABASE_URL
from utils.logging_utils import setup_logging
from utils.db_utils import get_engine, save_to_db, save_to_db_non_dupe
from scrapers.optioncarriere import scrape_optioncarriere
from scrapers.keejob import scrape_keejob
from utils.deduplicate_jobs import deduplicate_jobs_by_description

def main():
    """Main function to run the scraper."""
    logger = setup_logging()
    engine = get_engine(DATABASE_URL)

    try:
        Majors = ["Business", "Finance", "Marketing", "Information Technology", "Accounting", "comptabilité"]
        all_jobs = []  # Initialize once outside the loop

        # Step 1: Scrape job postings from all sources for each major
        for Major in Majors:
            logger.info(f" Currently working on {Major}  Major ")
            optioncarriere_jobs = scrape_optioncarriere(logger,Major)
            keejob_jobs = scrape_keejob(logger,Major)
            all_jobs.extend(keejob_jobs + optioncarriere_jobs)  # Accumulate


        if not all_jobs:
            logger.info("No jobs scraped. Exiting.")
            return {"status": "success", "new_jobs_added": 0}

        # Step 2: Append all raw jobs to job_postings table
        save_to_db(all_jobs, engine)

        # Step 3: Read all raw jobs from database
        all_jobs_df = pd.read_sql("SELECT * FROM job_postings", engine)

        # Step 4: Deduplicate based on job description
        non_dupe_jobs = deduplicate_jobs_by_description(all_jobs_df)

        # Step 5: Replace deduplicated results in another table
        save_to_db_non_dupe(non_dupe_jobs, engine)

        return {"status": "success", "new_jobs_added": len(all_jobs)}

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    result = main()
    print(json.dumps(result, default=str))
