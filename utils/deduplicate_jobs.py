import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

def deduplicate_jobs_by_description(job_data, similarity_threshold=0.9):
    """
    Removes duplicate job descriptions using cosine similarity.
    Keeps the job with the earliest  Scraped.

    Parameters:
    - job_data (list of dict): List of job postings, each with 'Description' and ' Scraped'.
    - similarity_threshold (float): Threshold above which descriptions are considered duplicates.

    Returns:
    - List of deduplicated job dicts.
    """
    logger.info("Starting job deduplication process")

    # Convert to DataFrame
    logger.info("Converting job data to DataFrame")
    df = pd.DataFrame(job_data)
    
    if 'Description' not in df.columns or 'Scraped' not in df.columns:
        logger.error("Missing required columns: 'Description' and 'Scraped'")
        raise ValueError("Each job dict must contain 'Description' and 'Scraped' keys.")

    # Parse date
    logger.info("Parsing  Scraped column")
    df['Scraped'] = pd.to_datetime(df['Scraped'], errors='coerce')
    
    # Vectorize job descriptions
    logger.info("Vectorizing job descriptions")
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix = vectorizer.fit_transform(df['Description'].fillna(""))

    # Compute similarity
    logger.info("Computing cosine similarity matrix")
    similarity_matrix = cosine_similarity(tfidf_matrix)

    # Track which jobs to keep
    logger.info("Identifying duplicate jobs")
    to_keep = set()
    seen = set()

    for i in range(len(df)):
        if i in seen:
            continue

        group = [i]
        for j in range(i + 1, len(df)):
            if similarity_matrix[i][j] >= similarity_threshold:
                group.append(j)
                seen.add(j)

        # Select earliest  Scraped in the group
        earliest_index = df.loc[group]['Scraped'].idxmin()
        to_keep.add(earliest_index)

    logger.info(f"Completed deduplication. Keeping {len(to_keep)} unique jobs out of {len(df)}")
    return df.loc[to_keep].to_dict(orient='records')