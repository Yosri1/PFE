import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from langdetect import detect
from nltk.corpus import stopwords
import nltk
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Download stopwords if not already downloaded
nltk.download('stopwords')

def detect_language(text):
    try:
        return detect(text)
    except:
        return 'unknown'

def deduplicate_jobs_by_description(job_data, similarity_threshold=0.92):
    """
    Removes duplicate job descriptions using cosine similarity, grouped by language.
    Keeps the job with the earliest 'Scraped' date.

    Parameters:
    - job_data (list of dict): List of job postings, each with 'Description' and 'Scraped'.
    - similarity_threshold (float): Threshold above which descriptions are considered duplicates.

    Returns:
    - List of deduplicated job dicts.
    """
    logger.info("Starting job deduplication process")

    # Convert to DataFrame
    df = pd.DataFrame(job_data)
    
    if 'Description' not in df.columns or 'Scraped' not in df.columns:
        logger.error("Missing required columns: 'Description' and 'Scraped'")
        raise ValueError("Each job dict must contain 'Description' and 'Scraped' keys.")

    # Parse date
    logger.info("Parsing 'Scraped' column")
    df['Scraped'] = pd.to_datetime(df['Scraped'], errors='coerce')

    # Detect language
    logger.info("Detecting language of job descriptions")
    df['lang'] = df['Description'].fillna("").apply(detect_language)

    # Prepare output
    to_keep = set()

    for lang_code, group in df.groupby('lang'):
        logger.info(f"Processing language group: {lang_code} with {len(group)} entries")

        if lang_code not in ['en', 'fr']:
            logger.warning(f"Skipping unsupported language: {lang_code}")
            continue

        stop_words = stopwords.words('english') if lang_code == 'en' else stopwords.words('french')

        vectorizer = TfidfVectorizer(stop_words=stop_words)
        tfidf_matrix = vectorizer.fit_transform(group['Description'].fillna(""))

        similarity_matrix = cosine_similarity(tfidf_matrix)

        local_seen = set()
        group_indices = group.index.tolist()

        for i, idx_i in enumerate(group_indices):
            if idx_i in local_seen:
                continue

            dup_group = [idx_i]

            for j in range(i + 1, len(group_indices)):
                idx_j = group_indices[j]
                if similarity_matrix[i][j] >= similarity_threshold:
                    dup_group.append(idx_j)
                    local_seen.add(idx_j)

            earliest_index = df.loc[dup_group]['Scraped'].idxmin()
            to_keep.add(earliest_index)

    logger.info(f"Completed deduplication. Keeping {len(to_keep)} unique jobs out of {len(df)}")
    return df.loc[list(to_keep)].to_dict(orient='records')

