# process_data.py
import pandas as pd
from sqlalchemy import create_engine
from config.config import DATABASE_URL, GOOGLE_API_KEY
from nlp.gemini_nlp import setup_gemini, job_analysis, process_json_list
from utils.db_utils import get_engine

def melt_dataframe_columns(df, columns_to_explode):
    """Melt specified columns of a DataFrame, handling lists."""
    if df.empty:
        return pd.DataFrame()
    
    id_vars = [col for col in df.columns if col not in columns_to_explode]
    melted_dfs = []
    
    for col_name in columns_to_explode:
        if col_name in df.columns:
            temp_melted = df.melt(id_vars=id_vars, value_vars=[col_name],
                                  var_name='skill_type', value_name='skill_name')
            temp_melted = temp_melted.dropna(subset=['skill_name'])
            if temp_melted['skill_name'].apply(lambda x: isinstance(x, list)).any():
                temp_melted = temp_melted.explode('skill_name')
            melted_dfs.append(temp_melted)
    
    if not melted_dfs:
        return pd.DataFrame()
    
    return pd.concat(melted_dfs, ignore_index=True)

def process_job_data():
    """Process job data with NLP and store results."""
    engine = get_engine(DATABASE_URL)
    model = setup_gemini(GOOGLE_API_KEY)
    
    # Load job postings
    df = pd.read_sql("SELECT * FROM job_postings_commerce", engine)
    
    # Perform NLP analysis
    analysis_results = [job_analysis(desc, model) for desc in df['Description']]
    cleaned_json_data = process_json_list(analysis_results)
    
    # Merge and melt data
    df_a = pd.DataFrame(cleaned_json_data)
    merged_dataframe = df.merge(df_a, right_index=True, left_index=True)
    
    columns_to_explode = ["technical_skills", "certifications", "behavioral_skills", "languages"]
    final_melted = melt_dataframe_columns(merged_dataframe, columns_to_explode)
    
    # Save to database
    final_melted.to_sql('job_postings_commerce', engine, if_exists='replace', index=False)
    print("Data successfully stored in Neon PostgreSQL!")

if __name__ == "__main__":
    process_job_data()
