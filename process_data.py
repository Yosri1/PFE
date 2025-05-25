import pandas as pd
from config.config import DATABASE_URL, GOOGLE_API_KEY
from LLM.gemini_nlp import setup_gemini, job_analysis, process_json_list
from utils.db_utils import get_engine
import time

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

def save_batch_data(merged_df, melted_df, engine, batch_number, merged_table='merged_data_processed', melted_table='melted_data_processed'):
    """Save merged and melted data for a batch to the database."""
    try:
        # Save merged data
        if not merged_df.empty:
            print(f"Saving merged batch {batch_number} to database...")
            merged_df.to_sql(merged_table, engine, if_exists='append', index=False)
            print(f"Merged batch {batch_number} saved to '{merged_table}' table.")
        
        # Save melted data
        if not melted_df.empty:
            print(f"Saving melted batch {batch_number} to database...")
            melted_df.to_sql(melted_table, engine, if_exists='append', index=False)
            print(f"Melted batch {batch_number} saved to '{melted_table}' table.")
    except Exception as e:
        print(f"Error saving batch {batch_number}: {str(e)}")
        import traceback
        print(traceback.format_exc())

def process_job_data(batch_size=100):
    """Process job data with NLP and store results in batches."""
    try:
        engine = get_engine(DATABASE_URL)
        model = setup_gemini(GOOGLE_API_KEY)
        
        # Load job postings using SQLAlchemy engine
        df = pd.read_sql_table('job_postings', engine)
        
        if df.empty:
            print("No job postings found in database!")
            return
        
        # Process data in batches
        total_rows = len(df)
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_df = df.iloc[batch_start:batch_end]
            batch_number = batch_start // batch_size + 1
            
            print(f"Processing batch {batch_number} (rows {batch_start} to {batch_end-1})...")
            
            # Perform NLP analysis with rate limiting
            analysis_results = []
            for desc in batch_df['Description']:
                try:
                    result = job_analysis(desc, model)
                    analysis_results.append(result)
                    time.sleep(3)  # Rate limiting between API calls
                except Exception as e:
                    print(f"Error analyzing job description: {str(e)}")
                    analysis_results.append(None)
            
            cleaned_json_data = process_json_list([r for r in analysis_results if r is not None])
            
            # Merge and melt data
            df_a = pd.DataFrame(cleaned_json_data)
            if df_a.empty:
                print(f"No valid analysis results for batch {batch_number}!")
                continue
                
            merged_dataframe = batch_df.reset_index(drop=True).merge(df_a, right_index=True, left_index=True)
            
            columns_to_explode = ["technical_skills", "certifications", "behavioral_skills", "languages"]
            final_melted = melt_dataframe_columns(merged_dataframe, columns_to_explode)
            
            # Save batch data
            save_batch_data(merged_dataframe, final_melted, engine, batch_number)
            
    except Exception as e:
        print(f"An error occurred during processing: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    process_job_data(batch_size=100)