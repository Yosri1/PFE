# process_data.py
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

def process_job_data():
    """Process job data with NLP and store results."""
    try:
        engine = get_engine(DATABASE_URL)
        model = setup_gemini(GOOGLE_API_KEY)
        
        # Load job postings using SQLAlchemy engine
        df = pd.read_sql_table('job_postings', engine)
        
        if df.empty:
            print("No job postings found in database!")
            return
        
        # Perform NLP analysis with rate limiting
        analysis_results = []
        for desc in df['Description']:
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
            print("No valid analysis results to process!")
            return
            
        merged_dataframe = df.merge(df_a, right_index=True, left_index=True)
        # Save merged data to SQL table
        print("Saving merged data to database...")
        merged_dataframe.to_sql('merged_data_processed', engine, if_exists='replace', index=False)
        print("Merged data saved to 'merged_data_processed' table.")
        
        columns_to_explode = ["technical_skills", "certifications", "behavioral_skills", "languages"]
        final_melted = melt_dataframe_columns(merged_dataframe, columns_to_explode)
        
        if not final_melted.empty:
            # Save to database

           # Save melted data to SQL table
            print("Saving melted data to database...")
            final_melted.to_sql('melted_data_processed', engine, if_exists='replace', index=False)
            print("Melted data saved to 'melted_data_processed' table.")
        else:
            print("No data to save after processing!")
            
    except Exception as e:
        print(f"An error occurred during processing: {str(e)}")
        import traceback
        print(traceback.format_exc())  # This will print the full error traceback

if __name__ == "__main__":
    process_job_data()

