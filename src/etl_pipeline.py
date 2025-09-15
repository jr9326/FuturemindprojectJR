import pandas as pd
import os
import argparse
import logging
from dotenv import load_dotenv
from utils import get_movie_details
from database_utils import load_data_to_db

# logger formatting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# reads .env
load_dotenv()
OMDB_API_KEY = os.getenv('OMDB_API_KEY')
REVENUE_FILE_PATH = os.getenv('REVENUES_CSV_PATH')

def extract_and_clean_data(path, limit=0):
    """
    Reads movie revenue data from a CSV file, cleans it, and performs initial preprocessing.

    Args:
        path (str): The file path to the CSV data.
        limit (int, optional): The maximum number of rows to read from the CSV. 
                               Defaults to 0, which means all rows are read.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the cleaned and preprocessed data.
                      Returns None if the file is not found or an error occurs.
    """
    try:
        logging.info("Reading data")
        # During testing I've used limit set on only 200 due to API limitations. Hope it is not a problem.
        if limit > 0:
            df = pd.read_csv(path).head(limit)
        else:
            df = pd.read_csv(path)
        
        logging.info("Transform data")
        df['date'] = pd.to_datetime(df['date'])
        df['theaters'] = df['theaters'].astype('Int64')
        df['distributor'] = df['distributor'].replace("-", None)
        df['distributor'] = df['distributor'].astype('category')
        
        logging.info("Data read and preprocessed successfully.")
        return df
    except FileNotFoundError:
        logging.error(f"File wasn't found in - {path}")
        return None
    except Exception as e:
        logging.error(e)


def transform_data(df, api_key):
    """
    Transforms the raw DataFrame into dimension and fact tables.

    This function orchestrates the creation of the movie dimension, date dimension,
    and revenue fact tables.

    Args:
        df (pd.DataFrame): The cleaned DataFrame from the extraction step.
        api_key (str): The API key for fetching movie details.

    Returns:
        tuple: A tuple containing three pandas DataFrames:
               (dim_movie, dim_date, fact_revenue).
               Returns None if an error occurs.
    """
    try:
        logging.info("Data transform started.")
        dim_movie = _create_dim_movie(df, api_key)

        dim_date = _create_dim_date(df)

        fact_revenue = _get_fact_table(df, dim_movie)

        logging.info("Data transform ended.")
        return dim_movie, dim_date, fact_revenue
    
    except Exception as e:
        logging.error(e)

def _create_dim_movie(df: pd.DataFrame, api_key: str) -> pd.DataFrame:
    # Creting dim_movie movie
    unique_titles = df['title'].unique()
    movie_details_list = []
    for title in unique_titles:
        details = get_movie_details(title, api_key)
        if details:
            movie_details_list.append(details)
    dim_movie = pd.DataFrame(movie_details_list)
    dim_movie.dropna(subset=['movie_id'], inplace=True) # Removes titles without movie_id value

    return dim_movie

def _create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    # Creating dim_date
    unique_dates = df['date'].unique()
    dim_date = pd.DataFrame({'date': unique_dates})
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['day_of_week'] = dim_date['date'].dt.dayofweek

    return dim_date

def _get_fact_table(df: pd.DataFrame, dim_movie: pd.DataFrame) -> pd.DataFrame:
    # Creating fact_revenue to have completed data about the movies
    fact_revenue = pd.merge(df, dim_movie[['title', 'movie_id']], on='title', how='inner')
    fact_revenue = fact_revenue[['id', 'movie_id', 'date', 'revenue']]
    fact_revenue.rename(columns={'id': 'revenue_id', 'revenue': 'revenue_amount'}, inplace=True)

    return fact_revenue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL process for movie revenue data.")
    parser.add_argument(
        '--limit', 
        type=int, 
        default=200, 
        help='Number of rows to process from the source revenues_per_day file.'
    )
    args = parser.parse_args()

    logging.info("====== STARTS ETL PROCESS ======")
    
    # Extract & Clean data
    revenue_df_from_csv = extract_and_clean_data(path=REVENUE_FILE_PATH, limit=args.limit)
    
    # Transform data
    dim_movie, dim_date, fact_revenue = transform_data(df=revenue_df_from_csv, api_key=OMDB_API_KEY)
    
    # Load tables into db
    tables_to_load = {
        'DimMovie': dim_movie,
        'DimDate': dim_date,
        'FactRevenue': fact_revenue
    }
    load_data_to_db(tables=tables_to_load)
    
    logging.info("====== ETL PROCESS FINISHED ======")