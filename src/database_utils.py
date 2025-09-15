from sqlalchemy import create_engine
import logging
import os
from dotenv import load_dotenv

# Getting env
load_dotenv()
PROCESSED_DB_PATH = os.getenv('PROCESSED_DB_PATH')

# Creating DB Engine
DB_ENGINE = create_engine(f'sqlite:///{PROCESSED_DB_PATH}')

def load_data_to_db(tables, engine=DB_ENGINE):
    if not tables:
        logging.error("Tables not found")
        return
        
    logging.info("Starting data saving process into database.")
    try:
        for name, df in tables.items():
            if df is not None:
                df.to_sql(name, engine, if_exists='replace', index=False)
                logging.info(f"Table '{name}' saved successfully.")
    except Exception as e:
        logging.error(f"Error during data writing process: {e}")