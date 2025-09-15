import requests
import pandas as pd
import logging
import time

def get_movie_details(title, api_key):
    url = f"http://www.omdbapi.com/?t={title}&apikey={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status() # Get status to catch an error early
        data = response.json()
        if data.get('Response') == 'True':
            return {
                'movie_id': data.get('imdbID'),
                'title': data.get('Title'),
                'genre': data.get('Genre'),
                'director': data.get('Director'),
                'release_date': pd.to_datetime(data.get('Released'), errors='coerce'),
                'runtime_minutes': pd.to_numeric(str(data.get('Runtime', '')).replace(' min', ''), errors='coerce'),
                'imdb_rating': pd.to_numeric(data.get('imdbRating'), errors='coerce')
            }
    except requests.RequestException as e:
        logging.warning(f"API error for title '{title}': {e}")
    except Exception as e:
        logging.error(e)
    time.sleep(0.4) # Small pause for api requests
    return None