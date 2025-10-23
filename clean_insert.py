from DbConnector import DbConnector
import pandas as pd
import ast


def parse_json(x):
    if pd.isna(x) or x == "":
        return []
    try:
        return ast.literal_eval(x)
    except(ValueError, SyntaxError):
        return []

class MoviePipeline:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def clean_movies(self):
        df_movies = pd.read_csv("movies_metadata.csv",
                                engine="python",
                                quotechar='"',
                                escapechar='\\',
                                on_bad_lines='warn')
        # Change budget to numeric (number or NaN values)
        df_movies['budget'] = pd.to_numeric(df_movies['budget'], errors='coerce')
        df_movies['popularity'] = pd.to_numeric(df_movies['popularity'], errors='coerce')
        df_movies['id'] = pd.to_numeric(df_movies['id'], errors='coerce')
        # Drop NaN values, as they should not exist in id,budget and revenue.
        df_movies = df_movies.dropna(subset=['id', 'budget', 'revenue'])
        df_movies[['id', 'budget', 'revenue']] = df_movies[['id', 'budget', 'revenue']].astype('int64')
        # Embedded columns
        cols = [
            "belongs_to_collection", "genres",
            "production_companies", "production_countries",
            "spoken_languages"
        ]
        # Parse it to string to a dictionary.
        for col in cols:
            df_movies[col] = df_movies[col].apply(parse_json)

        # Mapping boolean values
        df_movies['video'] = df_movies['video'].replace('', 'False')
        df_movies['video'] = df_movies['video'].map({'False': False, 'True': True})
        df_movies['video'] = df_movies['video'].fillna(False).astype(bool)
        df_movies['adult'] = df_movies['adult'].map({'False': False, 'True': True})
        df_movies['video'] = df_movies['video'].fillna(False).astype(bool)
        df_movies['release_date'] = pd.to_datetime(df_movies['release_date'], errors='coerce')
        return df_movies

    def clean_links(self):
        df_links = pd.read_csv("links_small.csv",)
        # Drop links that are NaN
        df_links = df_links.dropna(subset=['movieId', 'tmdbId', 'imdbId'])
        # Drop duplicates
        df_links = df_links.drop_duplicates(subset=['movieId'])
        # Cast tmdbId to int, as it previously contained NaN which made it a float.
        df_links['tmdbId'] = df_links['tmdbId'].astype('int64')
        return df_links


    def clean_ratings(self):
        df_ratings = pd.read_csv("ratings_small.csv")
        # Ratings can only be in this set
        valid_ratings = {0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0}
        # check if it is in this sete
        df_ratings = df_ratings[df_ratings['rating'].isin(valid_ratings)]
        df_ratings['timestamp'] = pd.to_datetime(df_ratings['timestamp'], unit='s', errors='coerce')






