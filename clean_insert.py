from DbConnector import DbConnector
import pandas as pd
import ast
import unicodedata


def parse_json(x):
    if pd.isna(x) or x == "":
        return []
    try:
        return ast.literal_eval(x)
    except(ValueError, SyntaxError):
        return []


def normalize_term(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).strip().lower()
    return " ".join(s.split())


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
        return df_ratings

    def clean_credits(self):
        df_credits = pd.read_csv("movies/credits.csv")

        df_credits["cast"] = df_credits["cast"].apply(parse_json)
        df_credits["crew"] = df_credits["crew"].apply(parse_json)

        ex_crew = (
            df_credits[["id", "crew"]]
            .rename(columns={"id": "movie_id"})
            .explode("crew", ignore_index=True)
        )

        ex_cast = (
            df_credits[["id", "cast"]]
            .rename(columns={"id": "movie_id"})
            .explode("cast", ignore_index=True)
        )

        # Keep only dicts, drop the rest early
        ex_crew["crew"] = ex_crew["crew"].apply(lambda x: x if isinstance(x, dict) else None)
        ex_crew = ex_crew.dropna(subset=["crew"]).copy()

        ex_cast["cast"] = ex_cast["cast"].apply(lambda x: x if isinstance(x, dict) else None)
        ex_cast= ex_cast.dropna(subset=["cast"]).copy()

        # Normalize dicts to columns
        df_crew = pd.json_normalize(ex_crew["crew"])
        df_crew["movie_id"] = ex_crew["movie_id"].values

        df_cast = pd.json_normalize(ex_cast["cast"])
        df_cast["movie_id"] = ex_cast["movie_id"]

        # Select columns to keep
        df_crew = df_crew[["movie_id", "id", "name", "gender", "department", "job"]]

        df_cast = df_cast[["movie_id", "id", "name", "gender", "character", "order"]]

        # Types and validation
        df_crew["id"] = pd.to_numeric(df_crew["id"], errors="coerce").astype("Int64")
        df_crew["gender"] = pd.to_numeric(df_crew["gender"], errors="coerce").astype("Int64")
        df_crew["name"] = df_crew["name"].astype("string")
        df_crew["department"] = df_crew["department"].astype("string")
        df_crew["job"] = df_crew["job"].astype("string")

        df_cast["id"] = pd.to_numeric(df_cast["id"], errors="coerce").astype("Int64")
        df_cast["gender"] = pd.to_numeric(df_cast["gender"], errors="coerce").astype("Int64")
        df_cast["order"] = pd.to_numeric(df_cast["order"], errors="coerce").astype("Int64")
        df_cast["name"] = df_cast["name"].astype("string")
        df_cast["character"] = df_cast["character"].astype("string")

        # Removing duplicates, defining dedupe keys
        cast_subset = ["movie_id", "id", "character"]
        crew_subset = ["movie_id", "id", "job", "department"]

        # Count number of duplicates (extra copies beyond the first)
        cast_duplicates = df_cast[cast_subset].dropna().duplicated().sum()
        crew_duplicates = df_crew[crew_subset].dropna().duplicated().sum()

        print(f"Number of cast duplicates: {cast_duplicates}")
        print(f"Number of crew duplicates: {crew_duplicates}")

        # Removing duplicates (keep first)
        if cast_duplicates:
            before_cast = len(df_cast)
            df_cast = df_cast.drop_duplicates(subset=cast_subset, keep="first").reset_index(drop=True)
            removed_cast = before_cast - len(df_cast)
            print(f"Removed {removed_cast} cast entries. {len(df_cast)} remain.")

        if crew_duplicates:
            before_crew = len(df_crew)
            df_crew = df_crew.drop_duplicates(subset=crew_subset, keep="first").reset_index(drop=True)
            removed_crew = before_crew - len(df_crew)
            print(f"Removed {removed_crew} crew entries. {len(df_crew)} remain.")

        cast_grouped = (
            df_cast
            .groupby("movie_id", dropna=False)[["id", "name", "gender", "character", "order"]]
            .apply(lambda g: g.dropna(how="all").to_dict("records"))
            .reset_index(name="cast")
        )

        crew_grouped = (
            df_crew
            .groupby("movie_id", dropna=False)[["id", "name", "gender", "department", "job"]]
            .apply(lambda g: g.dropna(how="all").to_dict("records"))
            .reset_index(name="crew")
        )

        credits_df = cast_grouped.merge(crew_grouped, on="movie_id", how="outer")
        credits_df = credits_df.astype({"cast": "object", "crew": "object"})

        for col in ["cast", "crew"]:
            credits_df[col] = credits_df[col].apply(lambda v: v if isinstance(v, list) else [])

        return credits_df

    def clean_keywords(self):
        df_keywords = pd.read_csv("movies/keywords.csv")

        df_keywords["keywords"] = df_keywords["keywords"].apply(parse_json)

        ex_keywords = (
            df_keywords[["id", "keywords"]]
            .rename(columns={"id": "movie_id"})
            .explode("keywords", ignore_index=True)
        )

        # Keep only dicts, drop the rest early
        ex_keywords["keywords"] = ex_keywords["keywords"].apply(lambda x: x if isinstance(x, dict) else None)
        ex_keywords = ex_keywords.dropna(subset=["keywords"]).copy()

        # Normalize dicts to columns
        df_keywords = pd.json_normalize(ex_keywords["keywords"])
        df_keywords["movie_id"] = ex_keywords["movie_id"].values

        # Select columns to keep
        df_keywords = df_keywords[["movie_id", "id", "name"]]

        # Types and validation
        df_keywords["id"] = pd.to_numeric(df_keywords["id"], errors="coerce").astype("Int64")
        df_keywords["name"] = df_keywords["name"].astype("string")

        # Removing duplicates, defining dedupe keys
        keywords_subset = ["movie_id", "id", "name"]

        # Count number of duplicates (extra copies beyond the first)
        keywords_duplicates = df_keywords[keywords_subset].dropna().duplicated().sum()
        print(f"Number of keyword duplicates: {keywords_duplicates}")

        # Removing duplicates (keep first)
        if keywords_duplicates:
            before_keywords = len(df_keywords)
            keywords_df = df_keywords.drop_duplicates(subset=keywords_subset, keep="first").reset_index(drop=True)
            removed_keywords = before_keywords - len(keywords_df)
            print(f"Removed {removed_keywords} keyword entries. {len(keywords_df)} remain.")

        # Grouping keywords by movie_id to list of strings
        keywords_grouped = (
            df_keywords
            .dropna(subset=["name"])
            .assign(name=lambda d: d["name"].map(lambda x: normalize_term(str(x))))
            .groupby("movie_id", dropna=False)["name"]
            .apply(lambda s: s.drop_duplicates().tolist())  # preserves first occurrence
            .reset_index(name="keywords")
        )

        df_keywords = keywords_grouped
        return df_keywords







