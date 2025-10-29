from DbConnector import DbConnector
from pprint import pprint
import pandas as pd
import ast
import unicodedata


def parse_json_list(x):
    if pd.isna(x) or x == "":
        return []
    try:
        return ast.literal_eval(x)
    except(ValueError, SyntaxError):
        return []


def parse_json_obj(x):
    if pd.isna(x) or x == "":
        return None
    try:
        v = ast.literal_eval(x)
        return v if isinstance(v, dict) else None
    except (ValueError, SyntaxError):
        return None


def normalize_term(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).strip().lower()
    return " ".join(s.split())


def normalize_id(s: pd.Series) -> pd.Series:
    cleaned = (s.astype('string')
                 .str.strip()
                 .str.lower()
                 .str.replace(r'^tt', '', regex=True)
                 .str.replace(r'\D+', '', regex=True))
    return pd.to_numeric(cleaned, errors='coerce').astype('Int64')


def create_coll(self, collection_name):
    collection = self.db.create_collection(collection_name)
    print('Created collection: ', collection)


class MoviePipeline:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def clean_movies(self):
        df_movies = pd.read_csv("movies/movies_metadata.csv",
                                engine="python",
                                quotechar='"',
                                escapechar='\\',
                                on_bad_lines='warn')

        # Numeric casts
        df_movies['budget'] = pd.to_numeric(df_movies['budget'], errors='coerce')
        df_movies['popularity'] = pd.to_numeric(df_movies['popularity'], errors='coerce')
        df_movies['id'] = pd.to_numeric(df_movies['id'], errors='coerce')

        # Drop NaN in required cols and cast to int
        df_movies = df_movies.dropna(subset=['id', 'budget', 'revenue'])
        df_movies[['id', 'budget', 'revenue']] = df_movies[['id', 'budget', 'revenue']].astype('int64')
        df_movies = df_movies.sort_values(
            by=['vote_count', 'popularity'],
            ascending=[False, False]
        )

        # Drop duplicates based on movie ID, keeping the most recent one
        df_movies = df_movies.drop_duplicates(subset='id', keep='first')
        df_movies = df_movies.reset_index(drop=True)

        # Embedded columns
        array_cols = ["genres", "production_companies", "production_countries","spoken_languages"]
        object_cols = ["belongs_to_collection"]

        for col in array_cols:
            df_movies[col] = df_movies[col].apply(parse_json_list)

        for col in object_cols:
            df_movies[col] = df_movies[col].apply(parse_json_obj)

        def _sanitize_collection(d):
            if not isinstance(d, dict):
                return None
            # Accept if it has an id (number or string) and a non-empty name
            has_id = 'id' in d and d['id'] is not None
            has_name = isinstance(d.get('name'), str) and d['name'].strip() != ""
            return d if has_id and has_name else None

        df_movies['belongs_to_collection'] = df_movies['belongs_to_collection'].apply(_sanitize_collection)

        # Mapping boolean values
        df_movies['video'] = df_movies['video'].replace('', 'False')
        df_movies['video'] = df_movies['video'].map({'False': False, 'True': True})
        df_movies['video'] = df_movies['video'].fillna(False).astype(bool)
        df_movies['adult'] = df_movies['adult'].map({'False': False, 'True': True})
        df_movies['video'] = df_movies['video'].fillna(False).astype(bool)
        df_movies['release_date'] = pd.to_datetime(df_movies['release_date'], errors='coerce')
        df_movies['release_date'] = df_movies['release_date'].replace({pd.NaT: None})
        df_movies = df_movies.drop(['poster_path', 'homepage'], axis=1, errors='ignore')

        # Normalize imdbId to only contain numbers
        df_movies['imdb_id'] = normalize_id(df_movies['imdb_id'])

        df_movies = df_movies.rename(columns={'id': 'tmdbId', 'imdb_id': 'imdbId'})

        return df_movies

    def clean_links(self):
        df_links = pd.read_csv("movies/links.csv", )
        # Drop links that are NaN
        df_links = df_links.dropna(subset=['movieId', 'tmdbId', 'imdbId'])
        # Drop duplicates
        df_links = df_links.drop_duplicates(subset=['movieId'])
        # Cast tmdbId to int, as it previously contained NaN which made it a float.
        df_links['tmdbId'] = df_links['tmdbId'].astype('int64')
        return df_links

    def clean_ratings(self):
        df_ratings = pd.read_csv("movies/ratings.csv")
        # Ratings can only be in this set
        valid_ratings = {0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0}
        # check if it is in this sete
        df_ratings = df_ratings[df_ratings['rating'].isin(valid_ratings)]
        df_ratings['timestamp'] = pd.to_datetime(df_ratings['timestamp'], errors='coerce')
        return df_ratings

    def clean_credits(self):
        import pandas as pd
        df_credits = pd.read_csv("movies/credits.csv")

        # Remove entries with no id
        df_credits["id"] = pd.to_numeric(df_credits["id"], errors="coerce").astype("Int64")
        df_credits = df_credits.dropna(subset=["id"]).copy()

        # Convert list strings to actual lists (json objects)
        df_credits["cast"] = df_credits["cast"].apply(parse_json_list)
        df_credits["crew"] = df_credits["crew"].apply(parse_json_list)

        # --- CREW ---
        ex_crew = df_credits.set_index('id')['crew'].explode().rename('crew_dict')
        df_crew = ex_crew.to_frame()
        df_crew = df_crew[df_crew['crew_dict'].apply(lambda x: isinstance(x, dict))]
        df_crew_normalized = pd.json_normalize(df_crew['crew_dict'])
        df_crew_normalized['tmdbId'] = df_crew.index.values

        # --- CAST ---
        ex_cast = df_credits.set_index('id')['cast'].explode().rename('cast_dict')
        df_cast = ex_cast.to_frame()
        df_cast = df_cast[df_cast['cast_dict'].apply(lambda x: isinstance(x, dict))]
        df_cast_normalized = pd.json_normalize(df_cast['cast_dict'])
        df_cast_normalized['tmdbId'] = df_cast.index.values

        # Select and rename columns
        df_crew = df_crew_normalized.rename(columns={"id": "person_id"})
        df_crew = df_crew[["tmdbId", "person_id", "name", "gender", "department", "job"]].copy()

        df_cast = df_cast_normalized.rename(columns={"id": "person_id"})
        df_cast = df_cast[["tmdbId", "person_id", "name", "gender", "character", "order"]].copy()

        # Use 'Int64' (nullable integer) for IDs and numeric values
        for col in ["person_id", "gender"]:
            df_crew[col] = pd.to_numeric(df_crew[col], errors="coerce").astype("Int64")
            df_cast[col] = pd.to_numeric(df_cast[col], errors="coerce").astype("Int64")

        df_cast["order"] = pd.to_numeric(df_cast["order"], errors="coerce").astype("Int64")

        # Deduplication is performed on the primary movie/person/role key
        cast_subset = ["tmdbId", "person_id", "character"]
        crew_subset = ["tmdbId", "person_id", "job", "department"]

        # Count all duplicates
        cast_duplicates = df_cast.duplicated(subset=cast_subset, keep=False).sum() // 2
        crew_duplicates = df_crew.duplicated(subset=crew_subset, keep=False).sum() // 2

        print(f"Number of cast duplicates: {cast_duplicates}")
        print(f"Number of crew duplicates: {crew_duplicates}")

        # Remove all duplicates (keep first)
        before_cast = len(df_cast)
        df_cast = df_cast.drop_duplicates(subset=cast_subset, keep="first").reset_index(drop=True)
        removed_cast = before_cast - len(df_cast)
        print(f"Removed {removed_cast} cast entries. {len(df_cast)} remain.")

        before_crew = len(df_crew)
        df_crew = df_crew.drop_duplicates(subset=crew_subset, keep="first").reset_index(drop=True)
        removed_crew = before_crew - len(df_crew)
        print(f"Removed {removed_crew} crew entries. {len(df_crew)} remain.")

        # --- CORRECTED GROUPING LOGIC ---

        # Helper function to create the desired dict from a row
        def create_cast_dict(row):
            return {
                'id': row['person_id'],
                'name': row['name'],
                'gender': row['gender'],
                'character': row['character'],
                'order': row['order']
            }

        def create_crew_dict(row):
            return {
                'id': row['person_id'],
                'name': row['name'],
                'gender': row['gender'],
                'department': row['department'],
                'job': row['job']
            }

        # Apply the helper functions to create a dictionary column for each person
        df_cast['cast_member'] = df_cast.apply(create_cast_dict, axis=1)
        df_crew['crew_member'] = df_crew.apply(create_crew_dict, axis=1)

        # Group the dictionary lists
        # Group the dictionary columns (cast_member/crew_member) into a list for each movie
        cast_grouped = df_cast.groupby("tmdbId")['cast_member'].apply(list).reset_index(name="cast")
        crew_grouped = df_crew.groupby("tmdbId")['crew_member'].apply(list).reset_index(name="crew")

        # Final merge
        credits_df = cast_grouped.merge(crew_grouped, on="tmdbId", how="outer")

        # Ensure missing groups become empty lists
        for col in ["cast", "crew"]:
            credits_df[col] = credits_df[col].apply(lambda v: v if isinstance(v, list) else [])

        return credits_df

    def clean_keywords(self):
        df_keywords = pd.read_csv("movies/keywords.csv")

        df_keywords["keywords"] = df_keywords["keywords"].apply(parse_json_list)

        ex_keywords = (
            df_keywords[["id", "keywords"]]
            .rename(columns={"id": "tmdbId"})
            .explode("keywords", ignore_index=True)
        )

        # Keep only dicts, drop the rest early
        ex_keywords["keywords"] = ex_keywords["keywords"].apply(lambda x: x if isinstance(x, dict) else None)
        ex_keywords = ex_keywords.dropna(subset=["keywords"]).copy()

        # Normalize dicts to columns
        df_keywords = pd.json_normalize(ex_keywords["keywords"])
        df_keywords["tmdbId"] = ex_keywords["tmdbId"].values

        # Select columns to keep
        df_keywords = df_keywords[["tmdbId", "id", "name"]]

        # Types and validation
        df_keywords["id"] = pd.to_numeric(df_keywords["id"], errors="coerce").astype("Int64")
        df_keywords["name"] = df_keywords["name"].astype("string")

        # Removing duplicates, defining dedupe keys
        keywords_subset = ["tmdbId", "id", "name"]

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
            .groupby("tmdbId", dropna=False)["name"]
            .apply(lambda s: s.drop_duplicates().tolist())  # preserves first occurrence
            .reset_index(name="keywords")
        )

        df_keywords = keywords_grouped
        return df_keywords

    def merge_movies_and_links(self, df_movies, df_links):
        df_movies = df_movies.merge(df_links, on=['imdbId', 'tmdbId'], how='left')
        df_movies["movieId"] = df_movies["movieId"].astype("Int64")

        return df_movies

    def merge_keywords(self, df_movies, df_links):
        df_movies = df_movies.merge(df_links, on=['tmdbId'], how='left')
        return df_movies

    def build_user_stats(self, df_movies, df_ratings):
        """
        Compute per-user rating statistics and genre diversity using pandas.
        """
        # 1) Keep only required columns
        df_movies = df_movies[["movieId", "genres"]].copy()
        df_ratings = df_ratings[["userId", "movieId", "rating"]].copy()

        # 2)  Merge ratings and genres
        merged = df_ratings.merge(df_movies, on="movieId", how="left")

        # Clean genres into name lists
        merged["genres"] = merged["genres"].apply(
            lambda g: [d.get("name") for d in g if isinstance(d, dict) and "name" in d]
            if isinstance(g, list) else []
        )

        # 3) Compute per-user stats
        user_stats = (
            merged.groupby("userId")["rating"]
            .agg(["count", "mean", "var"])
            .rename(columns={"count": "ratingCount", "mean": "ratingMean", "var": "ratingVariance"})
            .reset_index()
        )

        # 4) Count distinct genres
        exploded = merged.explode("genres", ignore_index=True)
        genre_counts = (
            exploded.dropna(subset=["genres"])
            .groupby("userId")["genres"]
            .nunique()
            .reset_index(name="distinctGenres")
        )

        # 5) Merge both sets of stats
        df_users = user_stats.merge(
            genre_counts, on="userId", how="left").fillna({"distinctGenres": 0})
        df_users = df_users[user_stats["ratingCount"] >= 20].reset_index(drop=True)

        # 6) Round values
        df_users["ratingVariance"] = df_users["ratingVariance"].round(3)
        df_users["ratingMean"] = df_users["ratingMean"].round(2)
        df_users["distinctGenres"] = df_users["distinctGenres"].astype(int)

        return df_users


    def create_coll(self, collection_name):
        existing = self.db.list_collection_names()

        if collection_name not in existing:
            collection = self.db.create_collection(collection_name)
            print('Created collection: ', collection)
        else:
            print('Collection already exists: ', collection_name)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def insert_documents(self, collection_name, df, chunk_size=100000):
        collection = self.db[collection_name]

        total_inserted = 0
        total_rows = len(df)

        for i in range(0, total_rows, chunk_size):
            df_chunk = df.iloc[i:i + chunk_size]
            documents = df_chunk.to_dict("records")

            try:
                collection.insert_many(documents)
                total_inserted += len(documents)
                print(
                    f" -> Inserted batch {i // chunk_size + 1} to {collection_name}. Total documents inserted: {total_inserted}/{total_rows}")
            except Exception as e:
                print(f"Warning: Failed to insert batch starting at index {i}. Error: {e}")

    def show_coll(self):
        collections = self.client['movie_db'].list_collection_names()
        print(collections)


def main():
    program = None
    try:
        program = MoviePipeline()

        # program.drop_coll(collection_name="Movie")
        program.drop_coll(collection_name="Credits")
        # program.drop_coll(collection_name="Ratings")
        # program.drop_coll(collection_name="Users")

        # program.create_coll(collection_name="Movie")
        # program.create_coll(collection_name="Credits")
        # program.create_coll(collection_name="Ratings")
        # program.create_coll(collection_name="Users")

        # df_movies = program.clean_movies()
        # df_links = program.clean_links()
        df_credits = program.clean_credits()
        # df_ratings = program.clean_ratings()
        # df_keywords = program.clean_keywords()

        # df_movies = program.merge_movies_and_links(df_movies, df_links)
        # df_movies = program.merge_keywords(df_movies, df_keywords)

        # df_users = program.build_user_stats(df_movies, df_ratings)

        # program.insert_documents("Movie", df_movies)
        # program.insert_documents("Credits", df_credits)
        # program.insert_documents("Ratings", df_ratings)
        program.insert_documents("Credits", df_credits)

        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()










