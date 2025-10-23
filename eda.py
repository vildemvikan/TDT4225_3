import pandas as pd
import ast

df_movies = pd.read_csv("movies/movies_metadata.csv",
                        engine="python",
                        quotechar='"',
                        escapechar='\\',
                        on_bad_lines='warn')

# df_credits = pd.read_csv("credits.csv")
# df_keywords = pd.read_csv("keywords.csv")
df_links = pd.read_csv("movies/links_small.csv")
df_ratings = pd.read_csv("movies/ratings_small.csv")

def print_dash():
    print('-'*80)

print(df_movies.dtypes)
nan_rows = df_movies[df_movies[['budget', 'revenue', 'id']].isna().any(axis=1)]
df_movies['budget'] = pd.to_numeric(df_movies['budget'], errors='coerce')
df_movies['popularity'] = pd.to_numeric(df_movies['popularity'], errors='coerce')
df_movies['id'] = pd.to_numeric(df_movies['id'], errors='coerce')

df_movies = df_movies.dropna(subset=['id', 'budget', 'revenue'])
df_movies[['id', 'budget', 'revenue']] = df_movies[['id', 'budget', 'revenue']].astype('int64')
print(nan_rows[['adult','id']])
def parse_json(x):
    if pd.isna(x) or x == "":
        return []
    try:
        return ast.literal_eval(x)
    except(ValueError, SyntaxError):
        return []

cols = [
    "belongs_to_collection", "genres",
    "production_companies", "production_countries",
    "spoken_languages"
]

for col in cols:
    df_movies[col] = df_movies[col].apply(parse_json)

df_movies['video'] = df_movies['video'].replace('', 'False')
df_movies['video'] = df_movies['video'].map({'False': False, 'True': True})
df_movies['video'] = df_movies['video'].fillna(False).astype(bool)
df_movies['adult'] = df_movies['adult'].map({'False': False, 'True': True})
df_movies['video'] = df_movies['video'].fillna(False).astype(bool)

print_dash()
print(f"adult is type: {type(df_movies['adult'].iloc[0])}")
print(f"video is type: {type(df_movies['video'].iloc[0])}")
print_dash()
invalid_adult = df_movies[~df_movies['video'].isin([True, False])]
print(len(invalid_adult[['id']]))
df_movies['release_date'] = pd.to_datetime(df_movies['release_date'], errors='coerce')

df_movies = df_movies.drop(['poster_path', 'homepage'], axis=1, errors='ignore')



print(df_movies.dtypes)

print(df_links.dtypes)
print(df_ratings.dtypes)
nan_links = df_links[df_links['tmdbId'].isna()]
nan_time = df_movies[df_movies['release_date'].isna()]
print(len(nan_time))
print(nan_time[['title','id']])
df_links = df_links.dropna(subset=['movieId', 'tmdbId', 'imdbId'])
df_links = df_links.drop_duplicates(subset=['movieId'])
df_links['tmdbId'] = df_links['tmdbId'].astype('int64')
movie = df_movies[df_movies['adult']==True]
print(nan_links)

duplicate_ids = df_movies[df_movies.duplicated(subset='id', keep=False)]['id'].unique()
print(f"Found {len(duplicate_ids)} duplicate movie IDs.")

if len(duplicate_ids) > 0:
    print("\nDuplicate movie IDs:")
    print(duplicate_ids)

    # Show what those duplicates look like (optional)
    for movie_id, group in df_movies[df_movies['id'].isin(duplicate_ids)].groupby('id'):
        print(f"\n--- Before Cleaning: Movie ID {movie_id} ---")
        print(group[['id', 'title', 'vote_count', 'popularity']].to_string(index=False))
df_movies = df_movies.sort_values(
    by=['vote_count', 'popularity'],
    ascending=[False, False]
)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


df_movies = df_movies.drop_duplicates(subset='id', keep='first')

df_movies = df_movies.reset_index(drop=True)
remaining_ids = df_movies[df_movies['id'].isin(duplicate_ids)]['id'].unique()
removed_ids = set(duplicate_ids) - set(remaining_ids)

print("\nuplicates handled successfully.")
print(f"Removed {len(removed_ids)} duplicate entries, retained {len(remaining_ids)} unique IDs.")

if len(remaining_ids) > 0:
    print("\nMovies that had duplicates but were kept:")
    print(df_movies[df_movies['id'].isin(remaining_ids)][['id', 'title', 'vote_count', 'popularity']])



