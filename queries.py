from unittest import result

from DbConnector import DbConnector
from pprint import pprint

def print_results(results, order=None, round_floats=2, title=None):
    """Print query result so we can have consistent ordering cause that bothered me

    Args:
        results (list): list of dictionaries representing query results
        order (list): list of strings representing the ordering of the results
        round_floats (int): number of decimal places to round to
        title (str): optional title to display on table
    """

    if title:
        print("\n"+ "=" * len(title))
        print(title)
        print("=" * len(title))

    for r in results:
        # we enforce order if specified
        if order:
            ordered = {}
            for key in order:
                if key in r:
                    ordered[key] = r[key]
            for k,v in r.items():
                # if a field from result is not in the ordered that the user input, then just set field to standard name
                if k not in ordered:
                    ordered[k] = v
        else:
            ordered = dict(r)

        for k,v in ordered.items():
            if isinstance(v, float):
                ordered[k] = round(v, round_floats)

        print(ordered)

class QueryTasks:
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db



    def query1(self):
        """Top 10 directors with greater than 5 movies by median revenue"""
        pipeline = [
                # using unwind to so each job is own document
                {"$unwind": "$crew"},

                {"$match": {"crew.job": "Director"}},

                {
                    "$lookup": {
                        "from": "Movie",
                        "localField": "tmdbId",
                        "foreignField": "tmdbId",
                        "as": "movie_info"
                    }
                },

                # after joining, we flatten movie_info too
                {"$unwind": "$movie_info"},

                {
                    "$group": {
                        "_id": "$crew.name",
                        "movie_count": {"$sum": 1},
                        "median_revenue": {
                            "$median": {
                                "input": "$movie_info.revenue",
                                "method": "approximate"
                            }
                        },
                        "avg_vote": {"$avg": "$movie_info.vote_average"},
                    }
                },

                # finding directors with greater than 5 movies
                {"$match": {"movie_count": {"$gt": 5}}},
                {"$sort": {"median_revenue": -1}},
                {"$limit": 10},

                {
                    "$project": {
                        "director": "$_id",
                        "movie_count": 1,
                        "avg_vote": 1,
                        "median_revenue": 1,
                        "_id": 0
                    }
                }
            ]
        return list(self.db.Credits.aggregate(pipeline, allowDiskUse=True))



    def query3(self):
        """Top 10 actors with more than 10 movies with widest genre batch"""
        pipeline=[
            {"$unwind": "$cast"},
            {
                "$lookup": {
                   "from": "Movie",
                   "localField": "tmdbId",
                   "foreignField": "tmdbId",
                   "as": "movie_info"
                }
            },
            {"$unwind": "$movie_info"},
            {"$unwind": "$movie_info.genres"},

            {
                "$group": {
                    "_id": "$cast.name",
                    "genres": {"$addToSet": "$movie_info.genres.name"},
                    "movies": {"$addToSet": "$tmdbId"}
                }
            },

            {
                "$project": {
                    "actor": "$_id",
                    "genre_count": {"$size": "$genres"},
                    "movie_count": {"$size": "$movies"},
                    "examples_genre": {"$slice": ["$genres", 5]},
                    "_id": 0
                }
            },

            {"$match": {"movie_count": {"$gt": 10}}},
            {"$sort": {"genre_count": -1, "movie_count": -1}},
            {"$limit": 10}
                ]
        results = list(self.db.Credits.aggregate(pipeline, allowDiskUse=True))
        return results
    def query5(self):
        """By decade and primary genre (first element in genres), find median runtime and movie count"""
        pipeline=[
            {
                "$match": {
                    "runtime": {"$ne": None},
                    "release_date": {"$ne": None},
                    "genres": {"$ne": []},
                }
            },

            {
                "$addFields": {
                    "year": {"$year": "$release_date"},
                    "decade": {
                        "$multiply": [
                            {"$floor": {"$divide": [{"$year": "$release_date"}, 10]}},
                            10
                        ]
                    },
                    "primary_genre": {"$arrayElemAt": ["$genres.name", 0]}
                }
            },
            {
                "$group": {
                    "_id": {"decade": "$decade", "genre": "$primary_genre"},
                    "runtimes": {"$push": "$runtime"},
                    "movie_count": {"$sum": 1}
                }
            },
            {
                "$set": {
                    "median_runtime": {
                        "$median": {"input": "$runtimes", "method": "approximate"}
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "decade": "$_id.decade",
                    "primary_genre": "$_id.genre",
                    "median_runtime": 1,
                    "movie_count": 1,
                }
            },
            {"$sort": {"decade": 1, "median_runtime": -1}},
        ]
        return list(self.db.Movie.aggregate(pipeline, allowDiskUse=True))
    def query7(self):
        """Top 20 neo-noir or noir movies by vote_average (have to have more than 50 votes)"""

        pipeline=[
            # use text search from mongodb (remember to check if indexes exists before running this)
            {"$match": {"$text": {"$search": "noir"}}},
            {"$match": {"vote_count": {"$gte": 50}}},

            {"$addFields": {"year": {"$year": "$release_date"}}},
            {"$sort": {"vote_average": -1, "vote_count": -1}},

            {"$limit": 20},

            {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "year": 1,
                    "vote_average": 1,
                    "vote_count": 1
                }
            }
        ]
        return list(self.db.Movie.aggregate(pipeline, allowDiskUse=True))

    def query9(self):
        pipeline=[
            {
                "$match": {
                    "original_language": {"$ne": "en"},
                    "$or": [
                        {"production_companies.name": "United States"},
                        {"production_companies.name": "United States of America"},
                        {"production_countries.name": "United States"},
                        {"production_countries.name": "United States of America"}
                    ]
                }
            },

            {
                "$group": {
                    "_id": "$original_language",
                    "count": {"$sum": 1},
                    "example_title": {"$first": "$title"},
                }
            },

            {"$sort": {"count": -1}},

            {"$limit": 10},

            {
                "$project": {
                    "_id": 0,
                    "original_language": "$_id",
                    "count": 1,
                    "example_title": 1
                }
            }
        ]

        return list(self.db.Movie.aggregate(pipeline, allowDiskUse=True))





def main():
    q = QueryTasks()
    q1 = q.query1()
    print_results(
        q1,
        order=["director", "movie_count", "avg_vote", "median_revenue"],
        title="Top 10 directors with more than 5 movies with highest median revenue"
    )
    del q1
    q3 = q.query3()
    print_results(
        q3,
        order=["actor", "genre_count", "movie_count", "examples_genre"],
        title="Top 10 actors with more than 10 movies with widest genre"
    )
    del q3
    q5 = q.query5()
    print_results(
        q5,
        order=["decade", "primary_genre", "median_runtime", "movie_count"],
        title="Top genre and median runtime of movies summarized by decade"
    )
    del q5
    q7 = q.query7()
    print_results(
        q7,
        order=["title", "year", "vote_average", "vote_count"],
        title="Top 20 movies with neo-noir or noir in overview or tag, sorted by vote avg."
    )
    q9 = q.query9()
    print_results(
        q9,
        order=["original_language", "count", "example_title"],
        title="Top 10 Non-English movies that are produced by american company or in america"
    )
if __name__ == "__main__":
    main()