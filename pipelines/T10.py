t10A_pipeline = [

    # Select columns
    {"$project": {"_id": 0,
                  "userId": 1,
                  "ratingCount": 1,
                  "ratingMean": 1,
                  "ratingVariance": 1,
                  "distinctGenres": 1
                  }},

    # Only keep users with more than 20 ratings
    {"$match": {"ratingCount": {"$gte": 20}}},

    # Sort by number of distinct genres
    {"$sort": {"distinctGenres": -1, "ratingCount": -1, "userId": 1}},
    {"$limit": 10}
]

t10B_pipeline = [

    # Select  columns
    {"$project": {"_id": 0,
                  "userId": 1,
                  "ratingCount": 1,
                  "ratingMean": 1,
                  "ratingVariance": 1,
                  "distinctGenres": 1
                  }},

    # Only keep users with more than 20 ratings
    {"$match": {"ratingCount": {"$gte": 20}}},

    # Sort by rating variance
    {"$sort": {"ratingVariance": -1, "ratingCount": -1, "userId": 1}},
    {"$limit": 10}
]





