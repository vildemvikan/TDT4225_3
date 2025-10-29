t2_pipeline = [
    # Select columns and values
    {"$project": {
        "tmdbId": 1,
        "cast": {
            "$map": {
                "input": {"$ifNull": ["$cast", []]},
                "as": "c",
                "in": {
                    "id": "$$c.id",
                    "name": "$$c.name",
                }
            }
        }
    }},

    # Keep only movies with < 2 cast members
    {"$match": {"$expr": {"$gte": [{"$size": "$cast"}, 2]}}},

    # Join Movie data
    {"$lookup": {
        "from": "Movie",
        "localField": "tmdbId",
        "foreignField": "tmdbId",
        "pipeline": [{"$project": {"_id": 0, "vote_average": 1}}],
        "as": "m"
    }},
    {"$unwind": "$m"},
    {"$set": {"vote": "$m.vote_average"}},
    {"$unset": "m"},
]
