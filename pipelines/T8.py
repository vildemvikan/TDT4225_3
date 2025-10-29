t8_pipeline = [
    # Join with Movie
    {
        "$lookup": {
            "from": "Movie",
            "localField": "tmdbId",
            "foreignField": "tmdbId",
            "as": "movie"
        }
    },
    {"$unwind": "$movie"},

    # Keep only movies with sufficient votes
    {"$match": {"movie.vote_count": {"$gte": 100}}},

    # Select columns and build a director/actor array
    {
        "$project": {
            "tmdbId": 1,
            "directors": {
                "$map": {
                    "input": {
                        "$filter": {
                            "input": "$crew",
                            "as": "c",
                            "cond": {"$eq": ["$$c.job", "Director"]}
                        }
                    },
                    "as": "d",
                    "in": {"id": "$$d.id", "name": "$$d.name"}
                }
            },
            "actors": {
                "$map": {
                    "input": "$cast",
                    "as": "a",
                    "in": {"id": "$$a.id", "name": "$$a.name"}
                }
            },
            "vote_average": "$movie.vote_average",
            "revenue": "$movie.revenue"
        }
    },

    # Create one document per (director, actor, movie)
    {"$unwind": "$directors"},
    {"$unwind": "$actors"},

    # De-duplicate at movie level for each pair and carry metrics once
    {
        "$group": {
            "_id": {
                "directorId": "$directors.id",
                "directorName": "$directors.name",
                "actorId": "$actors.id",
                "actorName": "$actors.name",
                "tmdbId": "$tmdbId"
            },
            "vote_average": {"$first": "$vote_average"},
            "revenue": {"$first": "$revenue"}
        }
    },

    # Aggregate per pair across movies
    {
        "$group": {
            "_id": {
                "directorId": "$_id.directorId",
                "directorName": "$_id.directorName",
                "actorId": "$_id.actorId",
                "actorName": "$_id.actorName"
            },
            "films": {"$addToSet": "$_id.tmdbId"},
            "meanVote": {"$avg": "$vote_average"},
            "meanRevenue": {"$avg": "$revenue"}
        }
    },

    # Require ≥ 3 collaborations
    {
        "$addFields": {
            "filmCount": {"$size": "$films"}
        }
    },
    {"$match": {"filmCount": {"$gte": 3}}},

    # Sort by highest mean vote_average and limit to top-20
    {"$sort": {"meanVote": -1}},
    {"$limit": 20},

    # Final shape
    {
        "$project": {
            "_id": 0,
            "directorId": "$_id.directorId",
            "director": "$_id.directorName",
            "actorId": "$_id.actorId",
            "actor": "$_id.actorName",
            "filmCount": 1,
            "meanVote": {"$round": ["$meanVote", 2]},
            "meanRevenue": {"$round": ["$meanRevenue", 0]}
        }
    }
]
