t6_pipeline = [
    # Keep only docs that has non-empty cast array
    {"$match": {"cast": {"$type": "array", "$ne": []}}},

    # Select columns (movie id and top-5 billed cast genders)
    {"$project": {
        "tmdbId": 1,
        "castTop5": {
            "$filter": {
                "input": {
                    "$map": {
                        "input": "$cast",
                        "as": "c",
                        "in": {
                            "gender": "$$c.gender",
                            "order": {"$ifNull": ["$$c.order", 999]}
                        }
                    }
                },
                "as": "p",
                "cond": {"$lt": ["$$p.order", 5]}
            }
        }
    }},

    # Keep only known genders (1: female, 2: male)
    {"$set": {
        "castKnown": {
            "$filter": {"input": "$castTop5", "as": "p", "cond": {"$in": ["$$p.gender", [1, 2]]}}
        }
    }},

    # Count known and female cast among top-5 billed cast
    {"$set": {
        "knownCount": {"$size": "$castKnown"},
        "femaleCount": {
            "$size": {"$filter": {"input": "$castKnown", "as": "p", "cond": {"$eq": ["$$p.gender", 1]}}}
        }
    }},

    # Compute female proportion, drop movies where it is undefined
    {"$set": {
        "femaleProp": {
            "$cond": [{"$gt": ["$knownCount", 0]}, {"$divide": ["$femaleCount", "$knownCount"]}, None]
        }
    }},
    {"$match": {"femaleProp": {"$ne": None}}},

    # Join with Movie to get release year per movie
    {"$lookup": {
        "from": "Movie",
        "localField": "tmdbId",
        "foreignField": "tmdbId",
        "pipeline": [
            {"$project": {
                "_id": 0,
                "_rd": {
                    "$cond": [
                        {"$eq": [{"$type": "$release_date"}, "date"]},
                        "$release_date",
                        {"$convert": {"input": "$release_date", "to": "date", "onError": None, "onNull": None}}
                    ]
                },
                "releaseYear": 1
            }},
            {"$project": {
                "year": {
                    "$cond": [
                        {"$ne": ["$_rd", None]},
                        {"$year": "$_rd"},
                        "$releaseYear"
                    ]
                }
            }}
        ],
        "as": "m"
    }},
    {"$unwind": "$m"},

    # Ensure year is numeric
    {"$match": {"m.year": {"$type": "number"}}},

    # Compute decade bucket
    {"$set": {"decade": {"$subtract": ["$m.year", {"$mod": ["$m.year", 10]}]}}},

    # Aggregate by decade
    {"$group": {
        "_id": "$decade",
        "avg_female_prop": {"$avg": "$femaleProp"},
        "movie_count": {"$sum": 1}
    }},

    # Final shape
    {"$project": {
        "_id": 0,
        "decade": "$_id",
        "avg_female_prop": {"$round": ["$avg_female_prop", 2]},
        "movie_count": 1
    }},

    # Sort by highest average female proportion
    {"$sort": {"avg_female_prop": -1, "decade": 1}}
]
