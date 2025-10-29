t4_pipeline = [
  # Keep only movies that belong to a collection
  {"$match": {"belongs_to_collection.name": {"$ne": None}}},

  # Normalize documents
  {"$addFields": {
    "_collectionId": "$belongs_to_collection.id",
    "_collectionName": "$belongs_to_collection.name",
    "_revenue": {"$ifNull": ["$revenue", 0]},
    "_vote": {
      "$cond": [
        {"$and": [
          {"$ne": ["$vote_average", None]},
          {"$ne": ["$vote_average", float("nan")]}
        ]},
        "$vote_average",
        None
      ]
    },
    "_date": {
      "$cond": [
        {"$eq": [{"$type": "$release_date"}, "date"]},
        "$release_date",
        {"$toDate": "$release_date"}
      ]
    }
  }},

  # Group per collection
  # Compute movie count and total revenue, list vote average per movie, and find earliest and latest release date
  {"$group": {
    "_id": {"id": "$_collectionId", "name": "$_collectionName"},
    "movie_count": {"$sum": 1},
    "total_revenue": {"$sum": "$_revenue"},
    "votes": {"$push": {"$cond": [{"$ne": ["$_vote", None]}, "$_vote", "$$REMOVE"]}},
    "earliest_release_date": {"$min": "$_date"},
    "latest_release_date": {"$max": "$_date"}
  }},

  # Only keep collections with more than 3 movies
  {"$match": {"movie_count": {"$gte": 3}}},

  # Find the median vote average
  {"$set": {"votes_sorted": {"$sortArray": {"input": "$votes", "sortBy": 1}}}},
  {"$set": {
    "median_vote_average": {
      "$let": {
        "vars": {"n": {"$size": "$votes_sorted"}},
        "in": {
          "$cond": [
            {"$eq": ["$$n", 0]},
            None,
            {
              "$cond": [
                {"$eq": [{"$mod": ["$$n", 2]}, 1]},  # odd length
                {"$arrayElemAt": ["$votes_sorted", {"$floor": {"$divide": ["$$n", 2]}}]},
                {"$avg": [
                  {"$arrayElemAt": ["$votes_sorted", {"$subtract": [{"$divide": ["$$n", 2]}, 1]}]},
                  {"$arrayElemAt": ["$votes_sorted", {"$divide": ["$$n", 2]}]}
                ]}
              ]
            }
          ]
        }
      }
    }
  }},

  # Final shape
  {"$project": {
    "_id": 0,
    "collection_id": "$_id.id",
    "collection_name": "$_id.name",
    "movie_count": 1,
    "total_revenue": 1,
    "median_vote_average": 1,
    "earliest_release_date": 1,
    "latest_release_date": 1
  }},

  # Return top-10 by total revenue
  {"$sort": {"total_revenue": -1}},
  {"$limit": 10}
]
