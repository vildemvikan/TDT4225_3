import sys
from pprint import PrettyPrinter
from itertools import combinations
from operator import itemgetter
from pipelines.T2 import t2_pipeline
from pipelines.T4 import t4_pipeline
from pipelines.T6 import t6_pipeline
from pipelines.T8 import t8_pipeline
from pipelines.T10 import t10A_pipeline, t10B_pipeline

from DbConnector import DbConnector
from pymongo.errors import PyMongoError

# This file includes the query tasks: 2,4,6,8,10

class QueryPipeline:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client   # MongoClient
        self.db = self.connection.db           # Database

    def calculate_t2(self, cursor):
        final_aggregator = {}
        minimum = 3  # Minimum co-appearances
        limit = 10   # Result limit

        for movie_doc in cursor:
            vote = movie_doc.get('vote', 0.0)
            cast = movie_doc.get('cast', [])

            # Generate all unique, unordered pairs
            for actor_a, actor_b in combinations(cast, 2):

                #  Normalize the pair by ID
                id_a, name_a = actor_a['id'], actor_a['name']
                id_b, name_b = actor_b['id'], actor_b['name']

                # Ensure the smaller ID always comes first
                if id_a <= id_b:
                    aid, bid = id_a, id_b
                    name1, name2 = name_a, name_b
                else:
                    aid, bid = id_b, id_a
                    name1, name2 = name_b, name_a

                key = f"{aid}-{bid}"

                # 3. Aggregate
                if key not in final_aggregator:
                    final_aggregator[key] = {
                        'count': 1,
                        'sum_vote': vote,
                        'names': (name1, name2)
                    }
                else:
                    final_aggregator[key]['count'] += 1
                    final_aggregator[key]['sum_vote'] += vote

        # Final Filtering, Calculation, and Sorting
        final_results = []

        for pair_data in final_aggregator.values():
            count = pair_data['count']

            # Filter: Only pairs with minimum 3 co-appearances
            if count >= minimum:
                sum_vote = pair_data['sum_vote']

                # Calculate Average:
                avg_vote = round(sum_vote / count, 2)

                final_results.append({
                    'actor1': pair_data['names'][0],
                    'actor2': pair_data['names'][1],
                    'co_appearances': count,
                    'avg_vote': avg_vote
                })

        # Sort by number of co-appearances and average vote
        final_results.sort(key=itemgetter('co_appearances', 'avg_vote'), reverse=True)

        # Return the final results as an iterable list
        return final_results[:limit]

    def run_pipeline(self, pipeline, collection_name, *,
                  allow_disk_use=True, max_time_ms=120000, batch_size=1000):
        collection = self.db[collection_name]
        cursor = collection.aggregate(
            pipeline,
            allowDiskUse=allow_disk_use,
            maxTimeMS=max_time_ms,
            batchSize=batch_size,
        )
        return cursor

    def print_cursor(self, cursor, *, title=None, limit=None):
        ppr = PrettyPrinter(
            indent=2,
            width=120,        # wider lines before wrapping
            compact=True,     # tighter lists/tuples
            sort_dicts=False  # keep your field order
        )
        if title:
            print(f"\n=== {title} ===")

        for i, doc in enumerate(cursor, start=1):
            if limit and i > limit:
                print(f"... ({i-1} shown; truncated)")
                break
            print(f"\n#{i}")
            ppr.pprint(doc)


def main():
    qp = None
    try:
        qp = QueryPipeline()
        cursor = qp.run_pipeline(t2_pipeline, "Credits")
        cursor = qp.calculate_t2(cursor)
        qp.print_cursor(cursor, title="Top 10 actor-pairs with most co-appearances ")

        cursor = qp.run_pipeline(t4_pipeline, "Movie")
        qp.print_cursor(cursor, title="Top 10 collections with largest total revenue")

        cursor = qp.run_pipeline(t6_pipeline, "Credits")
        qp.print_cursor(cursor, title="Decades ranked by largest proportion of female cast")

        cursor = qp.run_pipeline(t8_pipeline, "Credits")
        qp.print_cursor(cursor, title="Top 20 director-actor pairs with highest mean average votes")

        cursor = qp.run_pipeline(t10A_pipeline, "Users")
        qp.print_cursor(cursor, title="Top 10 most genre-diverse users")

        cursor = qp.run_pipeline(t10B_pipeline, "Users")
        qp.print_cursor(cursor, title="Top 10 highest-variance users")

    except PyMongoError as e:
        print(f"Mongo error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if qp is not None and qp.client is not None:
            qp.client.close()


if __name__ == "__main__":
    main()
