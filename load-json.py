from pymongo import MongoClient
import os.path
import json
import time
import sys

def load_tweets(filename, col, batch_size = 1000):
    '''
    Loads tweet data from json file into mongodb database in batches

    :param filename: Path of json file to read tweet data from
    :param col: Collection to store tweet data in
    :param batch_size: Size of each tweet batch to be inserted into the collection
    '''
    with open(filename, "r") as data:
        eof = False

        while not eof:
            batch = []

            # Iterate until end of file or until we complete a batch
            for i in range (batch_size):
                tweet_data = data.readline().strip()

                # EOF has been reached
                if not tweet_data:
                    eof = True
                    break

                batch.append(json.loads(tweet_data)) # Add data to batch
            col.insert_many(batch) # Load batch into database

if __name__ == "__main__":
    argc = len(sys.argv) # Arg count
    
    # Make sure we passed 2 args
    if argc != 3:
        sys.exit("Error: Incorrect number of arguments")

    # Retrieve command line args
    arg_filename = sys.argv[1]
    arg_port = sys.argv[2]

    # Verify filepath is valid
    if not os.path.isfile(arg_filename):
        sys.exit("Error: Improper file path")

    client = MongoClient(f"mongodb://localhost:{arg_port}")

    db = client["291db"]
    db_collections = db.list_collection_names()

    # Collection name of tweets
    tweets_id = "tweets"

    # Drop existing collection
    if tweets_id in db_collections:
        print("Dropping existing collection", tweets_id)
        db[tweets_id].drop()
        print("Dropped", tweets_id)

    # Create new collection
    tweets_col = db[tweets_id]
    print("Created", tweets_id)

    # Load tweet data
    print("Loading data")
    start_time = time.perf_counter_ns()
    load_tweets(arg_filename, tweets_col, 5000)
    end_time = time.perf_counter_ns()

    total_time = end_time - start_time
    print(f"Data loaded in {total_time / 1000000000} seconds\n")
