import json
import os

import pymongo

USER = os.environ["DOCUMENTDB_USER"]
PASSWORD = os.environ["DOCUMENTDB_PASSWORD"]
HOST = os.environ["DOCUMENTDB_ENDPOINT"]
DATABASE = "testDb"
COLLECTION = "people"

client = pymongo.MongoClient(
    f"mongodb://{USER}:{PASSWORD}@{HOST}:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred"
)

db = client.{DATABASE}


def get_recently_updated_by_location(location):
    results = db.{COLLECTION}.find().sort(
        [("updatedAt", pymongo.DESCENDING)]
    )
    return results


results = get_recently_updated_by_location()
for person in results:
    print(f"Person: {person['lastName']}. Updated at {person['updatedAt']}")
