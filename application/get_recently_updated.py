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

db = client[DATABASE]


def get_recently_updated():
    results = db[COLLECTION].find().limit(1).sort(
        [("lastName", pymongo.DESCENDING)]
    )
    return results


results = get_recently_updated()
for person in results:
    print(f"Person: {person['firstName']} {person['lastName']}. Updated at {person['updatedAt']}")
