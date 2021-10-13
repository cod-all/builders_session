import datetime
import os

import pymongo

USER = os.environ["DOCUMENTDB_USER"]
PASSWORD = os.environ["DOCUMENTDB_PASSWORD"]
HOST = os.environ["DOCUMENTDB_ENDPOINT"]
DATABASE = "testDb"
COLLECTION = "people"

client = pymongo.MongoClient(
    f"mongodb://{USER}:{PASSWORD}@{HOST}:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retrywrites=false"
)

db = client.{DATABASE}


def add_review_to_restaurant(name):
    db.{COLLECTION}.update_one(
        {"name": name},
        {
            "$set": {"updatedAt": datetime.datetime.now().isoformat(),}
        },
    )
    return

def get_recently_updated(location):
    results = db.{COLLECTION}.aggregate([{ $sample: { size: 1 } }])
    return results

results = get_recently_updated()
for person in results:
    #print(f"Person: {person['lastName']}. Updated at {person['updatedAt']}")

    add_review_to_restaurant(
        {person['lastName']},
       )
