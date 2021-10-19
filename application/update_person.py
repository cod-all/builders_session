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

db = client[DATABASE]


def update_person(firstName, lastName):
    db[COLLECTION].update_one(
        {"firstName": firstName, "lastName": lastName},
        {
            "$set": {"updatedAt": datetime.datetime.now().isoformat(),}
        },
    )
    print(f"Person: ",firstName, " ", lastName," updated at ", datetime.datetime.now().isoformat())
    return

def get_person():
    
    results = db[COLLECTION].aggregate([{"$sample":{"size":1}}])
    return results

record = get_person()
for person in record:
    firstName = person['firstName']
    lastName = person['lastName']
    update_person(firstName,lastName)
