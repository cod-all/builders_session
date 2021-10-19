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

db[COLLECTION].create_index(
    [("firstName", pymongo.ASCENDING), ("lastName", pymongo.DESCENDING), ("updatedAt", pymongo.DESCENDING)]
)

db[COLLECTION].create_index(
    [("address.city", pymongo.ASCENDING), ("age", pymongo.DESCENDING)]
)

print("Index created successfully.")