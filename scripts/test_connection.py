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

results = db[COLLECTION].estimated_document_count()

print(
    f"Connected successfully! There are {results} documents in your {COLLECTION} collection."
)
