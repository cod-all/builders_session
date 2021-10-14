import json
import os

import pymongo

from pymongo.errors import BulkWriteError
from pymongo import InsertOne
from faker import Factory
from faker import Faker
import faker
from multiprocessing import Process
import time
import datetime
import random

#Settings for Faker - change the locale to create data in other languages
fake = Factory.create('en_US') 

#Number of processes to launch
processesNumber = 50
processesList = []

USER = os.environ["DOCUMENTDB_USER"]
PASSWORD = os.environ["DOCUMENTDB_PASSWORD"]
HOST = os.environ["DOCUMENTDB_ENDPOINT"]
DATABASE = "testDb"
COLLECTION = "people"

def run(processId):
    #Connect to your cluster
    client = pymongo.MongoClient(
        f"mongodb://{USER}:{PASSWORD}@{HOST}:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred"
    )

    #Define your database and collection
    db = client[DATABASE]
    coll = db[COLLECTION]

    document = {
                                "process":processId,
                                "updatedAt":datetime.datetime.now().isoformat(),
                                "lastName":fake.last_name(),
                                "firstName":fake.first_name(),
                                "ssn":fake.ssn(),
                                "job":fake.job(),
                                "dob":"1971-06-22",
                                "phone":[
                                        {"type":"home","number":fake.phone_number()},
                                        {"type":"cell","number":fake.phone_number()}
                                ],
                                "address":{
                                            "street":fake.street_address(),
                                            "city":fake.city()
                                },
                                "revenue": random.randint(50000,250000),
                                "age": random.randint(20,60),
                                "profile":{
                                          # "imageAvatar":fake.imageAvatar(),
                                            #"imageUrl":fake.imageUrl(),
                                            "filterValue":random.randint(0,1),
                                            "favColor":fake.color_name()
                                },
                                "favQuote":[
                                          {"quoteRank":"1","quote":fake.text()},
                                          {"quoteRank":"2","quote":fake.text()},
                                          {"quoteRank":"3","quote":fake.text()}
                                ],
                                "knownAlias":[
                                             {"filePath":fake.file_path(depth=5)},
                                             {"md5":fake.md5(raw_output=False)},
                                             {"aliasInfo":fake.csv(header=('Name', 'Address', 'Favorite Color'), data_columns=('{{name}}', '{{address}}', '{{safe_color_name}}'), num_rows=10, include_row_ids=True)}
                                ],
            }

    result = coll.insert_one(document) 

if __name__ == '__main__':
    # Creation of processesNumber processes
    for i in range(processesNumber):
        process = Process(target=run, args=(i,))
        processesList.append(process)

    # launch processes
    for process in processesList:
        process.start()


    # wait for processes to complete
    for process in processesList:
        process.join()

    print("Documents loaded successfully.")

#Create an index
client = pymongo.MongoClient(
    f"mongodb://{USER}:{PASSWORD}@{HOST}:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred"
)

db = client[DATABASE]

db[COLLECTION].create_index([("lastName", pymongo.DESCENDING)])

print("Index created successfully.")