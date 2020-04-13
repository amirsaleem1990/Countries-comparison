# first go to terminal and type <sudo mongod>
# https://api.mongodb.com/python/current/tutorial.html
from pymongo import MongoClient
import datetime
client = MongoClient('localhost', 27017)
db = client.test_database
collection = db.test_collection
post = {"author": "Mike",
        "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
        "date": datetime.datetime.utcnow()}
posts = db.posts
post_id = posts.insert_one(post).inserted_id
# db.list_collection_names()

import pprint
pprint.pprint(posts.find_one())

print(client.list_database_names())