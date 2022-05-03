import pymongo
from bson.objectid import ObjectId

if __name__ == '__main__':
    mongo = pymongo.MongoClient("mongodb://lattice-100:27018/")
    db = mongo["sustaindb"]
    nhd_hydrography = db['nhd_hydrography']
    document = nhd_hydrography.find().sort('_id').limit(1).next()
    print(document)
