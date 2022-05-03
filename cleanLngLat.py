
import sys
import queries
import utils
import json
import pymongo
from bson.objectid import ObjectId
from ThreadedDocumentProcessor import ThreadedDocumentProcessor


mongo = pymongo.MongoClient("mongodb://lattice-100:27018/")
db = mongo["sustaindb"]
nhd_hydrography = db['nhd_hydrography']


class DocumentProcessor(ThreadedDocumentProcessor):
    def __init__(self, collection, number_of_threads, query):
        super().__init__(collection, number_of_threads, query, DocumentProcessor.processDocument)

    def processDocument(self, document):
        '''
        This is the function that will be called by each thread on each document.
        If this function returns something, it must be a dictionary. 
        Said dictionary will be written in JSON format to the output.json file.

        Update this function to perform whatever actions you need to on each document.
        '''
        coordinates = document['geometry']['coordinates']
        utils.clean_coordinates(coordinates)
        id = str(document['_id'])
        nhd_hydrography.update_one( { '_id': ObjectId(id) }, { '$set': { 'geometry.coordinates': coordinates } } )


def main(collection, number_of_threads):
    query = {} # Update the `query` field to specify a mongo query
    documentProcessor = DocumentProcessor(collection, number_of_threads, query)
    documentProcessor.run()


if __name__ == '__main__':
    if len(sys.argv) == 3:
        collection = sys.argv[1]
        number_of_threads = int(sys.argv[2])
        main(collection, number_of_threads)
    else:
        print(f'Invalid args. Check the `README.md` file for program usage')

