
import sys
import queries
import pymongo
from bson.objectid import ObjectId
from ThreadedDocumentProcessor import ThreadedDocumentProcessor

mongo = pymongo.MongoClient("mongodb://lattice-100:27018/")
db = mongo["sustaindb"]
sites = db['water_quality_sites']

collection = ''

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
        GridCode = document['GridCode']
        coordinates = document['geometry']['coordinates']
        if collection == 'nhd_hydrography':
            GridCodeType = 'Multipolygon'
            site_list = queries.queryMongoForPolygonSites(sites, coordinates)
        else:
            GridCodeType = 'Line'
            site_list = queries.queryMongoForLineSites(sites, coordinates)

        for site_id in site_list:
            site = sites.find({'MonitoringLocationIdentifier' : site_id}).next()
            id = str(site['_id'])
            sites.update_one( { '_id': ObjectId(id) }, { '$set': { 'GridCode': GridCode } } )
            sites.update_one( { '_id': ObjectId(id) }, { '$set': { 'GridCodeType': GridCodeType } } )

        if len(site_list) > 0:
            return {GridCode: site_list}


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

