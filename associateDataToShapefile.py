import sys
import pymongo
from ThreadedDocumentProcessor import ThreadedDocumentProcessor

mongo = pymongo.MongoClient('mongodb://lattice-100:27018/')
db = mongo['sustaindb']
water_quality_data = db['water_quality_data']
water_quality_data_bodies_of_water = db['water_quality_data_bodies_of_water']
water_quality_data_rivers_and_streams = db['water_quality_data_rivers_and_streams']
water_quality_data_pipes = db['water_quality_data_pipes']


class DocumentProcessor(ThreadedDocumentProcessor):
    def __init__(self, collection, number_of_threads, query):
        super().__init__(collection, number_of_threads, query, DocumentProcessor.processDocument)

    def processDocument(self, site):
        '''
        This is the function that will be called by each thread on each document.
        If this function returns something, it must be a dictionary. 
        Said dictionary will be written in JSON format to the output.json file.

        Update this function to perform whatever actions you need to on each document.
        '''
        cursor = water_quality_data.find({'MonitoringLocationIdentifier': site['MonitoringLocationIdentifier']})
        
        if 'GridCodeType' in site:
            if site['GridCodeType'] == 'Multipolygon':
                for document in cursor:
                    del document('_id')
                    document['GridCode'] = site['GridCode']
                    water_quality_data_bodies_of_water.insert_one(document)
            else:
                for document in cursor:
                    del document('_id')
                    document['GridCode'] = site['GridCode']
                    water_quality_data_rivers_and_streams.insert_one(document)
        else:
            for document in cursor:
                del document('_id')
                water_quality_data_pipes.insert_one(document)
        

def main(collection, number_of_threads):
    query = {} # Update the `query` field to specify a mongo query
    documentProcessor = DocumentProcessor(collection, number_of_threads, query)
    documentProcessor.run()


if __name__ == '__main__':
    # NOTE make sure you have run both nhd_hydrography and nhd_flow thru associateSiteToShapefile.py
    # USE: python3 associateDataToShapefile.py water_quality_sites 8
    if len(sys.argv) == 3:
        collection = sys.argv[1]
        number_of_threads = int(sys.argv[2])
        main(collection, number_of_threads)
    else:
        print(f'Invalid args. Check the `README.md` file for program usage')