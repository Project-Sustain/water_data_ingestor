import utils

def queryMongoForPolygonSites(sites, coordinates):

    geoWithinQuery = {
        'geometry': {
            '$geoWithin': {
                '$geometry': {
                    'type': 'MultiPolygon',
                    'coordinates': coordinates
                }
            }
        }
    }

    try:
        sitesInThisPolygon = sites.find(geoWithinQuery, no_cursor_timeout = True)
    except Exception as e:
        sitesInThisPolygon = []

    siteList = []

    for site in sitesInThisPolygon:
        try:
            siteList.append(site['properties']['MonitoringLocationIdentifier'])
        except:
            pass

    for outerCoordinateList in coordinates:
        for innerCoordinateList in outerCoordinateList:
            for coordinatePair in innerCoordinateList:
                try:
                    geoNearQuery = {
                        '$geoNear': {
                            'near': { 'type': "Point", 'coordinates': coordinatePair },
                            'distanceField': "dist.calculated",
                            'maxDistance': 50,
                            'spherical': 'true'
                        }
                    }
                    sites_near_coordinate_pair = sites.aggregate([geoNearQuery])
                except Exception as e:
                    sites_near_coordinate_pair = []

                for site in sites_near_coordinate_pair:
                    try:
                        siteName = site['properties']['MonitoringLocationIdentifier']
                        if siteName not in siteList:
                            siteList.append(siteName)
                    except:
                        pass

    siteList.sort()
    return siteList


def queryMongoForLineSites(sites, coordinates):

    siteList = []
 
    for coordinatePair in coordinates:
        geoNearQuery = {
        '$geoNear': {
                'near': { 'type': "Point", 'coordinates': coordinatePair },
                'distanceField': "dist.calculated",
                'maxDistance': 50,
                'spherical': 'true'
            }
        }
        sites_near_coordinate_pair = sites.aggregate([geoNearQuery]) 

        for site in sites_near_coordinate_pair:
            try:
                site_name = site['properties']['MonitoringLocationIdentifier']
                if site_name not in siteList:
                    siteList.append(site_name)
            except:
                pass

    siteList.sort()
    return siteList
    