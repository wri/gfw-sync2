__author__ = 'Asa.Strong'

import subprocess
import logging
import ee
import json

from layers.vector_layer import VectorLayer

class FormaLayer(VectorLayer):

    def __init__(self, layerdef):
        logging.debug('Starting FormaLayer')

        super(FormaLayer, self).__init__(layerdef)

        ee.Initialize()

        self.source = layerdef['source']

    def update(self):

        forma_asset = ee.ImageCollection(self.source)

        forma_img = create_image(forma_asset)

        geojson = '{"type":"FeatureCollection","features":[{"geometry":{"coordinates":[[[[101.07421875,20.468189222640955],[96.85546875,18.145851771694467],[99.31640625,14.774882506516272],[103.359375,18.145851771694467],[101.07421875,20.468189222640955]]],[[[-75.9375,23.241346102386135],[-122.34375,33.13755119234614],[-91.40625,5.61598581915534],[-80.15625,-1.4061088354351594],[-85.78125,-12.554563528593656],[-74.53125,-16.63619187839765],[-81.5625,-42.0329743324414],[-54.84375,-42.0329743324414],[-15.46875,-2.811371193331128],[7.03125,1.4061088354351594],[9.84375,-29.535229562948444],[26.71875,-44.08758502824516],[53.4375,-12.554563528593656],[106.875,-8.407168163601074],[136.40625,7.013667927566642],[127.96875,29.53522956294847],[-22.5,27.059125784374068],[-75.9375,23.241346102386135]]]],"geodesic":true,"type":"MultiPolygon"},"id":"0","properties":{},"type":"Feature"}]}'

        geom = json.loads(geojson)

        export_table(forma_img, geom)

        # Update the csv
        self._update()

    def create_image(self, forma_asset):
        """prepare ee image"""

        forma_img = ee.Image(forma_asset.first()).select(['activity'])
        lonlat = ee.Image.pixelLonLat().reproject(forma_img.projection())

        forma_img = forma_img.addBands([lonlat])
        forma_img = forma_img.mask(forma_img.select(['activity']).gt(0))

        return forma_img

    def get_type(self, geom):
        """grab type attribute from geojson"""

        if geom.get('features') is not None:
            return geom.get('features')[0].get('geometry').get('type')
        elif geom.get('geometry') is not None:
            return geom.get('geometry').get('type')
        else:
            return geom.get('type')

    def get_coords(self, geom):
        """grab coordinates from geojson"""

        if geom.get('features') is not None:
            return geom.get('features')[0].get('geometry').get('coordinates')
        elif geom.get('geometry') is not None:
            return geom.get('geometry').get('coordinates')
        else:
            return geom.get('coordinates')

    def get_region(self, geom):
        """Return ee.Geometry from supplied GeoJSON object."""

        poly = get_coords(geom)
        ptype = get_type(geom)
        if ptype.lower() == 'multipolygon':
            region = ee.Geometry.MultiPolygon(poly)
        else:
            region = ee.Geometry.Polygon(poly)
        return region

    def get_coll_params(self, forma_img, geom):
        """create ee collection"""

        bands = forma_img.bandNames()

        coll_params =  {
                        'reducer': ee.Reducer.toCollection(bands),
                        'geometry': get_region(geom),
                        'scale': forma_img.projection().nominalScale(),
                        'maxPixels': ee.Number(1e12)
                        }

        return coll_params

    def get_count_params(self, forma_img, geom):
        """create count parameters"""

        count_params = {
                        'reducer': ee.Reducer.count(),
                        'geometry': get_region(geom),
                        'scale': forma_img.projection().nominalScale(),
                        'maxPixels': ee.Number(1e12)
                        }

        return count_params

    # print (count,coll.size(),coll.first())

    def export_table(self, forma_img, geom):
        """Export collection to csv"""

        coll_params = get_coll_params(forma_img, geom)
        coll = forma_img.reduceRegion(**coll_params)

        count_params = get_count_params(forma_img, geom)
        count = forma_img.reduceRegion(**count_params)

        coll = ee.FeatureCollection(coll.values()).flatten()

        export_params = {
                        'collection':coll,
                        'description':"test_alerts_pyton",
                        'bucket': "forma-2017",
                        'fileNamePrefix':'tmp/csv/test_alerts_python.csv',
                        'fileFormat':'CSV'
                        }

        task = ee.batch.Export.table.toCloudStorage(**export_params)
        task.start()
