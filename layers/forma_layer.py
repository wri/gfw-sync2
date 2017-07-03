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

        forma_img = self.create_image(forma_asset)

        start_date = "2012-01-01"
        end_date = "2017-06-26"

        mask = self.create_mask(forma_img, start_date, end_date)

        img = self.prepare_img(mask)

        geojson = '{"type":"FeatureCollection","features":[{"geometry":{"coordinates":[[[-59.14404369999999,28.2509496],[-119.71442160000002,34.107760999999996],[-80.7646799,-35.7052757],[-25.49333160000001,-40.820655800000004],[-13.6543579,0.21958820000000004],[18.8377143,-44.3353259],[50.0867817,-41.5724796],[89.92044180000002,-17.7600065],[128.9899959,-32.504978799999996],[169.5351853,-21.5634606],[180.9804286,3.5037111],[131.7706234,29.910959699999996],[45.0107953,25.616154900000005],[-15.104158600000005,15.245647699999997],[-33.25791799999999,0.27114449999999995],[-59.14404369999999,28.2509496]]],"geodesic":true,"type":"Polygon"},"id":"11","properties":{"geom_id":"6","name":"world"},"type":"Feature"}]}'

        geom = json.loads(geojson)

        gee_geom = self.get_region(geom)

        self.export_table(img, gee_geom, start_date, end_date)

        logging.debug("Forma exported to csv")

    def create_image(self, forma_asset):

        forma_ic = forma_asset.sort("system:time_start", False)
        forma_img = ee.Image(forma_ic.first())

        return forma_img

    def create_mask(self, forma_img, start_date, end_date):
        logging.debug("preparing GEE Image")

        start_millis = ee.Date(start_date).millis()
        end_millis = ee.Date(end_date).millis()

        date_band = forma_img.select(["alert_date"])
        date_mask = date_band.gt(start_millis).And(date_band.lte(end_millis))

        mask = forma_img.updateMask(date_mask)

        return mask

    def prepare_img(self, mask):

        img = mask.select(["alert_delta"])

        lonlat = ee.Image.pixelLonLat().reproject(img.projection())
        img = img.addBands([lonlat])
        img = img.mask(img.select(["alert_delta"]).gt(0))

        return img

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
        logging.debug("Formatting geojson to GEE Geom")

        poly = self.get_coords(geom)
        ptype = self.get_type(geom)
        if ptype.lower() == 'multipolygon':
            region = ee.Geometry.MultiPolygon(poly)
        else:
            region = ee.Geometry.Polygon(poly)
        return region

    def get_coll_params(self, img, gee_geom):
        """create ee collection"""

        bands = img.bandNames()

        coll_params =  {
                        'reducer': ee.Reducer.toCollection(bands),
                        'geometry': gee_geom,
                        'scale': img.projection().nominalScale(),
                        'maxPixels': ee.Number(1e12)
                        }

        return coll_params

    def get_count_params(self, img, gee_geom):
        """create count parameters"""

        count_params = {
                        'reducer': ee.Reducer.count(),
                        'geometry': gee_geom,
                        'scale': img.projection().nominalScale(),
                        'maxPixels': ee.Number(1e12)
                        }

        return count_params

    def export_table(self, img, gee_geom, start_date, end_date):
        """Export collection to csv"""

        coll_params = self.get_coll_params(img, gee_geom)
        coll = img.reduceRegion(**coll_params)

        count_params = self.get_count_params(img, gee_geom)
        count = img.reduceRegion(**count_params)

        coll = ee.FeatureCollection(coll.values()).flatten()

        export_params = {
                        'collection':coll,
                        'description':"test_alerts_pyton",
                        'bucket': "forma-2017",
                        'fileNamePrefix':'tmp/csv/forma_alerts_' + start_date + '_' + end_date + '.csv',
                        'fileFormat':'CSV'
                        }

        logging.debug("exporting table to csv")
        task = ee.batch.Export.table.toCloudStorage(**export_params)
        task.start()
