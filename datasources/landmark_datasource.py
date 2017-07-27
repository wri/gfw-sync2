__author__ = 'Asa.Strong'

import os
import logging
import requests
import json
import shutil
from osgeo import ogr

from datasource import DataSource
from utilities import archive
from utilities import util
from utilities import cartodb

class LandMarkDataSource(DataSource):
    """
    LandMark datasource class. Inherits from DataSource
    """

    def __init__(self, layerdef):
        logging.debug('Starting landmark_datasource')

        super(LandMarkDataSource, self).__init__(layerdef)

        self.layerdef = layerdef

    def get_esri_jsons(self, output_path):
        """Query landmark map services and save esri json to file"""

        #where 0 represents point files and 1 polygon files
        layer_ids = ['0','1']
        map_services = self.data_source.split(',')
        outfiles = []

        #query applied to each map service to retreieve json
        query = ("/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope"
        "&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=&returnGeometry=true"
        "&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false"
        "&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false"
        "&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance"
        "=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=json")

        #get json data from map services
        for map_service in map_services:
            url = map_service + query
            logging.debug(url)
            r = requests.get(url)
            data = r.json()
            if 'error' in data:
                raise ValueError('query resulted in error for {}'.format(url))
            elif len(data['features']) > 0:
                if '0' in map_service:
                    data_name = map_service.split('/')[-3] + '_' + '0'
                elif '1' in map_service:
                    data_name = map_service.split('/')[-3] + '_' + '1'
                out_json = output_path + '\\' + data_name + '.json'
                outfiles.append(out_json)

                #save esri json to files
                with open(out_json, 'w') as outfile:
                    json.dump(data, outfile)
                    logging.debug("dumped %s" %(data_name))

        return outfiles

    @staticmethod
    def json_to_shps(outfiles, output_path):
        """Convert esri json to shp with gdal cmds"""

        shps = []

        for outfile in outfiles:
            out_name = os.path.basename(outfile).split('.')[0]
            output = os.path.join(output_path, out_name + '.shp')
            shps.append(output)

            cmd = ['ogr2ogr', '-a_srs', 'EPSG:3857', output, outfile, 'OGRGeoJSON']
            try:
                util.run_subprocess(cmd)
                logging.debug("shp created for {}".format(out_name))
            except TypeError:
                logging.debug("TypeError caused ogr2ogr to fail on {}".format(out_name))

        return shps

    @staticmethod
    def add_field(shps):
        """Add field to each shp and enter map service name as value"""

        #create new field
        for shp in shps:
            try:
                data_name = os.path.basename(shp).split('.')[0]
                source = ogr.Open(shp, update=True)
                layer = source.GetLayer()
                new_field = ogr.FieldDefn('data_src', ogr.OFTString)
                layer.CreateField(new_field)
                feature = layer.GetNextFeature()
                logging.debug("field added to {}".format(shp))

                while feature:
                    feature.SetField("data_src", data_name)
                    layer.SetFeature(feature)
                    feature = layer.GetNextFeature()

                logging.debug("fields calculated for {}".format(shp))
                source = None
            except TypeError:
                logging.debug("{} NoneType".format(shp))

    @staticmethod
    def sort_shps(shps):
        """Sort shps into point and poly lists"""

        shps_point = []
        shps_poly = []

        for shp in shps:
            if '0' in shp:
                shps_point.append(shp)
            elif '1' in shp:
                shps_poly.append(shp)

        return shps_point, shps_poly

    @staticmethod
    def merge_and_zip_shps(shps_point, shps_poly, output_path):
        """Merge shps into two output shps then zip
        :return: zip shps"""

        logging.debug(shps_point)
        logging.debug(shps_poly)

        point_output = output_path + '\\' + 'landmark_point.shp'
        poly_output = output_path + '\\' + 'landmark_poly.shp'

        cmd_point_create = ['ogr2ogr', '-f', 'ESRI Shapefile', point_output, shps_point[0]]
        cmd_poly_create = ['ogr2ogr', '-f', 'ESRI Shapefile', poly_output, shps_poly[0]]

        #run subprocess to create polygon and point shps
        util.run_subprocess(cmd_poly_create)
        logging.debug("poly file created")
        util.run_subprocess(cmd_point_create)
        logging.debug("point file created")

        shps_point_max = len(shps_point)
        shps_poly_max = len(shps_poly)

        #append shps to poly and point files
        for x in range(1, shps_point_max):
            util.run_subprocess(['ogr2ogr', '-f', 'ESRI Shapefile', '-append', '-update', point_output, shps_point[x]])
            logging.debug("append %s" %(shps_point[x]))

        for x in range(1, shps_poly_max):
            util.run_subprocess(['ogr2ogr', '-f', 'ESRI Shapefile', '-append', '-update', poly_output, shps_poly[x]])
            logging.debug("append %s" %(shps_poly[x]))

        #zip merged files
        point_zip = archive.zip_shp(point_output)
        poly_zip = archive.zip_shp(poly_output)
        logging.debug("zipped files locally")

        return point_zip, poly_zip

    def sync_with_s3(self, point_zip, poly_zip):

        point_download, poly_download = self.download_output.split(',')

        #copy zipped files to S3
        shutil.copyfile(point_zip, point_download)
        logging.debug("copied point file to S3")
        shutil.copyfile(poly_zip, poly_download)
        logging.debug("copied poly file to S3")

        #Sync carto tables
        cartodb.cartodb_force_sync(self.gfw_env, 'landmark_point')
        cartodb.cartodb_force_sync(self.gfw_env, 'landmark_poly')

    def get_layer(self):
        """
        Get layer method called by layer_decision_tree.py
        Will perform the entire process of finding download and merging map service shapefiles
        :return: two merged files
        """
        #set class variables
        output_path = self.download_workspace
        outfiles = self.get_esri_jsons(output_path)
        shps = self.json_to_shps(outfiles, output_path)

        self.add_field(shps)
        shps_point, shps_poly = self.sort_shps(shps)
        point_zip, poly_zip = self.merge_and_zip_shps(shps_point, shps_poly, output_path)
        self.sync_with_s3(point_zip, poly_zip)
