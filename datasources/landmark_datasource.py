__author__ = 'Asa.Strong'

import os
import logging
import requests
import json
import subprocess
from osgeo import ogr

from datasource import DataSource

class LandMarkDataSource(DataSource):
    """
    LandMark datasource class. Inherits from DataSource
    """

    def __init__(self, layerdef):
        logging.debug('Starting landmark_datasource')

        super(LandMarkDataSource, self).__init__(layerdef)

        self.layerdef = layerdef

    def get_esri_jsons(self, map_services, output_path):

        layer_ids = ['0','1']
        outfiles = []

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
                pass
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
            else:
                pass

        return outfiles

    def json_to_shps(self, outfiles, output_path):

        shps = []

        for outfile in outfiles:
            out_name = os.path.basename(outfile).split('.')[0]
            output = os.path.join(output_path, out_name + '.shp')
            shps.append(output)

            cmd = ['ogr2ogr', '-a_srs', 'EPSG:3857', output, outfile, 'OGRGeoJSON']
            try:
                subprocess.check_call(cmd)
                logging.debug("shp created for {}".format(out_name))
            except TypeError:
                logging.debug("TypeError caused ogr2ogr to fail on {}".format(out_name))

        return shps

    def add_field(self, shps):

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

    def sort_shps(self, shps):

        shps_point = []
        shps_poly = []

        for shp in shps:
            if '0' in shp:
                shps_point.append(shp)
            elif '1' in shp:
                shps_poly.append(shp)

        return shps_point, shps_poly


    def merge_shps(self, shps_point, shps_poly, output_path):

        logging.debug(shps_point)
        logging.debug(shps_poly)

        shps_point_upper = len(shps_point)
        shps_poly_upper = len(shps_poly)

        point_output = output_path + '\\' + 'landmark_point.shp'
        poly_output = output_path + '\\' + 'landmark_poly.shp'

        cmd_point_create = ['ogr2ogr', '-f', 'ESRI Shapefile', point_output, shps_point[0]]
        cmd_poly_create = ['ogr2ogr', '-f', 'ESRI Shapefile', poly_output, shps_poly[0]]

        subprocess.check_call(cmd_poly_create)
        logging.debug("poly file created")
        subprocess.check_call(cmd_point_create)
        logging.debug("point file created")

        for x in range(1, shps_point_upper):
            cmd_append = ['ogr2ogr', '-f', 'ESRI Shapefile', '-append', '-update', point_output, shps_point[x]]
            logging.debug("append %s" %(x))
            subprocess.check_call(cmd_append)

        for x in range(1, shps_poly_upper):
            cmd_append = ['ogr2ogr', '-f', 'ESRI Shapefile', '-append', '-update', poly_output, shps_poly[x]]
            logging.debug("append %s" %(x))
            subprocess.check_call(cmd_append)

    def get_layer(self):
        """
        Get layer method called by layer_decision_tree.py
        Will perform the entire process of finding download and merging map service shapefiles
        :return: two merged files
        """
        # map_services = self.layerdef['source']
        map_services = self.data_source.split(',')
        output_path = self.download_workspace

        outfiles = self.get_esri_jsons(map_services, output_path)
        shps = self.json_to_shps(outfiles, output_path)
        self.add_field(shps)
        shps_point, shps_poly = self.sort_shps(shps)
        self.merge_shps(shps_point, shps_poly, output_path)
