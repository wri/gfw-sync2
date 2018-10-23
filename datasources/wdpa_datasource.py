import os
import sys
import logging
import arcpy
import re
import calendar

from datasource import DataSource
from utilities import google_sheet as gs
from utilities import util


class WDPADatasource(DataSource):
    """
    WDPA datasource class. Inherits from DataSource
    Used to download the source GDB, find the polygon FC, repair and simplify geometry
    """
    def __init__(self, layerdef):
        logging.debug('Starting simple_datasource')
        super(WDPADatasource, self).__init__(layerdef)

        self.layerdef = layerdef

    def prep_source_fc(self):

        # start a decent size (m4.4xlarge?) spot machine
        # download the WDPA database to that machine
        # convert gdb --> ogr2ogr -f Postgresql PG:"dbname=ubuntu" -t_srs PROJCS["World_Eckert_VI",GEOGCS["GCS_WGS_1984",DATUM["WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Eckert_VI"],PARAMETER["False_Easting",0],PARAMETER["False_Northing",0],PARAMETER["Central_Meridian",0],UNIT["Meter",1],AUTHORITY["EPSG","54010"]] WDPA_Oct2018_Public/WDPA_Oct2018_Public.gdb WDPA_poly_Oct2018 -nln wdpa -lco GEOMETRY_NAME=geom
        # Simplify geometry: UPDATE wdpa SET geom = ST_Multi(ST_SimplifyPreserveTopology(geom, 10));
        # Make valid: UPDATE wdpa SET geom = ST_CollectionExtract(ST_MakeValid(geom), 3) WHERE ST_IsValid(geom) <> '1';
        # Export: ogr2ogr wdpa_simplified.shp PG:"dbname=ubuntu" -sql "SELECT * FROM wdpa"
        # then zip, upload to s3 and download

        # raise ValueError('Automatic updates for this dataset are not currently possible- repair '
        #                  'geometry fails immediately in ArcGIS. Instead, process in PostGIS with the'
        #                  'instructions in wdpa_datasource.py, then comment out this error. Fun! ')
        self.data_source = r"D:\temp\wdpa_simplified.shp"

    def get_layer(self):
        """
        Full process, called in layer_decision_tree.py. Downloads and preps the data
        :return: Returns and updated layerdef, used in the layer.update() process in layer_decision_tree.py
        """

        self.prep_source_fc()

        self.layerdef['source'] = self.data_source

        return self.layerdef
