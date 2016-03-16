import os
import sys
import zipfile
import requests
import arcpy
import warnings
import shutil
import validators
from bs4 import BeautifulSoup
from urllib2 import urlopen

import settings

class DataSource(object):

    def __init__(self, layerdef):
        """ A general data source class
        :param layerdef: A Layer definition dictionary
        :return:
        """
        print 'starting datasource class'
        
        self.type = "DataSource"

        self._name = None
        self.name = layerdef['tech_title']

        self._download_workspace = None
        self.download_workspace = os.path.join(settings.settings['paths']['download_workspace'], self.name)

        self._source = None
        self.source = layerdef['source']


    # Validate name
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, n):
        if not n:
            warnings.warn("Name cannot be empty", Warning)
        self._name = n

    # Validate Download workspace
    @property
    def download_workspace(self):
        return self._download_workspace

    @download_workspace.setter
    def download_workspace(self, s):
        if os.path.exists(s):
            shutil.rmtree(s)
        os.mkdir(s)
        self._download_workspace = s
        
    # Validate source
    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, s):
        if s is not None:
            isURL = False
            try:
                validators.url(s)
                isURL = True
            except:
                pass

            if isURL:
                self.data_source_type = 'URL'
            else:
                if os.path.exists(s):
                    self.data_source_type = 'network_location'
                else:
                    warnings.warn("Cannot find source {0!s}".format(s), Warning)

        self._source = s

    def download_zipfile(self, url, output_dir):
        fname = os.path.split(url)[1]
        path = os.path.join(output_dir, fname)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        r = requests.get(url, stream=True)

        print "Downloading {0!s} to {1}".format(url, path)
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                f.write(chunk)
                f.flush
        print "Download complete."
        return path

    def unzip(self, filename, folder):
        print filename
        if zipfile.is_zipfile(filename):
            print 'Unzipping {0} to directory {1}'.format(filename, folder)
            zf = zipfile.ZipFile(filename, 'r')
            zf.extractall(folder)
            return zf.namelist()
        else:
            return []

    def remove_all_fields_except(self, fc, keep_field_list):
        field_list = [f.name for f in arcpy.ListFields(fc) if not f.required]

        for field in field_list:
            if field in keep_field_list:
                pass
            else:
                arcpy.DeleteField_management(fc, field)

        return

    def add_field_and_calculate(self, fc, fieldName, fieldType, fieldPrecision, fieldVal):
        
        arcpy.AddField_management(fc, fieldName, fieldType, fieldPrecision)

        if fieldType in ['TEXT', 'DATE']:
            fieldVal = "'{0}'".format(fieldVal)

        print fieldName, fieldVal
        arcpy.CalculateField_management(fc, fieldName, fieldVal, "PYTHON")




