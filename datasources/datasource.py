import os
import sys
import zipfile
import requests
import arcpy
import warnings
import shutil
import validators

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

        self._source = None
        self.source = layerdef['source']

        self._gfw_env = None
        self.gfw_env = layerdef['gfw_env']

        self._download_workspace = None
        self.download_workspace = os.path.join(settings.get_settings(self.gfw_env)['paths']['download_workspace'], self.name)

    # Validate name
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, n):
        if not n:
            warnings.warn("Name cannot be empty", Warning)
        self._name = n

    # Validate download workspace
    @property
    def download_workspace(self):
        return self._download_workspace

    @download_workspace.setter
    def download_workspace(self, d):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.mkdir(d)
        self._download_workspace = d
        
    # Validate source
    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, s):
        if s is not None:

            try:
                validators.url(s)
                isURL = True
            except:
                isURL = False

            if isURL:
                self.data_source_type = 'URL'
            else:
                if os.path.exists(s):
                    self.data_source_type = 'network_location'
                else:
                    warnings.warn("Cannot find source {0!s}".format(s), Warning)

        self._source = s

    def download_file(self, url, output_dir):
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


    def unzip_and_find_data(self, in_zipfile):
        self.unzip(in_zipfile, self.download_workspace)

        shp_list = [x for x in os.listdir(self.download_workspace) if os.path.splitext(x)[1] == '.shp']
        tif_list = [x for x in os.listdir(self.download_workspace) if os.path.splitext(x)[1] == '.tif']

        if len(shp_list) == 1:
            source_file = os.path.join(self.download_workspace, shp_list[0])

        elif len(tif_list) == 1:
            source_file = os.path.join(self.download_workspace, tif_list[0])

        else:
            print 'Unknown output from zip file, {0} shps, {1} tifs'.format(len(shp_list), len(tif_list))
            print 'May need to define a custom function to unpack this data source. Exiting now.'
            sys.exit(2)

        return source_file

    def get_layer(self):

        if self.data_source_type == 'URL':

            local_file = self.download_file(self.source, self.download_workspace)

            if os.path.splitext(local_file)[1] == '.zip':
                self.source = self.unzip_and_find_data(local_file)

            else:
                self.source = local_file

        else:
            print 'Data source type is not URL, why is the datasource module being called?'
            print 'Data source type is {0}'.format(self.data_source_type)
            sys.exit(1)





