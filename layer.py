import os
import sys
import settings
import arcpy
import warnings
import shutil
import time
from datetime import datetime
import archive
import json
import arcpy_metadata
import util

class Layer(object):

    def __init__(self, layerdef):
        """ A general Layer class
        :param layerdef: A Layer definition dictionary
        :return:
        """

        print 'starting layer class'
        
        self.type = "Layer"

        self._name = None
        self.name = layerdef['tech_title']

        self._scratch_workspace = None
        self.scratch_workspace = os.path.join(settings.settings['paths']['scratch_workspace'], self.name)

        self._zip_workspace = None
        self.zip_workspace = os.path.join(settings.settings['paths']['scratch_workspace'], self.name, 'zip')

        self._source = None
        self.source = layerdef['source']

        self._export_folder = None
        self.export_folder = os.path.join(self.scratch_workspace, 'export')

        self.metadata = self._get_metadata(self.source)

        self._esri_output_epsg = None
        self.esri_output_epsg = int(layerdef['esri_output_epsg'])
        
        self._esri_service_output = None
        self.esri_service_output = layerdef['esri_service_output']

        self._cartodb_service_output = None
        self.cartodb_service_output = layerdef['cartodb_service_output']

##        self._odp_wgs84_download = None
##        self.odp_wgs84_download = layerdef['odp_wgs84_download']
##        
##        self._odp_local_download = None
##        self.odp_local_download = layerdef['odp_local_download']
        
        self._archive_zip_output = None
        self.archive_zip_output = layerdef['archive_zip_output']

        self._transformation = None
        self.transformation = layerdef['transformation']

    # Validate name
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, n):
        if not n:
            warnings.warn("Name cannot be empty", Warning)
        self._name = n

    # Validate Scratch workspace
    @property
    def scratch_workspace(self):
        return self._scratch_workspace

    @scratch_workspace.setter
    def scratch_workspace(self, s):
        if os.path.exists(s):
            shutil.rmtree(s)
        os.mkdir(s)
        self._scratch_workspace = s

    @property
    def export_folder(self):
        return self._export_folder

    @export_folder.setter
    def export_folder(self, e):
        if os.path.exists(e):
            shutil.rmtree(e)
        os.mkdir(e)
        self._export_folder = e

    # Validate Zip workspace
    @property
    def zip_workspace(self):
        return self._zip_workspace

    @zip_workspace.setter
    def zip_workspace(self, z):
        if os.path.exists(z):
            shutil.rmtree(z)
        os.mkdir(z)
        self._zip_workspace = z

    # Validate esri_service_output
    @property
    def esri_service_output(self):
        return self._esri_service_output

    @esri_service_output.setter
    def esri_service_output(self, e):
        if not e:
            warnings.warn("esri_service_output cannot be empty", Warning)
        elif not arcpy.Exists(e):
            warnings.warn("Workspace for esri_service_output {0!s} does not exist".format(e), Warning)
        else:
            desc = arcpy.Describe(e)
            if desc.dataType not in ["Workspace", "Folder", "FeatureDataset"]:
                print desc.dataType
                warnings.warn("{0!s} is not a Workspace".format(e), Warning)
        self._esri_service_output = e
        
    # Validate esri_output_epsg
    @property
    def esri_output_epsg(self):
        return self._esri_output_epsg

    @esri_output_epsg.setter
    def esri_output_epsg(self, g):
        if not g:
            warnings.warn("esri_output_epsg cannot be empty", Warning)
        else:
            try:
                arcpy.SpatialReference(g)
            except:
                print "esri_output_epsg must be a valid EPSG code"
                sys.exit(2)
            
        self._esri_output_epsg = g

    # Validate source
    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, s):
        if s is not None:
            if not arcpy.Exists(s):
                warnings.warn("Cannot find source {0!s}".format(s), Warning)
            else:
                #Split the drive from the path returns (Letter and :), then take only the letter and lower it
                drive = os.path.splitdrive(s)[0][0].lower()

                if drive in [x[0].lower() for x in settings.settings["bucket_drives"].values()]:
                    out_data = os.path.join(self.scratch_workspace, os.path.basename(s))
                    print 'Input data source is in S3-- copying to {0}\r\n'.format(out_data)
                    
                    arcpy.Copy_management(s, out_data)
                    s = out_data
                elif drive not in ['c','d']:
                    warnings.warn("Are you sure this dataset is local to this machine? \
                    Make sure to list all S3 bucket drives in the config/settings.init", Warning)
        self._source = s

    # Validate transformation
    @property
    def transformation(self):
        return self._transformation

    @transformation.setter
    def transformation(self, t):
        from_desc = arcpy.Describe(self.source)
        from_srs = from_desc.spatialReference
        
        to_srs = arcpy.SpatialReference(self.esri_output_epsg)
        
        if from_srs.GCS != to_srs.GCS:
            if not t:
                warnings.warn("No transformation defined", Warning)
            else:
                extent = from_desc.extent
                transformations = arcpy.ListTransformations(from_srs, to_srs, extent)
                if self.transformation not in transformations:
                    warnings.warn("Transformation {0!s}: not compatible with in- and output spatial reference or extent".format(self.transformation), Warning)
                    
        self._transformation = t

    # Validate bucket
    @property
    def archive_zip_output(self):
        return self._archive_zip_output

    @archive_zip_output.setter
    def archive_zip_output(self, a):
        if not a:
            warnings.warn("archive_zip_output cannot be empty", Warning)
        self._archive_zip_output = a

    def _archive(self, input_file, local=False):

        zip_folder = os.path.join(self.archive_zip_output, "zip")
        archive_folder = os.path.join(self.archive_zip_output, "archive")

        print "Starting to zip the input file: {0}".format(input_file)

        zip_file = archive.zip_file(input_file, self.zip_workspace, local)
        source = os.path.join(self.zip_workspace, zip_file)

        if not os.path.exists(zip_folder):
            util.mkdir_p(zip_folder)
        dst = os.path.join(zip_folder, zip_file)
        print "Copy ZIP to {0!s} for download".format(zip_folder)
        shutil.copy(source, dst)

        if not os.path.exists(archive_folder):
            util.mkdir_p(archive_folder)
        ts = time.time()
        timestamp = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
        dst = os.path.join(archive_folder, "{0!s}_{1!s}.zip".format(os.path.splitext(zip_file)[0], timestamp))
        print "Copy ZIP and date stamp it to the archive: {0!s}".format(archive_folder)
        shutil.copy(source, dst)

        #why do we need the un-archvied version??
##        input_f = os.path.basename(input_file)
##        
##        arcpy.Copy_management(input_file, os.path.join(self.archive_zip_output, input_f))
        
    def _project_to_wgs84(self):

        wgs84_folder = os.path.join(self.export_folder, "wgs84")
        self.wgs84_file = os.path.join(wgs84_folder, self.name + "_wgs84.shp")
        
        if not os.path.exists(wgs84_folder):
            os.mkdir(wgs84_folder)

        sr_wgs84 = arcpy.SpatialReference(4326)

        print 'Projecting {0} output to WGS84'.format(self.export_file)
        print 'Output: {0}'.format(self.wgs84_file)

        if self.transformation is None or self.transformation == '':
            arcpy.Project_management(self.export_file, self.wgs84_file, sr_wgs84)
        else:
            arcpy.Project_management(self.export_file, self.wgs84_file, sr_wgs84, self.transformation)

        return

    def isWGS84(self, inputDataset):
        srAsString = arcpy.Describe(inputDataset).spatialReference.exporttostring()

        firstElement = srAsString.split(',')[0]

        if 'GEOGCS' in firstElement and 'GCS_WGS_1984' in firstElement:
            return True
        else:
            return False

    def add_field_and_calculate(self, fc, fieldName, fieldType, fieldPrecision, fieldVal):
        
        arcpy.AddField_management(fc, fieldName, fieldType, fieldPrecision)

        if fieldType in ['TEXT', 'DATE']:
            fieldVal = "'{0}'".format(fieldVal)

        print fieldName, fieldVal
        arcpy.CalculateField_management(fc, fieldName, fieldVal, "PYTHON")

    def _get_metadata(self, layer):

        md = arcpy_metadata.MetadataEditor(layer)

        cache_file = settings.settings['metadata']['cache']
        with open(cache_file) as c:
            data = c.read()

        md_gspread = json.loads(data)

        if self.name in md_gspread.keys():

            md.title.set(md_gspread[self.name]["Title"])
##            md.locals['english'].title.set(md_gspread[self.name]["Translated_Title"])
            md.title.set(md_gspread[self.name]["Translated_Title"])
            md.purpose.set(md_gspread[self.name]["Function"])
            md.abstract.set(md_gspread[self.name]["Overview"])
##            md.locals['english'].abstract.set(md_gspread[self.name]["Translated Overview"])
            md.abstract.set(md_gspread[self.name]["Translated Overview"])
            #  md_gspread[self.name]["category"]
            md.tags.add(util.csl_to_list(md_gspread[self.name]["Tags"]))
            md.extent_description.set(md_gspread[self.name]["Geographic Coverage"])
            md.last_update.set(md_gspread[self.name]["Date of Content"])
            md.update_frequency_description.set(md_gspread[self.name]["Frequency of Updates"])
            #  md.credits.set(md_gspread[self.name]["credits"])
            md.citation.set(md_gspread[self.name]["Citation"])
            md.limitation.set(md_gspread[self.name]["License"])
            md.supplemental_information.set(md_gspread[self.name]["Cautions"])
            md.source.set(md_gspread[self.name]["Source"])
            md.scale_resolution.set(md_gspread[self.name]["Resolution"])

        else:
##            raise RuntimeError("No Metadata for layer {0!s}".format(self.name))
            warnings.warn("No Metadata for layer {0!s}".format(self.name), Warning)
##            md = None

        return md

    def update_metadata(self):
        pass

