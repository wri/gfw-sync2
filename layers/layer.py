import json
import logging
import os
import shutil
import sys

import arcpy
import arcpy_metadata

from utilities import archive
from utilities import cartodb, settings
from utilities import util


class Layer(object):

    def __init__(self, layerdef):
        """ A general Layer class
        :param layerdef: A Layer definition dictionary
        :return:
        """

        logging.debug('starting layer class')

        self._name = None
        self.name = layerdef['tech_title']

        self._gfw_env = None
        self.gfw_env = layerdef['gfw_env']

        self._scratch_workspace = None
        self.scratch_workspace = os.path.join(settings.get_settings(self.gfw_env)['paths']['scratch_workspace'], self.name)

        self._type = None
        self.type = layerdef['type']

        self._source = None
        self.source = layerdef['source']

        self._esri_service_output = None
        self.esri_service_output = layerdef['esri_service_output']

        self._esri_merge_where_field = None
        self.esri_merge_where_field = layerdef['esri_merge_where_field']

        self._cartodb_service_output = None
        self.cartodb_service_output = layerdef['cartodb_service_output']

        self._cartodb_merge_where_field = None
        self.cartodb_merge_where_field = layerdef['cartodb_merge_where_field']

        self._delete_features_input_where_clause = None
        self.delete_features_input_where_clause = layerdef['delete_features_input_where_clause']

        self._archive_output = None
        self.archive_output = layerdef['archive_output']

        self._download_output = None
        self.download_output = layerdef['download_output']

        self._transformation = None
        self.transformation = layerdef['transformation']

        self._layerspec_maxdate_field_source = None
        self.layerspec_maxdate_field_source = layerdef['layerspec_maxdate_field_source']

        self._global_layer = None
        self.global_layer = layerdef['global_layer']

        self._add_country_value = None
        self.add_country_value = layerdef['add_country_value']

    # Validate name
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, n):
        if not n:
            logging.debug("Name cannot be empty")
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

    # Validate esri_service_output
    @property
    def esri_service_output(self):
        return self._esri_service_output

    @esri_service_output.setter
    def esri_service_output(self, e):
        if not arcpy.Exists(e):
            logging.error("esri_service_output {0} does not exist".format(e))
            sys.exit(1)

        self._esri_service_output = e

    # Validate cartodb_service_output
    @property
    def cartodb_service_output(self):
        return self._cartodb_service_output

    @cartodb_service_output.setter
    def cartodb_service_output(self, c):
        if not c:
            logging.debug("No cartodb output specified")
            c = None
        else:
            if not cartodb.cartodb_check_exists(c, self.gfw_env):
                logging.error("Table {0} does not exist in cartodb. Exiting now".format(c))
                sys.exit(1)
            if 'staging' in c:
                logging.error("The word 'staging' should not be in cartodb_service_output. "
                              "May cause conflicts with prod data. Exiting now.")
                sys.exit(1)
        self._cartodb_service_output = c

    # Validate esri_merge_where_field
    @property
    def esri_merge_where_field(self):
        return self._esri_merge_where_field

    @esri_merge_where_field.setter
    def esri_merge_where_field(self, m):
        if m:
            if m not in util.list_fields(self.source, self.gfw_env):
                logging.debug("Where clause field {0} specified for esri_merge_where_field but "
                              "field not in source dataset".format(m))

            if m not in util.list_fields(self.esri_service_output, self.gfw_env):
                logging.error("Where clause field {0} specified for esri_merge_where_field but "
                              "field not in esri_service_output. Data from this field will not be"
                              "appended due to the NO_TEST approach. Exiting ".format(m))
                sys.exit(1)

        else:
            m = None
        self._esri_merge_where_field = m


    # Validate cartodb_merge_where_field
    @property
    def cartodb_merge_where_field(self):
        return self._cartodb_merge_where_field

    @cartodb_merge_where_field.setter
    def cartodb_merge_where_field(self, c):
        if c:
            if c not in util.list_fields(self.source, self.gfw_env):
                logging.debug("Where clause field {0} specified for cartodb_merge_where_field but field not "
                              "in source dataset".format(c))

            if c not in util.list_fields(self.esri_service_output, self.gfw_env):
                logging.error("Where clause field {0} specified for cartodb_merge_where_field but "
                              "field not in cartodb_service_output. Data from this field will not be"
                              "appended due to the NO_TEST approach. Exiting ".format(c))
                sys.exit(1)

        else:
            c = None
        self._cartodb_merge_where_field = c

    # Validate layerspec_maxdate_field_source
    @property
    def layerspec_maxdate_field_source(self):
        return self._layerspec_maxdate_field_source

    @layerspec_maxdate_field_source.setter
    def layerspec_maxdate_field_source(self, l):
        if l:
            if l not in util.list_fields(self.source, self.gfw_env):
                logging.debug("Date field {0} specified for layerspec_maxdate_field_source but "
                              "field not in source dataset".format(l))
        else:
            l = None
        self._layerspec_maxdate_field_source = l

    # Validate delete_features_input_where_clause
    @property
    def delete_features_input_where_clause(self):
        return self._delete_features_input_where_clause

    @delete_features_input_where_clause.setter
    def delete_features_input_where_clause(self, f):
        if f:
            if "'" not in f and '"' not in f:
                logging.debug("delete_features_input_where_clause {0} doesn't have quoted strings. "
                              "It probably should.".format(f))

            where_clause_field_name = f.split()[0].replace("'","").replace('"',"")

            if where_clause_field_name in util.list_fields(self.source, self.gfw_env):
                try:
                        arcpy.MakeFeatureLayer_management(self.source, self.name, f)

                        #Clean up temporary feature layer after we create it
                        arcpy.Delete_management(self.name)
                except:
                    logging.error("delete_features_input_where_clause '{0!s}' is invalide or delete FL failed".format(f))
                    sys.exit(1)

        self._delete_features_input_where_clause = f

    # Validate layer type
    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, t):
        self._type = t

    # Validate source
    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, s):
        try:
            # TODO remove this and find a better solution . . . ie why are there so many connections always?
            arcpy.ClearWorkspaceCache_management(r'Database Connections\gfw_test (gfw@localhost).sde')
        except:
            logging.debug('Failed to clear workspace cache')

        if not arcpy.Exists(s):
            logging.error("Cannot find source {0!s}. Exiting".format(s))
            sys.exit(1)

        # We can assume that the source is local if it's in SDE, and os.path.splitdrive
        # does not handle those pathnames well
        # TODO change this to check for off-site SDE databases
        if s[0:20] == 'Database Connections':
            pass

        else:
            #Split the drive from the path returns (Letter and :), then take only the letter and lower it
            drive = os.path.splitdrive(s)[0][0].lower()

            if drive in util.list_network_drives():
                out_data = os.path.join(self.scratch_workspace, os.path.basename(s))
                logging.info('Input data source is in S3-- copying to {0}\r\n'.format(out_data))

                arcpy.Copy_management(s, out_data)
                s = out_data

            elif drive not in ['c','d']:
                logging.info("Are you sure the source dataset is local to this machine? \
                It's not on drives C or D . . .")

        self._source = s

    # Validate transformation
    @property
    def transformation(self):
        return self._transformation

    @transformation.setter
    def transformation(self, t):
        from_desc = arcpy.Describe(self.source)
        from_srs = from_desc.spatialReference

        to_srs = arcpy.Describe(self.esri_service_output).spatialReference

        if from_srs.GCS != to_srs.GCS:
            if not t:
                logging.debug("No transformation defined")
            else:
                extent = from_desc.extent
                transformations = arcpy.ListTransformations(from_srs, to_srs, extent)
                if self.transformation not in transformations:
                    logging.info("Transformation {0!s}: not compatible with in- and output "
                                  "spatial reference or extent".format(self.transformation))

                    t = None

        del from_desc
        del to_srs
        self._transformation = t

    # Validate archive folder
    @property
    def archive_output(self):
        return self._archive_output

    @archive_output.setter
    def archive_output(self, a):
        if not a:
            logging.error("archive_output cannot be empty. Exiting now.")
            sys.exit(1)

        archive_dir = os.path.dirname(a)

        if not os.path.exists(archive_dir):
            util.mkdir_p(archive_dir)

        self._archive_output = a

    # Validate download folder
    @property
    def download_output(self):
        return self._download_output

    @download_output.setter
    def download_output(self, d):
        if not d:
            logging.error("download_output cannot be empty. Exiting now")
            sys.exit(1)

        download_dir = os.path.dirname(d)

        if not os.path.exists(download_dir):
            util.mkdir_p(download_dir)

        self._download_output = d

    # Validate global layer
    @property
    def global_layer(self):
        return self._global_layer

    @global_layer.setter
    def global_layer(self, g):
        if not g:
            g = None

        self._global_layer = g

    # Validate add_country_value
    @property
    def add_country_value(self):
        return self._add_country_value

    @add_country_value.setter
    def add_country_value(self, c):
        if not c:
            c = None

        else:
            try:
                settings.get_country_iso3_list()[c]

            except:
                logging.error("Country code {0} specified but not found in iso country list\n Exiting now".format(c))
                sys.exit(1)

        self._add_country_value = c

    def _archive(self, input_fc, download_output, archive_output, sr_is_local):
        logging.debug('starting layer._archive')
        archive.zip_file(input_fc, self.scratch_workspace, download_output, archive_output, sr_is_local)

        return

    def isWGS84(self, inputDataset):
        logging.debug('starting layer.isWGS84')
        srAsString = arcpy.Describe(inputDataset).spatialReference.exporttostring()

        firstElement = srAsString.split(',')[0]

        if 'GEOGCS' in firstElement and 'GCS_WGS_1984' in firstElement:
            return True
        else:
            return False

    # TODO figure out metadata update process/requirements
    # TODO when should we call this??
    def _get_metadata(self, layer):
        logging.debug('_get_metadata')

        md = arcpy_metadata.MetadataEditor(layer)

        cache_file = settings.get_settings(self.gfw_env)['metadata']['cache']
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
            logging.debug("No Metadata for layer {0!s}".format(self.name))
##            md = None

        return md

    def update_metadata(self):
        pass

