import logging
import os
import shutil
import sys
import arcpy

from utilities import archive
from utilities import cartodb, settings
from utilities import util
from utilities import field_map


class Layer(object):
    """ A general Layer class. Used to pull information from the google sheet config table and pass it to
    various layer update function
    :param layerdef: A Layer definition dictionary
    :return:
    """
    
    def __init__(self, layerdef):
        logging.debug('starting layer class')

        self._name = None
        self.name = layerdef['tech_title']

        self._gfw_env = None
        self.gfw_env = layerdef['gfw_env']

        self._scratch_workspace = None
        self.scratch_workspace = os.path.join(settings.get_settings(self.gfw_env)['paths']['scratch_workspace'],
                                              self.name)

        self._layer_type = None
        self.layer_type = layerdef['type']

        self._field_map = None
        self.field_map = layerdef['field_map']

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
        """
        Ultimately used to build a unique where clause from the source data. If specified, any values in this field
        in the source dataset (i.e. the country field for a MEX layer) will be used to delete from the global output
        (DELETE FROM gfw_mining WHERE country = 'MEX') and then the source will be appended
        :param m: the merge feild
        :return:
        """
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
        """
        Ultimately used to build a unique where clause from the source data. If specified, any values in this field
        in the source dataset (i.e. the country field for a MEX layer) will be used to delete from the global output
        (DELETE FROM gfw_mining WHERE country = 'MEX') and then the source will be appended
        :param m: the merge feild
        :return:
        """
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

    # Validate delete_features_input_where_clause
    @property
    def delete_features_input_where_clause(self):
        return self._delete_features_input_where_clause

    @delete_features_input_where_clause.setter
    def delete_features_input_where_clause(self, f):
        """
        Used to filter the input dataset. If this exists, the source will be copied locally, then use this where
        clause to delete records before integrating with the rest of the update process
        :param f: the where clause
        :return:
        """
        if f:
            if "'" not in f and '"' not in f:
                logging.debug("delete_features_input_where_clause {0} doesn't have quoted strings. "
                              "It probably should.".format(f))

            where_clause_field_name = f.split()[0].replace("'", "").replace('"', "")

            if where_clause_field_name in util.list_fields(self.source, self.gfw_env):
                try:
                        arcpy.MakeFeatureLayer_management(self.source, self.name, f)

                        # Clean up temporary feature layer after we create it
                        arcpy.Delete_management(self.name)

                except arcpy.ExecuteError:
                    logging.error("delete_features_input_where_clause '{0!s}' is invalide "
                                  "or delete FL failed".format(f))
                    sys.exit(1)

        self._delete_features_input_where_clause = f

    # Validate layer_type
    @property
    def layer_type(self):
        return self._layer_type

    @layer_type.setter
    def layer_type(self, t):
        self._layer_type = t

    # Validate layer field_map
    @property
    def field_map(self):
        return self._field_map

    @field_map.setter
    def field_map(self, m):
        """
        Validates a field map if it exists
        :param m: D:\path\to\fieldmap.ini\{keyname}
        :return:
        """

        if m:
            # Insert this so that global_vector layers can pass validation checks
            # These layers may have fieldmaps associated, but they are really for the
            # input country vector datasets to use in meeting the schema of the global layers
            if self.layer_type == 'global_vector':
                m = None

            else:
                m = field_map.get_ini_dict(m)

        else:
            m = None

        self._field_map = m

    # Validate source
    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, s):
        """
        Validates source; can apply a fieldmap to an FC, also will copy locally if an external dataset
        :param s:
        :return:
        """
        if not arcpy.Exists(s):
            logging.error("Cannot find source {0!s} Exiting".format(s))
            sys.exit(1)

        # If there's a field map, use it as an input to the FeatureClassToFeatureClass tool and copy the data locally
        if self.field_map:
            s = field_map.ini_fieldmap_to_fc(s, self.name, self.field_map, self.scratch_workspace)

        # If there's not a field map, need to figure out what type of data source it is, and if it's local or not
        else:
            # This could be a folder, a gdb/mdb, a featuredataset, or an SDE database
            source_dirname = os.path.dirname(s)

            # we want to simply determine if this is local/not local so we can copy the datasource
            # first determine if our source dataset is within a featuredataset

            desc = arcpy.Describe(source_dirname)
            if hasattr(desc, "datasetType") and desc.datasetType == 'FeatureDataset':
                source_dirname = os.path.dirname(source_dirname)

            # Test if it's an SDE database
            try:
                server_address = arcpy.Describe(source_dirname).connectionProperties.server

                # If source SDE is localhost, don't need to worry about copying anywhere
                if server_address == 'localhost':
                    pass
                else:
                    s = util.copy_to_scratch_workspace(self.source, self.scratch_workspace)

            # Otherwise, just look at the drive letter to determine if it's local or not
            except AttributeError:

                # Split the drive from the path returns (Letter and :), then take only the letter and lower it
                drive = os.path.splitdrive(s)[0][0].lower()

                if drive in util.list_network_drives():
                    s = util.copy_to_scratch_workspace(self.source, self.scratch_workspace)

                elif drive not in ['c', 'd']:
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
        """
        If this is present, will be added to the feature class with the country field
        :param c: ISO3 country code
        :return:
        """
        if not c:
            c = None

        else:
            try:
                settings.get_country_iso3_list()[c]

            except KeyError:
                logging.error("Country code {0} specified but not found in iso country list\n Exiting now".format(c))
                sys.exit(1)

        self._add_country_value = c

    def _archive(self, input_fc, download_output, archive_output, sr_is_local=False):
        logging.debug('starting layer._archive')
        archive.zip_file(input_fc, self.scratch_workspace, download_output, archive_output, sr_is_local)

        return

    def cleanup(self):
        shutil.rmtree(self.scratch_workspace)
