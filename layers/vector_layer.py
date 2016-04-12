__author__ = 'Thomas.Maschler'

import os
import sys
import time
import logging
import arcpy

from layer import Layer
from utilities import cartodb
from utilities import util


class VectorLayer(Layer):
    """
    Vector layer class. Inherits from Layer
    """

    def __init__(self, layerdef):
        logging.debug('starting vectorlayer')

        super(VectorLayer, self).__init__(layerdef)

        self._sde_workspace = None
        self.sde_workspace = os.path.dirname(os.path.dirname(self.esri_service_output))

    # Validate sde_workspace
    @property
    def sde_workspace(self):
        return self._sde_workspace

    @sde_workspace.setter
    def sde_workspace(self, s):
        logging.debug('SDE workspace: ' + s)
        if not s:
            self._sde_workspace = ""
        else:
            if arcpy.Exists(s) and os.path.splitext(s)[1] == '.sde':
                self._sde_workspace = s
            else:
                logging.debug("sde_workspace path is invalid")

    def append_to_esri_source(self, input_fc, esri_output_fc, input_where_field, fms=None):

        """  Append new features to Vector Layer
        :param fms: an arcpy fieldmap
        :return: Nothing
        """
        logging.info('Starting vector_layer.append_to_esri_source for {0}'.format(self.name))

        # Check if source SR matches esri_service_output SR
        from_srs = arcpy.Describe(input_fc).spatialReference
        to_srs = arcpy.Describe(esri_output_fc).spatialReference

        if to_srs.exportToString() != from_srs.exportToString():

            # Project if not
            logging.debug('Source SR of {0} does not match esri_service_output of {1}'.format(input_fc, esri_output_fc))
            logging.debug('Projecting source data to temp FC before appending to esri_service_output')

            temp_proj_dataset = os.path.join(self.scratch_workspace, "src_proj.shp")

            if self.transformation:
                arcpy.env.geographicTransformations = self.transformation

            arcpy.Project_management(input_fc, temp_proj_dataset, to_srs, self.transformation, from_srs)

            fc_to_append = temp_proj_dataset

        else:
            fc_to_append = input_fc

        logging.debug('Creating a versioned FL from esri_service_output')
        arcpy.MakeFeatureLayer_management(esri_output_fc, "esri_service_output_fl")

        version_name = self.name + "_" + str(int(time.time()))
        arcpy.CreateVersion_management(self.sde_workspace, "sde.DEFAULT", version_name, "PRIVATE")

        arcpy.ChangeVersion_management("esri_service_output_fl", 'TRANSACTIONAL', 'gfw.' + version_name, '')

        # Build where clause
        esri_where_clause = self.build_update_where_clause(input_where_field)

        if esri_where_clause:
            logging.debug('Deleting features from esri_service_output based on input_where_field. ' \
                  'SQL statement: {0}'.format(esri_where_clause))

            arcpy.SelectLayerByAttribute_management("esri_service_output_fl", "NEW_SELECTION", esri_where_clause)

        else:
            logging.debug('No where clause for esri_service_output found; deleting all features before '
                          'appending from source')

        arcpy.DeleteRows_management("esri_service_output_fl")

        esri_output_pre_append_count = int(arcpy.GetCount_management("esri_service_output_fl").getOutput(0))
        input_feature_count = int(arcpy.GetCount_management(fc_to_append).getOutput(0))

        logging.debug('Starting to append to esri_service_output')
        arcpy.Append_management(fc_to_append, "esri_service_output_fl", "NO_TEST", fms, "")

        arcpy.ReconcileVersions_management(input_database=self.sde_workspace, reconcile_mode="ALL_VERSIONS",
                                           target_version="sde.DEFAULT", edit_versions='gfw.' + version_name,
                                           acquire_locks="LOCK_ACQUIRED", abort_if_conflicts="NO_ABORT",
                                           conflict_definition="BY_OBJECT", conflict_resolution="FAVOR_TARGET_VERSION",
                                           with_post="POST", with_delete="KEEP_VERSION", out_log="")

        arcpy.Delete_management("esri_service_output_fl")
        arcpy.DeleteVersion_management(self.sde_workspace, 'gfw.' + version_name)

        post_append_count = int(arcpy.GetCount_management(esri_output_fc).getOutput(0))

        if esri_output_pre_append_count + input_feature_count == post_append_count:
            logging.debug('Append successful based on sum of input features')
        else:
            logging.debug('esri_output_pre_append_count: {0}'.format(esri_output_pre_append_count))
            logging.debug('input_feature_count: {0}'.format(input_feature_count))
            logging.debug('post_append_count: {0}'.format(post_append_count))
            logging.error('Append failed, sum of input features does not match. Exiting')
            sys.exit(1)

        return

    def build_update_where_clause(self, input_field):

        if input_field:
            # Get unique values in specified where_clause field
            unique_values = list(set([x[0] for x in arcpy.da.SearchCursor(self.source, [input_field])]))

            unique_values_sql = "'" + "', '".join(unique_values) + "'"

            where_clause = """{0} IN ({1})""".format(input_field, unique_values_sql)

        else:
            where_clause = None

        return where_clause

    def archive_source(self):
        logging.info('Starting vector_layer.archive source for {0}'.format(self.name))
        archive_dir = os.path.dirname(self.archive_output)
        archive_src_dir = os.path.join(archive_dir, 'src')

        if not os.path.exists(archive_src_dir):
            os.mkdir(archive_src_dir)

        src_archive_output = os.path.join(archive_src_dir, os.path.basename(self.archive_output))
        self._archive(self.source, None, src_archive_output, False)

    def create_archive_and_download_zip(self):
        logging.info('Starting vector_layer.create_archive_and_download_zip for {0}'.format(self.name))

        # if source is in local projection, create separate download
        if not self.isWGS84(self.source):

            download_basename = self.name + '.shp'
            if os.path.basename(self.source) == download_basename:
                local_coords_source = self.source

            # If the name of the .shp is not correct (or it's not even a .shp; create it then zip
            else:
                local_coords_source = os.path.join(self.scratch_workspace, download_basename)
                arcpy.CopyFeatures_management(self.source, local_coords_source)

            # Create a separate _local.zip download file
            self._archive(local_coords_source, self.download_output, None, True)

        # Create an archive and a download file for final dataset (esri_service_output)
        self._archive(self.esri_service_output, self.download_output, self.archive_output, False)

    def add_country_code(self):
        if self.add_country_value:
            logging.info('Starting vector_layer.add_country_value for {0}, '
                         'country val {1}'.format(self.name, self._add_country_value))
            util.add_field_and_calculate(self.source, "country", 'TEXT', 3, self.add_country_value, self.gfw_env)

    def filter_source_dataset(self, input_wc):

        if input_wc:
            logging.info('Starting to vector_layer.filter_source_dataset for {0} with wc {1}'.format(self.name, input_wc))

            # If we are going to filter the source feature class, copy to a new location before deleting records
            # from it
            output_fc = os.path.join(self.scratch_workspace, os.path.basename(self.source))
            arcpy.CopyFeatures_management(self.source, output_fc)

            # Change input_fc to the fc we just created
            # Also set source to this fc, so that we use this from here on out and leave
            # the actual source data FC alone
            input_fc = output_fc
            self.source = output_fc

            # Make a feature layer from the input FC and delete any rows to filter using the input_wc
            arcpy.MakeFeatureLayer_management(self.source, "input_fl", input_wc)
            arcpy.DeleteRows_management("input_fl")
            arcpy.Delete_management("input_fl")

        else:
            logging.debug('No input where_clause found to filter source dataset')

    def update(self):
        self._update()

    def update_gfwid(self):
        logging.debug('starting vector_layer.update_gfwid for {0}'.format(self.name))

        if "gfwid" not in util.list_fields(self.source, self.gfw_env):
            arcpy.AddField_management(self.source, "gfwid", "TEXT", field_length=50, field_alias="GFW ID")

        arcpy.CalculateField_management(in_table=self.source,
                                        field="gfwid",
                                        expression="md5(!Shape!.WKT)",
                                        expression_type="PYTHON_9.3",
                                        code_block="import hashlib\n"
                                                   "def md5(shape):\n"
                                                   "   hash = hashlib.md5()\n"
                                                   "   hash.update(shape)\n"
                                                   "   return hash.hexdigest()")

    def update_layerspec_maxdate(self):

        if self.layerspec_maxdate_field_source:
            logging.debug("update layer spec max date")
            sql = "UPDATE layerspec set maxdate = (SELECT max({0}) FROM {1}) WHERE table_name='{1}'".format(self.layerspec_maxdate_field_source,self.cartodb_service_output)
            cartodb.cartodb_sql(sql, self.gfw_env)

    def sync_cartodb(self, input_fc, cartodb_output_fc, input_where_field):
        logging.info('Starting vector_layer.sync_cartodb for {0}. Output {1}, '
                     'wc {2}'.format(os.path.basename(input_fc), cartodb_output_fc, input_where_field))

        cartodb_where_clause = self.build_update_where_clause(input_where_field)

        cartodb.cartodb_sync(input_fc, cartodb_output_fc, cartodb_where_clause, self.gfw_env, self.scratch_workspace)

        self.update_layerspec_maxdate()

    def _update(self):

        self.archive_source()

        self.filter_source_dataset(self.delete_features_input_where_clause)

        self.update_gfwid()

        self.add_country_code()

        self.append_to_esri_source(self.source, self.esri_service_output, self.esri_merge_where_field)

        self.create_archive_and_download_zip()

        self.sync_cartodb(self.source, self.cartodb_service_output, self.cartodb_merge_where_field)
