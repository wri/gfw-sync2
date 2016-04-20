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
        logging.debug('Starting vectorlayer')

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
        if arcpy.Exists(s) and os.path.splitext(s)[1] == '.sde':
            self._sde_workspace = s
        else:
            logging.debug("sde_workspace path is invalid")

    def archive_source(self):
        """
        Creates an archive of the input data (listed in the config table under 'source' before the process begins
        :return:
        """
        logging.info('Starting vector_layer.archive source for {0}'.format(self.name))
        archive_dir = os.path.dirname(self.archive_output)
        archive_src_dir = os.path.join(archive_dir, 'src')

        if not os.path.exists(archive_src_dir):
            os.mkdir(archive_src_dir)

        src_archive_output = os.path.join(archive_src_dir, os.path.basename(self.archive_output))
        self._archive(self.source, None, src_archive_output)

    def filter_source_dataset(self, input_wc):
        """
        If the config table specifies a where_clause to apply as a filter to the source dataset, copy the source
        locally and then delete these rows. This copied dataset becomes the new source
        :param input_wc:
        :return:
        """
        if input_wc:
            logging.info('Starting vector_layer.filter_source_dataset for {0} with wc {1}'.format(self.name, input_wc))

            # If we are going to filter the source feature class, copy to a new location before deleting records
            # from it
            out_filename = os.path.splitext(os.path.basename(self.source))[0] + '.shp'
            output_fc = os.path.join(self.scratch_workspace, out_filename)
            arcpy.CopyFeatures_management(self.source, output_fc)

            # Delete records from this copied FC based on the input where clause
            arcpy.MakeFeatureLayer_management(output_fc, "input_fl", input_wc)
            arcpy.DeleteRows_management("input_fl")
            arcpy.Delete_management("input_fl")

            # Set the source to this new fc
            self.source = output_fc

        else:
            pass

    def update_gfwid(self):
        """
        For each row, take the hash of the well known text representation of the geometry
         This will be used in the API to cache analysis results for geometries previously analyzed
        :return:
        """
        logging.debug('Starting vector_layer.update_gfwid for {0}'.format(self.name))

        if "gfwid" not in util.list_fields(self.source, self.gfw_env):
            arcpy.AddField_management(self.source, "gfwid", "TEXT", field_length=50, field_alias="GFW ID")

        # Required to prevent calcuate field failures-- will likely fail to hash the !Shape! object if there are
        # null geometries
        logging.debug('Starting repair geometry')
        arcpy.RepairGeometry_management(self.source, "DELETE_NULL")

        logging.debug('Starting to calculate gfwid')
        arcpy.CalculateField_management(self.source, "gfwid", "md5(!Shape!.WKT)", "PYTHON_9.3",
                                        code_block="import hashlib\n"
                                                   "def md5(shape):\n"
                                                   "   hash = hashlib.md5()\n"
                                                   "   hash.update(shape)\n"
                                                   "   return hash.hexdigest()"
                                        )

    def add_country_code(self):
        """
        If there's a value for sell.add_country_value, add a country field and populate it
        :return:
        """
        if self.add_country_value:
            logging.info('Starting vector_layer.add_country_value for {0}, '
                         'country val {1}'.format(self.name, self.add_country_value))
            util.add_field_and_calculate(self.source, "country", 'TEXT', 3, self.add_country_value, self.gfw_env)

    def append_to_esri_source(self, input_fc, esri_output_fc, input_where_field):
        """
        Append to the esri source FC, including projecting to output coord system, creating a versioned FC of the
        output in SDE, deleting from that versioned FC based on a where_clause, appending the new data, and then
        posting that version
        :param input_fc: the input source data
        :param esri_output_fc: the output in SDE
        :param input_where_field: a field to build the where_clause
        :return:
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
        esri_where_clause = util.build_update_where_clause(input_fc, input_where_field)

        if esri_where_clause:
            logging.debug('Deleting features from esri_service_output feature layer based on input_where_field. '
                          'SQL statement: {0}'.format(esri_where_clause))

            arcpy.SelectLayerByAttribute_management("esri_service_output_fl", "NEW_SELECTION", esri_where_clause)

        else:
            logging.debug('No where clause for esri_service_output found; deleting all features before '
                          'appending from source')

        # If there's a where_clause, this will deleted selected rows
        # Otherwise, will truncate the table
        arcpy.DeleteRows_management("esri_service_output_fl")

        esri_output_pre_append_count = int(arcpy.GetCount_management("esri_service_output_fl").getOutput(0))
        input_feature_count = int(arcpy.GetCount_management(fc_to_append).getOutput(0))

        logging.debug('Starting to append to esri_service_output')
        arcpy.Append_management(fc_to_append, "esri_service_output_fl", "NO_TEST")

        arcpy.ReconcileVersions_management(input_database=self.sde_workspace, reconcile_mode="ALL_VERSIONS",
                                           target_version="sde.DEFAULT", edit_versions='gfw.' + version_name,
                                           acquire_locks="LOCK_ACQUIRED", abort_if_conflicts="NO_ABORT",
                                           conflict_definition="BY_OBJECT", conflict_resolution="FAVOR_TARGET_VERSION",
                                           with_post="POST", with_delete="KEEP_VERSION", out_log="")

        # For some reason need to run DeleteVersion_management here, will have errors if with_delete is used above
        arcpy.Delete_management("esri_service_output_fl")
        arcpy.DeleteVersion_management(self.sde_workspace, 'gfw.' + version_name)

        post_append_count = int(arcpy.GetCount_management(esri_output_fc).getOutput(0))

        if esri_output_pre_append_count + input_feature_count == post_append_count:
            logging.debug('Append successful based on sum of input features')
        else:
            logging.debug('esri_output_pre_append_count: {0}\input_feature_count: {1}\npost_append_count{2}\n'
                          'Append failed, sum of input features does not match. '
                          'Exiting'.format(esri_output_pre_append_count, input_feature_count, post_append_count))
            sys.exit(1)

        return

    def create_archive_and_download_zip(self):
        """
        Check if the source is wgs84; if not, create a local projection download zip
        Project to wgs84, and create a download zip and a timestamped archive zip
        :return:
        """
        logging.info('Starting vector_layer.create_archive_and_download_zip for {0}'.format(self.name))

        # if source is in local projection, create separate download
        if not util.is_wgs_84(self.source):

            download_basename = self.name + '.shp'
            if os.path.basename(self.source) == download_basename:
                local_coords_source = self.source

            # If the name of the .shp is not correct (or it's not even a .shp; create it then zip)
            else:
                local_coords_source = os.path.join(self.scratch_workspace, download_basename)
                arcpy.CopyFeatures_management(self.source, local_coords_source)

            # Create a separate _local.zip download file
            self._archive(local_coords_source, self.download_output, None, True)

        # Create an archive and a download file for final dataset (esri_service_output)
        self._archive(self.esri_service_output, self.download_output, self.archive_output)

    def sync_cartodb(self, input_fc, cartodb_output_fc, input_where_field):
        """
        Take the esri source dataset and append it to cartodb. If an input_where_field specified,
        use that to build a DELETE where clause to run against the cartoDB table before appending
        :param input_fc: the source dataset
        :param cartodb_output_fc: the output table in cartoDB
        :param input_where_field: a field to build a where clause from
        :return:
        """

        logging.info('Starting vector_layer.sync_cartodb for {0}. Output {1}, '
                     'wc {2}'.format(os.path.basename(input_fc), cartodb_output_fc, input_where_field))

        cartodb_where_clause = util.build_update_where_clause(input_fc, input_where_field)

        cartodb.cartodb_sync(input_fc, cartodb_output_fc, cartodb_where_clause, self.gfw_env, self.scratch_workspace)

    def update(self):
        """
        If a layer is type VectorLayer, this will be called by gfw-sync.py
        Otherwise, we'll make the _update() available to all objects that inherit from VectorLayer
        :return:
        """
        self._update()

    def _update(self):
        """
        Contains all relevant update functions for a vector layer
        :return:
        """

        self.archive_source()

        self.filter_source_dataset(self.delete_features_input_where_clause)

        self.update_gfwid()

        self.add_country_code()

        self.append_to_esri_source(self.source, self.esri_service_output, self.esri_merge_where_field)

        self.create_archive_and_download_zip()

        self.sync_cartodb(self.esri_service_output, self.cartodb_service_output, self.cartodb_merge_where_field)
