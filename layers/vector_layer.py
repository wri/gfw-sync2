__author__ = 'Thomas.Maschler'

import os
import sys
import time
import warnings

import arcpy

from layer import Layer
from utilities import cartodb
from utilities import google_sheet


class VectorLayer(Layer):
    """
    Vector layer class. Inherits from Layer
    """

    def __init__(self, layerdef):
        print 'starting vectorlayer'

        super(VectorLayer, self).__init__(layerdef)

        self._sde_workspace = None
        self.sde_workspace = os.path.dirname(os.path.dirname(self.esri_service_output))

    # Validate sde_workspace
    @property
    def sde_workspace(self):
        return self._sde_workspace

    @sde_workspace.setter
    def sde_workspace(self, s):
        print 'workspace: ' + s
        if not s:
            self._sde_workspace = ""
        else:
            if arcpy.Exists(s) and os.path.splitext(s)[1] == '.sde':
                self._sde_workspace = s
            else:
                warnings.warn("sde_workspace path is invalid", Warning)

    def append_to_esri_source(self, input_fc, esri_output_fc, input_where_field, fms=None):

        """  Append new features to Vector Layer
        :param fms: an arcpy fieldmap
        :return: Nothing
        """

        # Check if source SR matches esri_service_output SR
        from_srs = arcpy.Describe(input_fc).spatialReference
        to_srs = arcpy.Describe(esri_output_fc).spatialReference

        if to_srs.exportToString() != from_srs.exportToString():

            # Project if not
            print 'Source SR of {0} does not match esri_service_output of {1}'.format(input_fc, esri_output_fc)
            print 'Projecting source data to temp FC before appending to esri_service_output'

            temp_proj_dataset = os.path.join(self.scratch_workspace, "src_proj.shp")

            if self.transformation:
                arcpy.env.geographicTransformations = self.transformation

            arcpy.Project_management(input_fc, temp_proj_dataset, to_srs, self.transformation, from_srs)

            fc_to_append = temp_proj_dataset

        else:
            print 'SR of source dataset matches esri_service_output; not projecting'
            fc_to_append = input_fc

        print 'Creating a versioned FL from esri_service_output'
        arcpy.MakeFeatureLayer_management(esri_output_fc, "esri_service_output_fl")

        version_name = self.name + "_" + str(int(time.time()))
        arcpy.CreateVersion_management(self.sde_workspace, "sde.DEFAULT", version_name, "PRIVATE")

        arcpy.ChangeVersion_management("esri_service_output_fl", 'TRANSACTIONAL', 'gfw.' + version_name, '')

        # Build where clause
        esri_where_clause = self.build_update_where_clause(input_where_field)

        if esri_where_clause:
            print 'Deleting features from esri_service_output based on input_where_field. ' \
                  'SQL statement: {0}'.format(esri_where_clause)

            arcpy.SelectLayerByAttribute_management("esri_service_output_fl", "NEW_SELECTION", esri_where_clause)

        else:
            print 'No where clause for esri_service_output found; deleting all features before appending from source'

        arcpy.DeleteRows_management("esri_service_output_fl")

        esri_output_pre_append_count = int(arcpy.GetCount_management("esri_service_output_fl").getOutput(0))
        input_feature_count = int(arcpy.GetCount_management(fc_to_append).getOutput(0))

        print 'Starting to append to esri_service_output'
        arcpy.Append_management(fc_to_append, "esri_service_output_fl", "NO_TEST", fms, "")

        self.post_version(version_name)

        post_append_count = int(arcpy.GetCount_management(esri_output_fc).getOutput(0))

        if esri_output_pre_append_count + input_feature_count == post_append_count:
            print 'Append successful based on sum of input features'
        else:
            print 'Append failed, sum of input features does not match. Exiting'
            print 'esri_output_pre_append_count: {0}'.format(esri_output_pre_append_count)
            print 'input_feature_count: {0}'.format(input_feature_count)
            print 'post_append_count: {0}'.format(post_append_count)
            sys.exit(1)

        return


    def build_update_where_clause(self, input_field):

        if input_field:

            # Get unique values in specified where_clause field
            uniqueValues = list(set([x[0] for x in arcpy.da.SearchCursor(self.source, [input_field])]))

            uniqueValues_SQL = "'" + "', '".join(uniqueValues) + "'"

            where_clause = """{0} IN ({1})""".format(input_field, uniqueValues_SQL)

        else:
            where_clause = None

        return where_clause

            # arcpy.SimplifyPolygon_cartography(final_dataset,
            #                       self.export_file,
            #                       "POINT_REMOVE",
            #                       "10 Meters",
            #                       "0 Unknown",
            #                       "RESOLVE_ERRORS",
            #                       "KEEP_COLLAPSED_POINTS")


    def post_version(self, input_version):
        arcpy.ReconcileVersions_management(input_database=self.sde_workspace, reconcile_mode="ALL_VERSIONS",
                                           target_version="sde.DEFAULT", edit_versions='gfw.' + input_version,
                                           acquire_locks="LOCK_ACQUIRED", abort_if_conflicts="NO_ABORT",
                                           conflict_definition="BY_OBJECT", conflict_resolution="FAVOR_TARGET_VERSION",
                                           with_post="POST", with_delete="KEEP_VERSION", out_log="")


        arcpy.Delete_management("esri_service_output_fl")

        arcpy.DeleteVersion_management(self.sde_workspace, 'gfw.' + input_version)


    def archive_source(self):
        archive_dir = os.path.dirname(self.archive_output)
        archive_src_dir = os.path.join(archive_dir, 'src')

        if not os.path.exists(archive_src_dir):
            os.mkdir(archive_src_dir)

        src_archive_output = os.path.join(archive_src_dir, os.path.basename(self.archive_output))
        self._archive(self.source, None, src_archive_output, False)


    def create_archive_and_download_zip(self):

        # if source is in local projection, create separate download
        if not self.isWGS84(self.source):

            source_with_download_name = os.path.join(os.path.dirname(self.source), self.name + '.shp')

            if self.source == source_with_download_name:
                pass

            else:
                arcpy.CopyFeatures_management(self.source, source_with_download_name)
                # self.source = source_with_download_name

            self._archive(source_with_download_name, self.download_output, None, True)

            # arcpy.Delete_management(source_with_download_name)

        # Create an archive and a download file for final dataset (esri_service_output)
        self._archive(self.esri_service_output, self.download_output, self.archive_output, False)


    def add_country_code(self):
        if self.add_country_value:
            self.add_field_and_calculate(self.source, "country", 'TEXT', 3, self.add_country_value)


    def filter_source_dataset(self, input_wc):

        if input_wc:

            # If we are going to filter the source feature class, copy to a new location before deleting records
            # from it
            output_fc = os.path.join(self.scratch_workspace, os.path.basename(self.source))
            arcpy.CopyFeatures_management(self.source, output_fc)

            # Change input_fc to the fc we just created
            # Also set source to this fc, so that we use this from here on out and leave
            # the actual source data FC alone
            input_fc = output_fc
            self.source = output_fc

            arcpy.MakeFeatureLayer_management(self.source, "input_fl", input_wc)

            arcpy.DeleteRows_management("input_fl")

        else:
            print 'No input where_clause found to filter source dataset'


    def update(self):
        self._update()


    def update_gfwid(self):

        if "gfwid" not in self.list_fields(self.source):
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
            print "update layer spec max date"
            sql = "UPDATE layerspec set maxdate = (SELECT max({0}) FROM {1}) WHERE table_name='{1}'".format(self.layerspec_maxdate_field_source,self.cartodb_service_output)
            cartodb.cartodb_sql(sql, self.gfw_env)


    def sync_cartodb(self, input_fc, cartodb_output_fc, input_where_field):

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
