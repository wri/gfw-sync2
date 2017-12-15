__author__ = 'Thomas.Maschler'

import os
import sys
import time
import logging
import arcpy
import shutil

from layer import Layer
from utilities import cartodb
from utilities import util
from utilities import arcgis_server


class VectorLayer(Layer):
    """
    Vector layer class. Inherits from Layer
    """

    def __init__(self, layerdef):
        logging.debug('Starting vectorlayer')

        super(VectorLayer, self).__init__(layerdef)

        self.update_where_clause = None

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

    def delete_and_append(self):

        logging.info('Starting delete and append for {0}'.format(self.name))
        arcpy.DeleteRows_management(self.esri_service_output)
        arcpy.Append_management(self.source, self.esri_service_output)

        logging.info('restarting service for {0}'.format(self.name))
        service_dict = {'gran_chaco_deforestation': 'forest_change'}
        service_name = service_dict[self.name]

        service = service_name

        for i in range(0, 2):
            arcgis_server.set_service_status(service, 'stop')
            arcgis_server.set_service_status(service, 'start')

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
            out_filename = os.path.splitext(os.path.basename(self.source))[0]
            output_fc = os.path.join(self.scratch_workspace, out_filename).replace('.', '_') + '.shp'
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

    def build_update_where_clause(self, input_fc, input_where_field):
        """
        Build the where clause to use when deleting from/adding to the esri and cartodb output datasets
        :param input_fc: the input FC
        :param input_where_field: a field used to build the where_clause-- all values in the field will be selected
        :return:
        """
        # Build where clause
        self.update_where_clause = util.build_update_where_clause(input_fc, input_where_field)

    def append_to_esri_source(self, input_fc, esri_output_fc, input_where_clause):
        """
        Append to the esri source FC, including projecting to output coord system, creating a versioned FC of the
        output in SDE, deleting from that versioned FC based on a where_clause, appending the new data, and then
        posting that version
        :param input_fc: the input source data
        :param esri_output_fc: the output in SDE
        :param input_where_clause: where clause used to add to/delete from the output esri FC
        :return:
        """

        logging.info('Starting vector_layer.append_to_esri_source for {0}'.format(self.name))

        fc_to_append = self.project_to_output_srs(input_fc, esri_output_fc)
        logging.info('appending {} to {}'.format(input_fc, esri_output_fc))

        logging.debug('Creating a versioned FL from esri_service_output')
        arcpy.MakeFeatureLayer_management(esri_output_fc, "esri_service_output_fl")

        version_name = self.name + "_" + str(int(time.time()))

        sde_workspace = os.path.dirname(esri_output_fc)
        desc = arcpy.Describe(sde_workspace)
        if hasattr(desc, "datasetType") and desc.datasetType == 'FeatureDataset':
            sde_workspace = os.path.dirname(sde_workspace)

        del desc

        if os.path.splitext(sde_workspace)[1] != '.sde':
            logging.error('Could not find proper SDE workspace. Exiting.')
            sys.exit(1)

        arcpy.CreateVersion_management(sde_workspace, "sde.DEFAULT", version_name, "PRIVATE")
        arcpy.ChangeVersion_management("esri_service_output_fl", 'TRANSACTIONAL', 'gfw.' + version_name, '')

        if input_where_clause:
            logging.debug('Deleting features from esri_service_output feature layer based on input_where_clause. '
                          'SQL statement: {0}'.format(input_where_clause))

            arcpy.SelectLayerByAttribute_management("esri_service_output_fl", "NEW_SELECTION", input_where_clause)

            # Delete the features selected by the input where_clause
            arcpy.DeleteRows_management("esri_service_output_fl")

        else:
            logging.debug('No where clause for esri_service_output found; deleting all features before '
                          'appending from source')

            # commented out at this already happens below with wc
            # arcpy.MakeFeatureLayer_management("esri_service_output_fl", "fl_to_delete")
            # arcpy.DeleteRows_management("fl_to_delete")
            # arcpy.Delete_management("fl_to_delete")

            sde_sql_conn = arcpy.ArcSDESQLExecute(sde_workspace)
            esri_fc_name = os.path.basename(esri_output_fc)
            print esri_fc_name

            # why this, exactly?
            # there's also a lbr_plantations_old feature class (for some reason)
            # and lbr_plantation_evw points to that.
            # I don't know why and I don't have time to fix it
            if esri_fc_name != 'gfw_countries.gfw.lbr_plantations':
                esri_fc_name += '_evw'

            # Find the min and max OID values
            to_delete_oid_field = [f.name for f in arcpy.ListFields(esri_output_fc) if f.type == 'OID'][0]

            sql = 'SELECT min({0}), max({0}) from {1}'.format(to_delete_oid_field, esri_fc_name)
            to_delete_min_oid, to_delete_max_oid = sde_sql_conn.execute(sql)[0]

            # If there are features to delete, do it
            if to_delete_min_oid and to_delete_max_oid:

                for wc in util.generate_where_clause(to_delete_min_oid, to_delete_max_oid, to_delete_oid_field, 1000):

                    logging.debug('Deleting features with {0}'.format(wc))
                    arcpy.MakeFeatureLayer_management("esri_service_output_fl", "fl_to_delete", wc)

                    arcpy.DeleteRows_management("fl_to_delete")
                    arcpy.Delete_management("fl_to_delete")

            else:
                pass

        esri_output_pre_append_count = int(arcpy.GetCount_management("esri_service_output_fl").getOutput(0))
        input_feature_count = int(arcpy.GetCount_management(fc_to_append).getOutput(0))

        logging.debug('Starting to append to esri_service_output')

        # don't need to batch append if it's coming from an SDE data source
        # these are used exclusively by country-vector layers
        # and the data is generally small, compared to things like WDPA
        if 'sde' in fc_to_append:
            logging.debug("Appending all features from {}- no wc because it's an SDE input".format(fc_to_append))

            arcpy.MakeFeatureLayer_management(fc_to_append, "fl_to_append")
            arcpy.Append_management("fl_to_append", "esri_service_output_fl", "NO_TEST")

            arcpy.Delete_management("fl_to_append")

        else:
            # Find the min and max OID values
            to_append_oid_field = [f.name for f in arcpy.ListFields(fc_to_append) if f.type == 'OID'][0]
            to_append_min_oid, to_append_max_oid = cartodb.ogrinfo_min_max(fc_to_append, to_append_oid_field)

            for wc in util.generate_where_clause(to_append_min_oid, to_append_max_oid, to_append_oid_field, 1000):

                logging.debug('Appending features with {0}'.format(wc))
                arcpy.MakeFeatureLayer_management(fc_to_append, "fl_to_append", wc)

                arcpy.Append_management("fl_to_append", "esri_service_output_fl", "NO_TEST")
                arcpy.Delete_management("fl_to_append")

        logging.debug('Append finished, starting to reconcile versions')

        arcpy.ReconcileVersions_management(input_database=sde_workspace, reconcile_mode="ALL_VERSIONS",
                                           target_version="sde.DEFAULT", edit_versions='gfw.' + version_name,
                                           acquire_locks="LOCK_ACQUIRED", abort_if_conflicts="NO_ABORT",
                                           conflict_definition="BY_OBJECT", conflict_resolution="FAVOR_TARGET_VERSION",
                                           with_post="POST", with_delete="KEEP_VERSION", out_log="")

        logging.debug('Deleting temporary FL and temporary version')

        # For some reason need to run DeleteVersion_management here, will have errors if with_delete is used above
        arcpy.Delete_management("esri_service_output_fl")
        arcpy.DeleteVersion_management(sde_workspace, 'gfw.' + version_name)

        post_append_count = int(arcpy.GetCount_management(esri_output_fc).getOutput(0))

        if esri_output_pre_append_count + input_feature_count == post_append_count:
            logging.debug('Append successful based on sum of input features')
        else:
            logging.debug('esri_output_pre_append_count: {0}\input_feature_count: {1}\npost_append_count{2}\n'
                          'Append failed, sum of input features does not match. '
                          'Exiting'.format(esri_output_pre_append_count, input_feature_count, post_append_count))
            sys.exit(1)

        return

    def project_to_output_srs(self, input_fc, esri_output_fc):

        # Check if source SR matches esri_service_output SR
        from_srs = arcpy.Describe(input_fc).spatialReference
        to_srs = arcpy.Describe(esri_output_fc).spatialReference

        if to_srs.exportToString() != from_srs.exportToString():

            # Project if not
            logging.debug('Source SR of {0} does not match esri_service_output of {1}'.format(input_fc, esri_output_fc))
            logging.debug('Projecting source data to temp FC before appending to esri_service_output')

            temp_gdb = os.path.join(self.scratch_workspace, 'source_data.gdb')
            if not os.path.exists(temp_gdb):
                arcpy.CreateFileGDB_management(self.scratch_workspace, os.path.basename(temp_gdb))

            temp_proj_dataset = os.path.join(temp_gdb, "src_proj")

            if self.transformation:
                arcpy.env.geographicTransformations = self.transformation

            arcpy.Project_management(input_fc, temp_proj_dataset, to_srs, self.transformation, from_srs)

            fc_to_append = temp_proj_dataset

        else:
            fc_to_append = input_fc

        return fc_to_append

    def vector_to_raster(self, input_fc):

        if self.vector_to_raster_output:

            temp_dir = util.create_temp_dir(self.scratch_workspace)
            arcpy.CreateFileGDB_management(temp_dir, 'temp.gdb')

            # Get spatial reference of output
            sr = arcpy.Describe(self.vector_to_raster_output).spatialReference

            logging.debug('Starting to project input vector FC to the spatial reference of the output raster')
            out_projected_fc = os.path.join(temp_dir, 'temp.gdb', 'src_prj_to_ras_sr')
            arcpy.Project_management(input_fc, out_projected_fc, sr)

            util.add_field_and_calculate(out_projected_fc, 'ras_val', 'SHORT', '', 1, self.gfw_env)

            # Get cell size of output
            cell_size = int(arcpy.GetRasterProperties_management(self.vector_to_raster_output,
                                                                 'CELLSIZEX').getOutput(0))

            arcpy.env.pyramid = "NONE"
            arcpy.env.snapRaster = self.vector_to_raster_output

            logging.debug('Rasterizing and outputting as tif')
            out_raster = os.path.join(temp_dir, 'out.tif')

            arcpy.PolygonToRaster_conversion(out_projected_fc, 'ras_val', out_raster, "CELL_CENTER", "", cell_size)

            # Stop service that has a lock on the raster
            service = r'image_services/analysis'
            # arcgis_server.set_service_status(service, 'stop')

            logging.debug('Sleeping for 10 seconds to let the lock files resolve themselves')
            time.sleep(10)

            logging.debug('Copying raster {0} to output {1}'.format(out_raster, self.vector_to_raster_output))
            arcpy.Delete_management(self.vector_to_raster_output)

            # Move all related tif files to final destination
            # Much faster than using CopyRaster_management-- just need to physically move the files
            src_dir = os.path.dirname(out_raster)
            src_file_name = os.path.splitext(os.path.basename(out_raster))[0]

            out_dir = os.path.dirname(self.vector_to_raster_output)
            out_file_name = os.path.splitext(os.path.basename(self.vector_to_raster_output))[0]

            for extension in ['.tif', '.tfw', '.tif.aux.xml', '.tif.vat.cpg', '.tif.vat.dbf', '.tif.xml']:
                src_file = os.path.join(src_dir, src_file_name + extension)
                out_file = os.path.join(out_dir, out_file_name + extension)

                try:
                    shutil.move(src_file, out_file)
                except IOError:
                    print 'No such file {0}'.format(src_file)

            # Restart the service after we're finished
            # arcgis_server.set_service_status(service, 'start')

        else:
            pass

    def create_archive_and_download_zip(self):
        """
        Check if the source is wgs84; if not, create a local projection download zip
        Project to wgs84, and create a download zip and a timestamped archive zip
        :return:
        """
        logging.info('Starting vector_layer.create_archive_and_download_zip for {0}'.format(self.name))

        # if source is in local projection, create separate download
        # if it has a merge where_field specified, don't create a local download
        # this suggests that our source dataset is only part of the larger whole, which is in wgs84
        # example: imazon_sad-- we download pieces each month, project to WGS84, then share the output
        # don't want to pretend that one of those monthly local projection files is actually the entire dataset
        if not util.is_wgs_84(self.source) and not self.merge_where_field:

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

    def sync_cartodb(self, input_fc, cartodb_output_fc, where_clause):
        """
        Take the esri source dataset and append it to cartodb. If a where_clause specified,
        use that to build a DELETE where clause to run against the cartoDB table before appending
        :param input_fc: the source dataset
        :param cartodb_output_fc: the output table in cartoDB
        :param where_clause: a where clause to apply to select from the input_fc and to delete from the cartodb_output
        :return:
        """
        if cartodb_output_fc:
            logging.info('Starting vector_layer.sync_cartodb for {0}. Output {1}, '
                         'wc {2}'.format(os.path.basename(input_fc), cartodb_output_fc, where_clause))

            cartodb.cartodb_sync(input_fc, cartodb_output_fc, where_clause, self.gfw_env, self.scratch_workspace)
        else:
            logging.debug('No cartodb output fc specified. Moving on.')

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

        self.build_update_where_clause(self.source, self.merge_where_field)

        self.append_to_esri_source(self.source, self.esri_service_output, self.update_where_clause)

        self.vector_to_raster(self.esri_service_output)

        # Running into issues with SDE locks
        # Works fine for one layer, but when that layer is part of a larger country layer, it
        # doesn't release the lock that it creates
        # self.update_esri_metadata()

        self.update_tile_cache()

        self.create_archive_and_download_zip()

        self.sync_cartodb(self.esri_service_output, self.cartodb_service_output, self.update_where_clause)

        self.post_process()
