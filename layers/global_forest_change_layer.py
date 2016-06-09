__author__ = 'Charlie.Hofmann'

import arcpy
import logging
import os
import shutil
import time
import subprocess
from Queue import Queue
from threading import Thread

from collections import OrderedDict
from layers.raster_layer import RasterLayer


class GlobalForestChangeLayer(RasterLayer):
    """
    GlobalForestChange layer class. Inherits from RasterLayer
    """

    def __init__(self, layerdef):
        logging.debug('Starting global_forest_change_layer')
        super(GlobalForestChangeLayer, self).__init__(layerdef)

        self.footprint_dict = {}
        self.num_threads = 3
        self.q = Queue()

    def archive_source_rasters(self):
        """
        Create timestamped backup of source datasets
        :return:
        """
        for ras in self.source:
            self.archive_source(ras)

    def copy_to_esri_output_multiple(self):
        """
        Copy inputs downloaded from the source to the proper output location
        :return:
        """
        esri_output_list = self.esri_service_output.split(',')
        input_output_tuples = zip(self.source, esri_output_list)

        for input_ras, output_ras in input_output_tuples:
            self.copy_to_esri_output(input_ras, output_ras)

    def build_footprint_dict(self):
        """
        Using a template footprint feature class, build a dictionary that will be used to define
        various overviews by cell size and bounding boxes

        To create this FC: make a copy of the final geodatabase with all component rasters added
        (i.e. R:\glad_alerts\filter_glad_alerts.gdb). In ArcCatalog, right click on any full-extent raster dataset
        and select Optimize >> Define Overviews. Set Overview Tile Parameters >> Overview Sampling Factor
        to 2, and Overview Image Parameters >> Resampling Method to Nearest.

        Once these overviews are defined, pull the confidence mosaic dataset into ArcMap, and export
        the footprint FC that was generated. Example: R:\glad_alerts\processing\footprint\footprint.gdb\unprocessed
        Identify any multipart features and split them into single part, (be sure to give them a unique name)
        and combine them with the rest of the polygons (those that were already single part) in final_footprint fc
        :return:
        """
        field_list = ['Name', 'MinPS', 'MaxPS', 'LowPS', 'region', 'Shape@']

        with arcpy.da.SearchCursor(self.overview_template_fc, field_list) as cursor:
            for row in cursor:
                row_dict = {'MinPS': row[1], 'MaxPS': row[2], 'LowPS': row[3], 'region': row[4], 'extent': {}}

                # Read in all the points so we can find the upper left/lower right to create an extent windows
                point_dict = {'x': [], 'y': []}

                for part in row[5]:
                    for point in part:
                        point_dict['x'].append(point.X)
                        point_dict['y'].append(point.Y)

                # Populate upper left x (ulx), upper left y (uly), lrx, lry so that we can extract rasters using GDAL
                row_dict['extent']['ulx'] = min(point_dict['x'])
                row_dict['extent']['uly'] = max(point_dict['y'])
                row_dict['extent']['lry'] = min(point_dict['y'])
                row_dict['extent']['lrx'] = max(point_dict['x'])

                # Add this information to our footprint dict, using the Name field as a key
                if row[0] not in self.footprint_dict and row[4] in self.region_list:
                    self.footprint_dict[row[0]] = row_dict

                else:
                    # We're not processing this region currently; likely a test run of the system
                    if row[4] not in self.region_list:
                        pass
                    else:
                        raise KeyError('The key {0} already exists in self.footprint_dict. This is likely due '
                                       'to an error in the overview template fc-- the field "Name" must be unique. '
                                       'Exiting.'.format(row[0]))

    def export_mosaics(self):
        """
        Export each mosaic dataset (4 in total, 1 for each band) to tif
        This is necessary for resampling; we need to use arcpy.Resample (it has the MAJORITY resample method
        available) and for some reason Arc crashes when this tool is run using a mosaic dataset as input
        :return:
        """

        for band_name in self.band_list:

            for region in self.region_list:

                region_dir = os.path.join(self.processing_dir, region)
                mosaic_path = os.path.join(region_dir, 'export_mosaic', 'export.gdb', band_name)

                tif_output = os.path.join(self.processing_dir, region, 'resampled', band_name + '_27m.tif')

                # Define job config and add it to the queue
                config = {'type': 'export', 'band_name': band_name, 'input': mosaic_path, 'output': tif_output}

                self.q.put(config)

    def resample_tifs(self):
        """
        Using the footprint_dict as an input, list all the required rasters we need based on cell size.
        Once we have these, add them to the queue for processing
        :return:
        """

        # List all the cell sizes we need to create in order from smallest to largest
        cell_size_list = sorted([v['LowPS'] for k, v in self.footprint_dict.iteritems()])

        # build a dict with cell_size_int as a lookup to cell_size_float
        # this will remove all the duplicate cell sizes that seem to profilerate (55.659745, 55.659746, etc)
        cell_size_dict = OrderedDict((int(x), x) for x in cell_size_list)

        for band_name in self.band_list:
            for region in self.region_list:

                # Grab the 'raw' (i.e. not resampled) input tiff for this combination of band_name and region
                resampled_dir = os.path.join(self.processing_dir, region, 'resampled')

                # Set this as the initial input cell size
                src_cell_size = 27

                for cell_size_int, cell_size_float in cell_size_dict.iteritems():

                    input_tif = os.path.join(resampled_dir, band_name + '_{0}m.tif'.format(src_cell_size))
                    output_tif = os.path.join(resampled_dir, band_name + '_{0}m.tif'.format(cell_size_int))

                    config = {'type': 'resample', 'band_name': band_name, 'input': input_tif,
                              'output': output_tif, 'cell_size': str(cell_size_float)}

                    # Add the job to the queue
                    self.q.put(config)

                    # Set the source cell size for the next raster as the current cell size
                    # We'll use the output raster generated above as the input for our next resample operation
                    src_cell_size = cell_size_int

    def build_overviews(self):
        """
        After we've resampled, grab the bounding boxes from footprint_dict for the tiles we need to generate.
        This will extract the tiles from the resampled rasters and name them properly.
        This function also starts the processing of all jobs in the queue-- export, resampling and building overviews
        :return:
        """

        for output_name, output_dict in self.footprint_dict.iteritems():

            for band_name in self.band_list:

                region_dir = os.path.join(self.processing_dir, output_dict['region'])

                input_cell_size = int(output_dict['LowPS'])
                input_tif = os.path.join(region_dir, 'resampled', '{0}_{1}m.tif'.format(band_name, input_cell_size))

                output_tif = os.path.join(region_dir, 'overviews', band_name, output_name)

                config = {'type': 'gdal_translate', 'output': output_tif,
                          'input': input_tif, 'extent': output_dict['extent']}

                self.q.put(config)

        # Process the queue of jobs we just created
        self.process_queue()

    def add_rasters_to_mosaic_datasets(self):
        """
        After all overviews are generated, add the rasters to the appropriate mosaic datsaet and set their type to
        be Overview
        :return:
        """
        logging.debug("Adding rasters to mosaic datasets")

        for region in self.region_list:
            overview_dir = os.path.join(self.processing_dir, region, 'overviews')

            for band_name in self.band_list:

                output_mosaic = os.path.join(self.mosaic_gdb, band_name)
                band_overview_dir = os.path.join(overview_dir, band_name)

                arcpy.env.workspace = band_overview_dir
                overview_list = ';'.join(arcpy.ListRasters())

                arcpy.AddRastersToMosaicDataset_management(output_mosaic, "Raster Dataset", overview_list,
                                                           "UPDATE_CELL_SIZES", "UPDATE_BOUNDARY", "NO_OVERVIEWS", 0)

                self.update_mosaic_attribute_table(band_name)

    def remove_existing_overviews(self):
        """
        Remove any datasets of type 'overview' from all the band mosaics
        :return:
        """

        for band_name in self.band_list:
            # Use where clause "Category = 2" to remove only overviews, not primary datasets
            full_path = os.path.join(self.mosaic_gdb, band_name)
            arcpy.RemoveRastersFromMosaicDataset_management(full_path, "Category = 2","UPDATE_BOUNDARY",
                                                            "MARK_OVERVIEW_ITEMS", "DELETE_OVERVIEW_IMAGES",
                                                            "DELETE_ITEM_CACHE", "REMOVE_MOSAICDATASET_ITEMS",
                                                            "UPDATE_CELL_SIZES")

    def update_mosaic_attribute_table(self, band):
        """
        For all the overview rasters that we've added to each band, register them as such (Category = 2)
        If registered, ArcGIS will be able to use them properly when displaying the source mosiacs
        Also make sure MinPS/MaxPS cell sizes are set appropriately so that the overviews are used
        :param band:
        :return:
        """

        output_mosaic = os.path.join(self.mosaic_gdb, band)
        with arcpy.da.UpdateCursor(output_mosaic, ['Name', 'Category', 'MinPS', 'MaxPS']) as cursor:
            for row in cursor:

                # TIFs are entered in the footprint dict as <filename>.tif, but just as <filename> in the
                # output mosaic
                tif_name = row[0] + '.tif'

                # identify overview rasters and set category value to 2 (built overview)
                # Also update pixel sizes
                if row[0][0:3] == 'Ov_':
                    row[1] = 2
                    row[2] = self.footprint_dict[tif_name]['MinPS']
                    row[3] = self.footprint_dict[tif_name]['MaxPS']

                # If we're dealing with a source raster (non-overview) make sure the minps is 0
                # and the max PS is the minimum for the overview
                else:
                    min_ps_value = min([x['MinPS'] for x in self.footprint_dict.values()])

                    row[2] = 0
                    row[3] = min_ps_value

                cursor.updateRow(row)

    def remove_existing_output_dirs(self):
        """
        Clean up the resampled and overviews dirs before we create new overviews
        :return:
        """

        # First, remove these overviews from their mosaics
        self.remove_existing_overviews()

        for region in self.region_list:
            for folder in ['resampled', 'overviews']:

                remove_dir = os.path.join(self.processing_dir, region, folder)

                # Remove and then recreate as empty dir
                shutil.rmtree(remove_dir)
                os.mkdir(remove_dir)

            for band_name in self.band_list:
                band_overview_dir = os.path.join(self.processing_dir, region, 'overviews', band_name)
                os.mkdir(band_overview_dir)

    def process_queue(self):
        """
        Create multiple workers to process the queue
        :return:
        """
        logging.debug('Starting queue process now using {0} threads'.format(self.num_threads))
        for i in range(self.num_threads):
            worker = Thread(target=self.process_jobs, args=(i,))
            worker.setDaemon(True)
            worker.start()

        # Blocks the main thread of the program until the workers have finished the queue
        self.q.join()

        # Sleep for a second to finish logging all messages from the workers
        time.sleep(1)
        logging.debug('Queued process complete')

    def process_jobs(self, i):
        """This is the worker thread function. It processes items in the queue one after
        another.  These daemon threads go into an infinite loop, and only exit when
        the main thread ends.
        :param i: the ID of the worker
        """
        while True:
            logging.debug('{0}: Looking for the next job'.format(i))
            config = self.q.get()

            # If the input is a tif, and it doesn't exist yet, move on to the next item in the queue
            complete_txt_file = os.path.splitext(config['input'])[0] + '.txt'
            if os.path.splitext(config['input'])[1] == '.tif' and not os.path.exists(complete_txt_file):
                pass
                # logging.debug('Input not ready: {0}'.format(os.path.basename(config['input'])))

                self.q.task_done()
                self.q.put(config)

            else:

                logging.debug(config)

                # Each worker needs to use subprocess (apparently) due to ArcGIS issues with locking GDBs
                # Define the parameters required for each type of process (export, resample and gdal_translate)
                if config['type'] == 'export':

                    cmd = ['python', 'utilities\mosaic_processing.py', 'export_mosaic_to_tif',
                           config['input'], config['output'], config['band_name']]

                elif config['type'] == 'resample':

                    cmd = ['python', 'utilities\mosaic_processing.py', 'resample_single_tif',
                           config['input'], config['output'], config['band_name'], config['cell_size']]

                else:
                    cmd = ['python', 'utilities\mosaic_processing.py',
                           'run_gdal_translate', config['input'], config['output']]

                    extent_args = [str(config['extent']['ulx']), str(config['extent']['uly']),
                                   str(config['extent']['lrx']), str(config['extent']['lry'])]

                    # Add extent info for projwin to extract bounding boxes for overviews
                    cmd += extent_args

                subprocess.check_call(cmd)

                # Leave a .txt file in the folder. This will signal to other workers that this
                # output raster is complete, and can be used as an input to other processes
                output_file = os.path.splitext(config['output'])[0] + '.txt'

                with open(output_file, 'wb') as theFile:
                    theFile.write('Complete')

                # Log that the worker has finished the task
                self.q.task_done()

    def _update(self):

        # self.archive_source_rasters()
        #
        # self.copy_to_esri_output_multiple()

        self.build_footprint_dict()

        self.remove_existing_output_dirs()

        self.export_mosaics()

        self.resample_tifs()

        self.build_overviews()

        self.add_rasters_to_mosaic_datasets()
