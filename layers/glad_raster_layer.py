__author__ = 'Charlie.Hofmann'

import arcpy
import logging
import os
import sys
import time
import subprocess
from Queue import Queue
from threading import Thread

from collections import OrderedDict
from layers.raster_layer import RasterLayer


class GladRasterLayer(RasterLayer):
    """
    GladRaster layer class. Inherits from RasterLayer
    """

    def __init__(self, layerdef):
        logging.debug('Starting glad_raster_layer')
        super(GladRasterLayer, self).__init__(layerdef)

        self.processing_dir = r'R:\glad_alerts\processing'
        self.mosaic_gdb = r'R:\glad_alerts\filter_glad_alerts.gdb'
        self.region_list = ['africa', 'asia', 'south_america']

        self.footprint_dict = {}
        self.num_threads = 3
        self.q = Queue()

    def archive_source_rasters(self):
        # Create timestamped backup of source dataset
        for ras in self.source:
            self.archive_source(ras)

    def copy_to_esri_output_multiple(self):

        esri_output_list = self.esri_service_output.split(',')
        input_output_tuples = zip(self.source, esri_output_list)

        for input_ras, output_ras in input_output_tuples:
            self.copy_to_esri_output(input_ras, output_ras)

    def build_footprint_dict(self):

        # To create this FC: make a copy of the final geodatabase with all component rasters added
        # (i.e. R:\glad_alerts\filter_glad_alerts.gdb). Right click on the confidence raster in ArcCatalog
        # and select Optimize >> Define Overviews. Set Overview Tile Parameters >> Overview Sampling Factor
        # to 2, and Overview Image Parameters >> Resampling Method to Nearest.
        #
        # Once these overviews are defined, pull the confidence mosaic dataset into ArcMap, and export
        # the footprint FC that was generated. Example: R:\glad_alerts\processing\footprint\footprint.gdb\unprocessed
        # Identify any multipart features and split them into single part, (be sure to give them a unique name)
        # and combine them with the rest of the polygons (those that were already single part) in final_footprint fc
        overview_template_fc = r'R:\glad_alerts\processing\footprint\footprint.gdb\final_footprint'

        field_list = ['Name', 'MinPS', 'MaxPS', 'LowPS', 'region', 'Shape@']
        with arcpy.da.SearchCursor(overview_template_fc, field_list) as cursor:
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
                if row[0] not in self.footprint_dict:
                    self.footprint_dict[row[0]] = row_dict

                else:
                    logging.error('The key {0} already exists in self.footprint_dict. This is likely due '
                                  'to an error in the overview template fc-- the field Name must be unique. '
                                  'Exiting.'.format(row[0]))
                    sys.exit(1)

    def process_queue(self):
        # Create multiple workers to process the queue
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

    def export_mosaics(self):

        # Process all confidence and filter_glad_alerts rasters first; intensity raster needs
        # the output from confidence as an input
        # We'll ensure this happens by adding confidence rasters to the queue first
        for ras_type in ['confidence', 'filter_glad_alerts', 'intensity']:

            for region in self.region_list:

                region_dir = os.path.join(self.processing_dir, region)
                export_gdb = os.path.join(region_dir, 'export_mosaic', 'export.gdb')

                # Intensity rasters just use the confidence rasters as input, not the intensity mosaic
                if ras_type == 'intensity':
                    mosaic_path = os.path.join(region_dir, 'resampled', 'confidence' + '_27m.tif')

                else:
                    mosaic_path = os.path.join(export_gdb, ras_type)

                tif_output = os.path.join(self.processing_dir, region, 'resampled', ras_type + '_27m.tif')

                # Define job config and add it to the queue
                config = {'type': 'export', 'ras': ras_type, 'input': mosaic_path, 'output': tif_output}

                self.q.put(config)

        self.process_queue()

    def resample_tifs(self):

        # List all the cell sizes we need to create in order from smallest to largest
        cell_size_list = sorted([v['LowPS'] for k, v in self.footprint_dict.iteritems()])

        # build a dict with cell_size_int as a lookup to cell_size_float
        # this will remove all the duplicate cell sizes that seem to profilerate (55.659745, 55.659746, etc)
        cell_size_dict = OrderedDict((int(x), x) for x in cell_size_list)

        for ras_type in ['confidence', 'filter_glad_alerts', 'intensity']:
            for region in self.region_list:

                # Grab the 'raw' (i.e. not resampled) input tiff for this combination of ras_type and region
                resampled_dir = os.path.join(self.processing_dir, region, 'resampled')

                # Set this as the initial input cell size
                src_cell_size = 27

                for cell_size_int, cell_size_float in cell_size_dict.iteritems():

                    input_tif = os.path.join(resampled_dir, ras_type + '_{0}m.tif'.format(src_cell_size))
                    output_tif = os.path.join(resampled_dir, ras_type, '_{0}m.tif'.format(cell_size_int))

                    config = {'type': 'resample', 'ras': ras_type, 'input': input_tif,
                              'output': output_tif, 'cell_size': cell_size_float}

                    # Add the job to the queue
                    self.q.put(config)

                    # Set the source cell size for the next raster as the current cell size
                    # We'll use the output raster generated above as the input for our next resample operation
                    src_cell_size = cell_size_int

        # Process the queue of jobs we just created
        self.process_queue()

    def build_overviews(self):

        for output_name, output_dict in self.footprint_dict.iteritems():

            for ras_type in ['confidence', 'filter_glad_alerts', 'intensity']:

                region_dir = os.path.join(self.processing_dir, output_dict['region'])

                input_cell_size = int(output_dict['LowPS'])
                input_tif = os.path.join(region_dir, 'resampled', '{0}_{1}m.tif'.format(ras_type, input_cell_size))

                output_tif = os.path.join(region_dir, 'overviews', ras_type, output_name)
                extent = output_dict['extent']

                source_mosaic = os.path.join(self.mosaic_gdb, ras_type)

                config = {'type': 'gdal_translate', 'output': output_tif, 'input': input_tif,
                          'source_mosaic': source_mosaic, 'extent': extent}

                self.q.put(config)

        # Process the queue of jobs we just created
        self.process_queue()

    def process_jobs(self, i):
        """This is the worker thread function.
        It processes items in the queue one after
        another.  These daemon threads go into an
        infinite loop, and only exit when
        the main thread ends.
        :param i: the ID of the worker
        """
        while True:
            logging.debug('{0}: Looking for the next job'.format(i))
            config = self.q.get()

            # If the input is a tif, and it doesn't exist yet, move on to the next item in the queue
            if os.path.splitext(config['input'])[1] == '.tif' and not os.path.exists(config['input']):

                logging.debug("Putting this task back in the queue; input doesn't exist yet.")
                self.q.task_done()
                self.q.put(config)

                logging.debug("Sleeping for a minute in case it's the only task left")
                time.sleep(60)

            else:

                logging.debug(config)

                # Each worker needs to use subprocess (apparently) due to ArcGIS issues with locking GDBs
                if config['type'] == 'export':

                    cmd = ['python', 'utilities\mosaic_processing.py', 'export_mosaic_to_tif',
                           config['input'], config['output'], config['ras']]

                elif config['type'] == 'resample':

                    cmd = ['python', 'utilities\mosaic_processing.py', 'resample_single_tif',
                           config['input'], config['output'], config['ras'], config['cell_size']]

                else:
                    cmd = ['python', 'utilities\mosaic_processing.py', 'run_gdal_translate',
                           config['input'], config['output'], config['source_mosaic']]

                    extent_args = [config['extent']['ulx'], config['extent']['uly'],
                                   config['extent']['lrx'], config['extent']['lry']]

                    # Add extent info for projwin to extract bounding boxes for overviews
                    cmd += extent_args

                subprocess.check_call(cmd)

                # Log that the worker has finished the task
                self.q.task_done()

    def update(self):

        # self.archive_source_rasters()
        #
        # self.copy_to_esri_output_multiple()

        self.build_footprint_dict()

        self.export_mosaics()

        self.resample_tifs()

        self.build_overviews()






    

    

    



