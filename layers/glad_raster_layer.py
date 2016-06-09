__author__ = 'Charlie.Hofmann'

import logging

from layers.global_forest_change_layer import GlobalForestChangeLayer


class GladRasterLayer(GlobalForestChangeLayer):
    """
    GladRaster layer class. Inherits from GlobalForestChangeLayer
    """

    def __init__(self, layerdef):
        logging.debug('Starting glad_raster_layer')
        super(GladRasterLayer, self).__init__(layerdef)

        self.processing_dir = r'R:\glad_alerts\processing'
        self.mosaic_gdb = r'R:\glad_alerts\filter_glad_png.gdb'

        self.region_list = ['africa', 'asia', 'south_america']
        self.band_list = ['band1_day', 'band2_day', 'band3_conf_and_year', 'band4_intensity']

        self.overview_template_fc = r'R:\glad_alerts\processing\footprint\footprint.gdb\final_footprint'

    def update(self):

        self._update()
