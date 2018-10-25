import os
import logging
import subprocess
from isoweek import Week


def post_process(layerdef):

    # for the most part this postprocessing is handled on the terranalysis machine now
    # in the future, we may choose to resurrect the make_climate_maps workflow
    # If we do, would recommend either:
    # - finding a way to do this with open source tools on the linux server
    # - grabbing one of the CSV point extracts from here: s3://gfw2-data/alerts-tsv/glad-download
    #   filtering it by week, and using that as the input

    # make_climate_maps(region_list)
    pass


def make_climate_maps(region_list):

    logging.debug('starting make_climate_maps')

    gfw_sync_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.dirname(gfw_sync_dir)
    climate_maps_dir = os.path.join(scripts_dir, 'gfw-climate-glad-maps')

    python_exe = r'C:\PYTHON27\ArcGISx6410.5\python'

    current_week = Week.thisweek()

    for i in range(1, 5):
        offset = current_week - i

        year = str(offset.year)
        week = str(offset.week)

        for region in region_list:

            cmd = [python_exe, 'create_map.py', '-y', year, '-w', week, '-r', region]
            logging.debug('calling subprocess:')
            logging.debug(cmd)

            subprocess.check_call(cmd, cwd=climate_maps_dir)





