import subprocess
import logging

import util


def set_service_status(service, action):

    logging.debug("starting to execute {0} on service {1}".format(service, action))

    password = util.get_token('arcgis_server_pass')

    cwd = r"C:\Program Files\ArcGIS\Server\tools\admin"
    cmd = [r'C:\PYTHON27\ArcGISx6410.5\python', "manageservice.py", '-u', 'astrong', '-p', password]
    cmd += ['-s', 'http://gis-gfw.wri.org/arcgis/admin', '-n', service, '-o', action]

    # Added check_call so it will crash if the subprocess fails
    subprocess.check_call(cmd, cwd=cwd)
    logging.debug("{0} on service {1} complete".format(service, action))
