import time
import subprocess
import os
import ee
import uuid

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import util


ee.Initialize()


class Asset(object):
    def __init__(self, image_id, band_list, bbox, raster_size, output_name, scratch_workspace):

        self.image = ee.Image(image_id)
        self.band_list = band_list
        self.bbox = bbox
        self.raster_size = raster_size

        self.output_name = output_name
        self.out_tif = self.output_name + '.tif'
        self.vrt = None

        if not os.path.exists(scratch_workspace):
            os.mkdir(scratch_workspace)

        self.id = str(uuid.uuid4())[0:10]

        self.output_dir = os.path.join(scratch_workspace, self.id)
        os.mkdir(self.output_dir)

        self.task = None

    def start_export(self):

        print 'starting export for output file {}'.format(self.output_name)

        # selected to match the 2015 extent
        # a little larger given that EE seems to clip a little off for some reason
        region = ee.Geometry.Rectangle(self.bbox)

        export_config = {
            'image': self.image.select(self.band_list),
            'description': self.output_name,
            'folder': 'alertDownload_{}'.format(self.band_list[0]),
            'fileNamePrefix': self.id,
            'region': region.toGeoJSON()['coordinates'],
            'crs': 'EPSG:4326',
            'crsTransform': [0.00025, 0, 0, 0, -0.00025, 0],
            'maxPixels': 1e12
        }

        self.task = ee.batch.Export.image.toDrive(**export_config)

        self.task.start()

    def check_export(self):

        while self.task.status()['state'] in ['RUNNING', 'READY']:
            print 'Waiting for export for {}. Status: {}'.format(self.output_name, self.task.status()['state'])
            time.sleep(10)

        final_state = self.task.status()['state']

        if final_state != 'COMPLETED':
            raise ValueError('Task status for {} is {}'.format(self.output_name, final_state))
        else:
            print 'Export for output file {} is {}'.format(self.output_name, final_state)

    def download(self):

        drive = authorize()
        file_list = drive.ListFile({'q': "title contains '{}'".format(self.id)}).GetList()

        for f in file_list:
            print('title: %s, id: %s' % (f['title'], f['id']))

            # grab the file by ID
            to_download = drive.CreateFile({'id': f['id']})

            # save it locally
            local_output = os.path.join(self.output_dir, f['title'])
            to_download.GetContentFile(local_output)

            # Delete from drive
            to_download.Delete()

    def postprocess(self):

        self.vrt = os.path.join(self.output_dir, 'out.vrt')
        build_vrt = ['gdalbuildvrt', self.vrt, '*.tif']
        subprocess.check_call(build_vrt, cwd=self.output_dir)

        to_tif = ['gdal_translate', '-co', 'COMPRESS=LZW', self.vrt, self.out_tif]
        to_tif += ['-projwin'] + [str(x) for x in self.bbox]
        to_tif += ['-a_nodata', '0']
        subprocess.check_call(to_tif, cwd=self.output_dir)

        gdalinfo_list = util.run_subprocess(['gdalinfo', self.vrt])
        print '\n'.join(gdalinfo_list)

        size_line = gdalinfo_list[2]
        size_results = size_line.replace(',', '').split()[2:]

        size_tuple = [int(x) for x in size_results]
        print 'Checking size of the VRT that we downloaded from GEE'
        print size_tuple
        print self.raster_size
        if size_tuple != self.raster_size:
            raise ValueError('Size tuple does not match expected {} boundaries'.format(self.output_name))

    def upload_to_s3(self):

        aws_bucket = r's3://gfw-gee-glad-export/'
        aws_cp = ['aws', 's3', 'cp', self.out_tif, aws_bucket]

        subprocess.check_call(aws_cp, cwd=self.output_dir)


def authorize():
    # this is a terrible thing, but appears slightly better than the google libraries
    # http://stackoverflow.com/questions/24419188/automating-pydrive-verification-process
    # http://stackoverflow.com/questions/28184419/pydrive-invalid-client-secrets-file
    gauth = GoogleAuth()

    # client_secrets.json must be in the root
    # why this is necessary, and why i can't figure out how to get it to work
    # using gauth.LoadCredentialsFile("mycreds.txt") is beyond me

    # Try to load saved client credentials
    gauth.LoadCredentialsFile("mycreds.txt")
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()

    # Save the current credentials to a file
    gauth.SaveCredentialsFile("mycreds.txt")

    drive = GoogleDrive(gauth)

    return drive
