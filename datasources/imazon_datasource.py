__author__ = 'Charlie.Hofmann'

import os
import sys
import arcpy
import datetime
import shutil
import calendar
import logging
from bs4 import BeautifulSoup
from urllib2 import urlopen

from datasource import DataSource
from utilities import util

class ImazonDataSource(DataSource):
    """
    ImazonVector datasource class. Inherits from DataSource
    """

    def __init__(self, layerdef):
        logging.debug('starting imazon_datasource')

        super(ImazonDataSource, self).__init__(layerdef)

        self._imazon_archive_folder = None
        self.imazon_archive_folder = r'F:\forest_change\imazon_sad'

        self.layerdef = layerdef


    # Validate name
    @property
    def imazon_archive_folder(self):
        return self._imazon_archive_folder

    @imazon_archive_folder.setter
    def imazon_archive_folder(self, i):
        if not i:
            logging.debug("No imazon archive folder listed")
        self._imazon_archive_folder = i

    def recent_file(self, min_date, url):
        try:
            yrmo = url.split('_')[3]
            year, month = [int(i) for i in yrmo.split('-')]
            dt = datetime.datetime(year, month, 1)

            if dt >= min_date:
                return True
            else:
                return False

        except:
            return False

    def list_sad_urls(self):
        mindate = datetime.datetime(2014, 1, 1)

        html = urlopen(r'http://www.imazongeo.org.br/doc/downloads.php').read()
        bs = BeautifulSoup(html, 'lxml')

        # get list elements
        tds = [td.find('a') for td in bs('td')]

        # get urls from list elements li if they exist
        urls = [td['href'] for td in tds if td]

        # only keep urls containing 'desmatamento' or 'degradacao'
        deforest_degrade_urls = [url for url in urls if 'desmatamento' in url or 'degradacao' in url]

        return [x for x in deforest_degrade_urls if self.recent_file(mindate, x)]

    def check_imazon_already_downloaded(self, urlList):
        toDownloadList = []

        imazon_dir_list = os.listdir(self.imazon_archive_folder)

        imazon_zip_files = [x for x in imazon_dir_list if os.path.splitext(x)[1] == '.zip']

        for url in urlList:
            url_zip_file = os.path.basename(url)

            if url_zip_file not in imazon_zip_files:
                toDownloadList.append(url)

        return toDownloadList

    def download_sad_zipfiles(self, to_download_list):
        unzip_list = []
        
        for url in to_download_list:
            if not 'http://' in url:
                url = 'http://imazongeo.org.br{0!s}'.format(url)

            z = self.download_file(url, self.download_workspace)
            
            output_zip_name = os.path.basename(z)
            imazon_source_path = os.path.join(self.imazon_archive_folder, output_zip_name)

            logging.debug('Copying archive to ' + self.imazon_archive_folder)
            shutil.copy2(z, imazon_source_path)

            unzip_list.append(z)

        return unzip_list


    def download_new_source_data(self):
        sad_urls = self.list_sad_urls()

        to_download = self.check_imazon_already_downloaded(sad_urls)

        if not to_download:
            logging.info('No new data on the imazon site')

            # Important for the script that reads the log file and sends an email
            # Including this 'Checked' message will show that we checked the layer but it didn't need updating
            logging.critical('Checked | {0}'.format(self.name))
            sys.exit(0)
            
        else:
            source_list = []

            for file in self.download_sad_zipfiles(to_download):
                outdir = os.path.dirname(file)
                
                self.unzip(file, outdir)

                outfilename = os.path.splitext(os.path.basename(file))[0] + '.shp'
                outfilepath = os.path.join(outdir, outfilename)
                
                source_list.append(outfilepath)

        return source_list

    def data_type(self, shpName):
        if 'degradacao' in shpName:
            data_type = 'degrad'
        elif 'desmatamento' in shpName:
            data_type = 'defor'
        else:
            logging.error("Unknown data type for {0} Exiting now.".format(shpName))
            sys.exit(1)

        return data_type

    def get_date_from_filename(self, filename):
        parts = filename.split('_')
        date_obj = datetime.datetime.strptime(parts[3], '%Y-%m')
        year = date_obj.year
        month = date_obj.month
        day = calendar.monthrange(year, month)[1]
        imazon_date = datetime.date (year, month, day)
        imazon_date_text = imazon_date.strftime("%m/%d/%Y")
        
        return imazon_date_text

    def clean_source_shps(self, shpList):

        cleaned_shp_list = []

        for shp in shpList:
            
            shpName = os.path.basename(shp).replace('-','_')
            
            singlePartPath = os.path.join(os.path.dirname(shp), shpName.replace('.shp','') + '_singlepart.shp')

            logging.info('Starting multipart to singlepart for ' + shpName)
            arcpy.MultipartToSinglepart_management(shp, singlePartPath)

            arcpy.RepairGeometry_management(singlePartPath, "DELETE_NULL")

            # orig_oid_field = 'orig_oid'
            # arcpy.AddField_management(singlePartPath, orig_oid_field, 'TEXT', '255')
            # arcpy.CalculateField_management(singlePartPath, orig_oid_field, '!FID!', 'PYTHON_9.3')

            # Must have one field before we delete all the other ones. So says arcgis anyway
            orig_oid_field = 'orig_oid'
            util.add_field_and_calculate(singlePartPath, orig_oid_field, 'Text', '255', '!FID!', self.gfw_env)
            self.remove_all_fields_except(singlePartPath, keep_field_list=[orig_oid_field])

            imazon_date_str = self.get_date_from_filename(os.path.basename(shp))

            util.add_field_and_calculate(singlePartPath, 'Date', 'DATE', "", imazon_date_str, self.gfw_env)
            util.add_field_and_calculate(singlePartPath, 'data_type', 'TEXT', "255", self.data_type(shp), self.gfw_env)
            util.add_field_and_calculate(singlePartPath, 'orig_fname', 'TEXT', "255", shpName, self.gfw_env)

            cleaned_shp_list.append(singlePartPath)

        return cleaned_shp_list

    def build_all_imazon_layers(self):
        
        layerdef_list = []

        source_list = self.download_new_source_data()

        cleaned_source_list = self.clean_source_shps(source_list)

        return cleaned_source_list

    def get_layer(self):

        input_layers = self.build_all_imazon_layers()

        logging.info('merging datasets: {0}'.format(', '.join(input_layers)))

        output_dataset = os.path.join(self.download_workspace, 'imazon_sad.shp')
        logging.debug('output dataset: {0}\n'.format(output_dataset))

        arcpy.Merge_management(input_layers, output_dataset)

        # overwrite properties from original layerdef read
        # from the gfw-sync2 config Google Doc
        self.layerdef['source'] = output_dataset

        return self.layerdef


            

    

    

    



