__author__ = 'Charlie.Hofmann'

import os
import arcpy
import datetime
import shutil
from bs4 import BeautifulSoup
from urllib2 import urlopen

import cartodb
import util
from vector_layer import VectorLayer

class ImazonVectorLayer(VectorLayer):
    """
    ImazonVector layer class. Inherits from VectorLayer
    """

    def __init__(self, layerdef):
        print 'starting imazon_vector_layer'

        self._imazon_archive_folder = None
        self.imazon_archive_folder = r'F:\forest_change\imazon_sad'

        self.scratch_workspace = os.path.join(self.imazon_archive_folder, 'temp')

        if os.path.exists(self.scratch_workspace):
            shutil.rmtree(self.scratch_workspace)
            
        os.mkdir(self.scratch_workspace)
        
        self.check_for_new_source_data()

        super(ImazonVectorLayer, self).__init__(layerdef)

    # Validate name
    @property
    def imazon_archive_folder(self):
        return self._imazon_archive_folder

    @imazon_archive_folder.setter
    def imazon_archive_folder(self, i):
        if not i:
            warnings.warn("No imazon archive folder listed", Warning)
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

            z = util.download_zipfile(url, self.scratch_workspace)

            unzip_list.append(z)


    def check_for_new_source_data(self):
        sad_urls = self.list_sad_urls()

        to_download = self.check_imazon_already_downloaded(sad_urls)

        if not to_download:
            print 'No new data on the imazon site'
            sys.exit(0)
            
        else:
            unzip_list = self.download_sad_zipfiles(to_download)

        print unzip_list

    def update(self):

        pass

            

        

        #Creates timestamped backup and download from source
##        self.archive() 

        #Exports to WGS84 if current dataset isn't already
        #We need this dataset to add the date info to
##        self.export_2_shp()

        #hardcoded - remove
##        self.wgs84_file = r"D:\GIS Data\GFW\temp\gfw-sync2_test\umd_landsat_alerts__borneo.shp"

##        self.populateDateField()

        self.add_iso_code(self.wgs84_file)

        self.sync_cartodb("""iso = {0}""".format(self.iso_code))

        self.copy_to_esri_output()


    

    

    



