import datetime

from gee_asset import Asset
import util


def download(scratch_workspace):
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%m_%d')

    glad_raster = 'projects/glad/alert/UpdResult/{}_SA'.format(yesterday_str)

    # test bbox: [-75,-16,-74,-15]
    peru_bbox = [-82.0005000, 1.0005000, -67.9995000, -19.0005000]

    lkp_name_dict = {'peru_day2016': ['alertDate16'],
                     'peru_day2017': ['alertDate17'],
                     'peru_conf2016': ['conf16'],
                     'peru_conf2017': ['conf17']}
    # 'swir1_nir_red': ['swir1', 'nir', 'red']}

    asset_list = []

    for output_name, band_list in lkp_name_dict.iteritems():
        asset_object = Asset(glad_raster, band_list, peru_bbox, output_name, scratch_workspace)

        # want to start all exports first
        asset_object.start_export()

        asset_list.append(asset_object)

    for asset_object in asset_list:
        asset_object.check_export()

        asset_object.download()

        asset_object.postprocess()

        qc_peru_download(asset_object.vrt)

        asset_object.upload_to_s3()


def qc_peru_download(input_vrt):

    gdalinfo_list = util.run_subprocess(['gdalinfo', input_vrt])
    print '\n'.join(gdalinfo_list)

    size_line = gdalinfo_list[2]
    size_results = size_line.replace(',', '').split()[2:]

    size_tuple = [int(x) for x in size_results]
    print 'Checking size of the VRT that we downloaded from GEE'
    print size_tuple
    if size_tuple != [56005, 80005]:
        raise ValueError('Size tuple does not match expected peru boundaries')
