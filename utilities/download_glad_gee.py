import datetime

from gee_asset import Asset


def download(gfw_env, scratch_workspace):
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%m_%d')

    region_list = [{
                    'name': 'africa',
                    'gee_suffix': 'AFR',
                    'ras_name_lkp': {
                                     'Africa_day_2017n': ['alertDate17'],
                                     'Africa_conf_2017n': ['conf17']},
                    'bbox': [7.0000000, 14.0000000, 36.0000000, -14.0000000],
                    'size': [116001, 112001]
                    },
                   {
                    'name': 'south_america',
                    'gee_suffix': 'SA',
                    'ras_name_lkp': {
                                     'sa_day2017': ['alertDate17'],
                                     'sa_conf2017': ['conf17']},
                    'bbox': [-82.0005000, 6.0005000, -33.9995000, -34.0005000],
                    'size': [192005, 160005]
                   },
                   {
                    'name': 'nsa',
                    'gee_suffix': 'NSA',
                    'ras_name_lkp': {
                                     'nsa_day2017': ['alertDate17'],
                                     'nsa_conf2017': ['conf17']},
                    'bbox': [-82.0005000, 12.639700000, -33.9995000, -34.0005000],
                    'size': [192005, 186562]
                   },
                   {
                    'name': 'se_asia',
                    'gee_suffix': 'SEA',
                    'ras_name_lkp': {
                                     'SEA_day_2017n': ['alertDate17'],
                                     'SEA_conf_2017n': ['conf17']},
                    'bbox': [94.9995000,   8.0005000, 156.0005000, -12.0005000],
                    'size': [244005, 80006]
                    }
                   ]

    # lkp which regions/countries to process based on
    download_dict = {'staging': ['nsa', 'africa', 'se_asia'],
                     'prod': ['nsa', 'africa', 'se_asia']}

    # grab the list of configs based on the gfw_env we're using
    download_list = [x for x in region_list if x['name'] in download_dict[gfw_env]]

    asset_list = []

    # iterate over the download_list, creating assets + exporting
    for to_download in download_list:
        for output_name, band_list in to_download['ras_name_lkp'].iteritems():

            # Temporary-- Brazil for the time being only has one valid date
            # ras_date = to_download.get('gee_date', yesterday_str)

            src_raster = 'projects/glad/alert/UpdResult/{}_{}'.format(yesterday_str, to_download['gee_suffix'])

            asset_object = Asset(src_raster, band_list, to_download['bbox'],
                                 to_download['size'], output_name, scratch_workspace)

            # want to start all exports first
            asset_object.start_export()

            asset_list.append(asset_object)

    for asset_object in asset_list:
        asset_object.check_export()

        asset_object.download()

        asset_object.postprocess()

        asset_object.upload_to_s3()
