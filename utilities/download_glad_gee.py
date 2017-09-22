import datetime

from gee_asset import Asset


def download(gfw_env, scratch_workspace):
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%m_%d')

    region_list = [{
                    'name': 'africa',
                    'gee_suffix': 'AFR',
                    'ras_name_lkp': {'Africa_day_2016n': ['alertDate16'],
                                     'Africa_day_2017n': ['alertDate17'],
                                     'Africa_conf_2016n': ['conf16'],
                                     'Africa_conf_2017n': ['conf17']},
                    'bbox': [7.0000000, 14.0000000, 36.0000000, -14.0000000],
                    'size': [116001, 112001]
                    },
                   {
                    'name': 'peru',
                    'gee_suffix': 'SA',
                    'ras_name_lkp': {'peru_day2016': ['alertDate16'],
                                     'peru_day2017': ['alertDate17'],
                                     'peru_conf2016': ['conf16'],
                                     'peru_conf2017': ['conf17']},
                    'bbox': [-82.0005000, 1.0005000, -67.9995000, -19.0005000],
                    'size': [56005, 80005]
                   },
                   {
                    'name': 'brazil',
                    'gee_suffix': 'BRA',
                    # 'gee_date': '5_21',
                    'ras_name_lkp': {'brazil_day2016': ['alertDate16'],
                                     'brazil_day2017': ['alertDate17'],
                                     'brazil_conf2016': ['conf16'],
                                     'brazil_conf2017': ['conf17']},
                    'bbox': [-75.0005000, 6.0005000, -33.9995000, -34.0005000],
                    'size': [164005, 160005]
                    }
                   ]

    # lkp which regions/countries to process based on
    download_dict = {'staging': {'peru', 'africa', 'brazil'},
                     'prod': ['peru']}

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
