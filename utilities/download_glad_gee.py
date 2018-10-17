import datetime

from gee_asset import Asset


def download(gfw_env, scratch_workspace):
    today = datetime.datetime.today()
    today_str = today.strftime('%m_%d')

    region_list = [{
                    'name': 'africa',
                    'gee_suffix': 'AFR',
                    'ras_name_lkp': {
                                     # 'Africa_day_2017n': ['alertDate17'],
                                     # 'Africa_conf_2017n': ['conf17'],
                                     'Africa_day_2018n': ['alertDate18'],
                                     'Africa_conf_2018n': ['conf18'],
                    },
                    'bbox': [7.0000000, 14.0000000, 36.0000000, -14.0000000],
                    'size': [116001, 112001]
                    },
                   {
                    'name': 'south_america',
                    'gee_suffix': 'SA',
                    'ras_name_lkp': {
                                     # 'nsa_day2017': ['alertDate17'],
                                     # 'nsa_conf2017': ['conf17'],
                                     'nsa_day2018': ['alertDate18'],
                                     'nsa_conf2018': ['conf18']
                    },
                    'bbox': [-82.0005000, 12.639700000, -33.9995000, -34.0005000],
                    'size': [192005, 186562]
                   },
                   {
                    'name': 'se_asia',
                    'gee_suffix': 'SEA',
                    'ras_name_lkp': {
                                     # 'SEA_day_2017n': ['alertDate17'],
                                     # 'SEA_conf_2017n': ['conf17'],
                                     'SEA_day_2018n': ['alertDate18'],
                                     'SEA_conf_2018n': ['conf18']
                    },
                    'bbox': [94.9995000,   8.0005000, 156.0005000, -12.0005000],
                    'size': [244005, 80006]
                    },
                    {
                    'name': 'central_america',
                    'gee_suffix': 'CA',
                    'ras_name_lkp': {
                                     'ca_day2018': ['alertDate18'],
                                     'ca_conf2018': ['conf18']
                    },
                    'bbox': [-120.00075, 30.0005, -58.999, 6.96625],
                    'size': [244006, 92009]
                    },
                    {
                        'name': 'southern_south_america',
                        'gee_suffix': 'SSA',
                        'ras_name_lkp': {
                            'ssa_day2018': ['alertDate18'],
                            'ssa_conf2018': ['conf18']
                        },
                        'bbox': [-73, -8.99675, -52.99825, -30.00275],
                        'size': [80007, 84011]
                    }
                   ]

    # lkp which regions/countries to process based on
    download_dict = {'staging': ['south_america', 'southern_south_america', 'central_america'],
                     'prod': ['south_america', 'africa', 'se_asia']}

    # grab the list of configs based on the gfw_env we're using
    download_list = [x for x in region_list if x['name'] in download_dict[gfw_env]]

    asset_list = []

    # iterate over the download_list, creating assets + exporting
    for to_download in download_list:
        for output_name, band_list in to_download['ras_name_lkp'].iteritems():

            suffix = to_download['gee_suffix']

            if suffix == 'AFR':
                src_raster = 'users/charliehofmann/AFR_alert_08_20'
            elif suffix == 'SA':
                src_raster = 'users/charliehofmann/SA_alert_08_20'
            elif suffix == 'SEA':
                src_raster = 'users/charliehofmann/SEA_alert_08_20'
            else:
                raise ValueError('unknown suffix {}'.format(suffix))

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
