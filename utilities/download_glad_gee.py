from gee_asset import Asset


def download(scratch_workspace):
    glad_raster = 'projects/glad/alert/UpdResult/05_11'

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

        asset_object.remove_output_dir()


if __name__ == '__main__':
    download()