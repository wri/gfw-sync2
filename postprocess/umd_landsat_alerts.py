import subprocess


def post_process(layerdef):

    cmd = ['python', 'update_country_stats.py', '-d', 'umd_landsat_alerts', '-a', 'gadm1_boundary', '--emissions']
    cwd = r'D:\scripts\gfw-country-pages-analysis'

    if layerdef.gfw_env == 'DEV':
        cmd.append('--test')

    subprocess.check_call(cmd, cwd=cwd)
