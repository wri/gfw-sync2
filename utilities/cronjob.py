import subprocess
import datetime
import argparse
import calendar

import google_sheet as gs
import email_stats

parser = argparse.ArgumentParser(description='Pass environment to kick off gfw-sync cron job.')
parser.add_argument('--environment', '-e', default='staging', choices=('staging', 'prod'),
                    help='the environment/config files to use for this run')
args = parser.parse_args()


def parse_update_freq(field_text):
    """
    Read the update_freq field from the config table and determine if the layer in question needs to be updated today
    :param field_text: the value in the update_freq column
    :return:
    """
    update_layer = False
    current_day = None
    day_list = []

    # Check that the layer has an update frequency first
    if field_text:

        # remove the brackets from the range
        field_text = field_text.replace('[', '').replace(']', '')

        # If it has '-', assume it's a range and build a list
        if '-' in field_text:
            start_day, end_day = field_text.split('-')
            day_list = range(int(start_day), int(end_day) + 1)

            # current day is the date integer given that the input is an integer range
            current_day = datetime.datetime.now().day

        # Otherwise assume it's a list of dates
        else:
            day_list_text = field_text.split(',')

            try:
                day_list = [int(x.strip()) for x in day_list_text]
                current_day = datetime.datetime.now().day

            # assume instead that we have a list of day-of-the-week names
            except ValueError:
                day_list = [x.strip() for x in day_list_text]

                # current day is the day name, in this example
                current_day = datetime.datetime.now().strftime('%A')

                # check that our day names are valid
                valid_day_list = list(calendar.day_name)

                if not set(day_list).issubset(valid_day_list):
                    raise ValueError('Day list {} not subset of all valid day names'.format(day_list))

    # Check to see if today's date is in the list we just built
    # If so, update this layer
    if current_day in day_list:
        update_layer = True

    return update_layer


def main():
    all_layer_dict = gs.sheet_to_dict(args.environment)

    for layername, layerdef in all_layer_dict.iteritems():

        update_layer_today = parse_update_freq(layerdef['update_days'])

        if update_layer_today:
            python_exe = r'C:\PYTHON27\ArcGISx6410.5\python'
            subprocess.call([python_exe, 'gfw-sync.py', '-l', layername, '-e', args.environment])

    email_stats.send_summary()


if __name__ == '__main__':
    main()
