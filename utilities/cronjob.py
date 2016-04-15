import os
import subprocess
import datetime
import argparse

import google_sheet
import email_stats

parser = argparse.ArgumentParser(description='Pass environment to kick off gfw-sync cron job.')
parser.add_argument('--environment', '-e', default='DEV', choices=('DEV', 'PROD'),
                    help='the environment/config files to use for this run')
args = parser.parse_args()


def parse_update_freq(field_text):
    update_layer = False

    # Check that the layer has an update frequency first
    if field_text:

        # If the field text has brackets, let's examine it
        if field_text[0] == '[' and field_text[-1] == ']':
            field_text = field_text.replace('[', '').replace(']', '')

            # If it has '-', assume it's a range and build a list
            if '-' in field_text:
                start_day, end_day = field_text.split('-')
                day_list = range(int(start_day), int(end_day) + 1)

            # Otherwise assume it's a list of dates
            else:
                day_list_text = field_text.split(',')
                day_list = [int(x.strip()) for x in day_list_text]

        # Check to see if today's date is in the list we just built
        # If so, update this layer
        if datetime.datetime.now().day in day_list:
            update_layer = True

    return update_layer

gs = google_sheet.GoogleSheet(args.environment)
all_layer_dict = gs.sheet_to_dict()

for layername, layerdef in all_layer_dict.iteritems():

    update_layer_today = parse_update_freq(layerdef['update_days'])

    if update_layer_today:
        subprocess.call(['python', 'gfw-sync.py', '-l', layername])

email_stats.send_summary()
