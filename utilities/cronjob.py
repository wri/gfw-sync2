import os
import sys
import subprocess
import datetime

import google_sheet
import email_stats

gfw_env = sys.argv[1].upper()

# # TODO remove this
os.remove(r"D:\scripts\gfw-sync2\logs\20160406.log")

def parse_update_freq(field_text):
    update_layer = False

    # Check that the layer has an update frequency first
    if field_text:

        if field_text[0] == '[' and field_text[-1] == ']':
            field_text = field_text.replace('[','').replace(']','')

            if '-' in field_text:
                start_day, end_day = field_text.split('-')
                day_list = range(int(start_day), int(end_day) + 1)

            else:
                day_list_text = field_text.split(',')
                day_list = [int(x.strip()) for x in day_list_text]

        if datetime.datetime.now().day in day_list:
            update_layer = True

    return update_layer

gs = google_sheet.GoogleSheet(gfw_env)
all_layer_dict = gs.sheet_to_dict()

for layername, layerdef in all_layer_dict.iteritems():

    update_layer_today = parse_update_freq(layerdef['update_days'])

    if update_layer_today:
        subprocess.call(['python', 'gfw-sync.py', '-l', layername], cwd=os.path.dirname(os.getcwd()))

email_stats.send_summary()