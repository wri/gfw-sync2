import os
import time
import argparse
import settings
import logging

from utilities import google_sheet

import layer_decision_tree

def main():

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--environment', '-e',
                       help='the environment/config files to use for this run', default='DEV', choices=('DEV', 'PROD'))
    parser.add_argument('--layer', '-l', required=True,
                       help='the data layer to process; must match a value for tech_title in the config')
    parser.add_argument('--verbose', '-v', default='debug', choices=('debug', 'warning', 'error'),
                       help='set verbosity level to print and write to file')
    args = parser.parse_args()

    # Setting logging parameters
    log_file = os.path.join(os.getcwd(), 'logs', time.strftime("%Y%m%d"))
    logging.basicConfig(filename=log_file, level=args.verbose.upper())

    console = logging.StreamHandler()
    console.setLevel(args.verbose.upper())

    logging.getLogger('').addHandler(console)

    # oauth2client logs automatically; set to only show critical messages
    logging.getLogger('oauth2client').setLevel(logging.CRITICAL)

    logging.critical("{0!s} v{1!s}\n".format(settings.get_settings(args.environment)['tool_info']['name'],
                                settings.get_settings(args.environment)['tool_info']['version']))

    # Get the layerdef from the Google Doc config based on the args supplied
    # Google Doc: https://docs.google.com/spreadsheets/d/1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI/edit#gid=0
    gs = google_sheet.GoogleSheet(args.environment)
    layerdef = gs.get_layerdef(args.layer)

    # Pass the layerdef and the google sheet object to the build_layer function
    layer = layer_decision_tree.build_layer(layerdef, gs)

    # Update the layer in the output data sources
    layer.update()

    # Update the last-updated timestamp in the Google Sheet
    gs.update_gs_timestamp(args.layer)

    # TODO add cleanup method (layer.cleanup() to delete scratch workspaces, etc
    # TODO add cleanup method for datasource too . . . maybe in the layer module?

if __name__ == "__main__":
    main()
