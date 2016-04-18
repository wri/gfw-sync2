import argparse

import layer_decision_tree
from utilities import google_sheet
from utilities import logger
from utilities import settings


def main():

    # Parse commandline arguments
    parser = argparse.ArgumentParser(description='Get layer name, environment and verbosity for gfw-sync.')
    parser.add_argument('--environment', '-e', default='DEV', choices=('DEV', 'PROD'),
                        help='the environment/config files to use for this run')
    parser.add_argument('--layer', '-l', required=True,
                        help='the data layer to process; must match a value for tech_title in the config')
    parser.add_argument('--verbose', '-v', default='debug', choices=('debug', 'info', 'warning', 'error'),
                        help='set verbosity level to print and write to file')
    args = parser.parse_args()

    # Instantiate logger; write to {dir}\logs
    logging = logger.build_logger(args.verbose)
    logging.info("\n{0}\n{1} v{2}\n{0}\n".format('*' * 50, settings.get_settings(args.environment)['tool_info']['name'],
                                                 settings.get_settings(args.environment)['tool_info']['version']))
    logging.critical('Starting | {0}'.format(args.layer))

    # Get the layerdef from the Google Doc config based on the args supplied
    # Google Doc: https://docs.google.com/spreadsheets/d/1pkJCLNe9HWAHqxQh__s-tYQr9wJzGCb6rmRBPj8yRWI/edit#gid=0
    gs = google_sheet.GoogleSheet(args.environment)

    # Pass the layerdef and the google sheet object to the build_layer function
    layer = layer_decision_tree.build_layer(gs, args.layer)

    # Update the layer in the output data sources
    layer.update()

    # Update the last-updated timestamp in the Google Sheet
    gs.update_gs_timestamp(args.layer)

    logging.critical('Finished | {0}'.format(args.layer))

    # Delete scratch workspace
    layer.cleanup()

if __name__ == "__main__":
    main()
