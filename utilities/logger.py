import logging
import os
import time


def build_logger(verbosity):
    """
    Instantiate a logger based on the verbosity specified in gfw-sync.py at the commandline
    :param verbosity:
    :return: a logger object used by the rest of the application
    """

    # Set logging output file, verbosity, and format
    log_file = os.path.join(os.getcwd(), 'logs', time.strftime("%Y%m%d") + '.log')
    logging.basicConfig(filename=log_file, level=verbosity.upper(), format='%(levelname)s | %(asctime)s | %(message)s',
                        datefmt='%H:%M:%S')

    # Set properties to that logging messages are displayed at the commandline as well
    console = logging.StreamHandler()
    console.setLevel(verbosity.upper())
    logging.getLogger().addHandler(console)

    # these libraries log automatically; set to only show critical messages
    logging.getLogger('oauth2client').setLevel(logging.CRITICAL)
    logging.getLogger("requests").setLevel(logging.CRITICAL)

    return logging.getLogger()
