import logging
import os
import time

def build_logger(verbosity):

    # Setting logging parameters
    log_file = os.path.join(os.getcwd(), 'logs', time.strftime("%Y%m%d") + '.log')
    logging.basicConfig(filename=log_file, level=verbosity.upper())

    console = logging.StreamHandler()
    console.setLevel(verbosity.upper())

    logging.getLogger().addHandler(console)

    # these libraries log automatically; set to only show critical messages
    logging.getLogger('oauth2client').setLevel(logging.CRITICAL)
    logging.getLogger("requests").setLevel(logging.CRITICAL)

    return logging.getLogger()
