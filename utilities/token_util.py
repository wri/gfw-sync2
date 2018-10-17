import json
import os
from ConfigParser import ConfigParser


def get_token(token_file):
    """
    Grab token from the tokens\ folder in the root directory
    :param token_file: name of the file
    :return: the token value
    """
    abspath = os.path.abspath(__file__)
    dir_name = os.path.dirname(os.path.dirname(abspath))
    token_path = os.path.join(dir_name, r"tokens\{0!s}".format(token_file))

    if not os.path.exists(token_path):
        raise IOError('Cannot find any token for {0!s}\n Make sure there is a file called {1!s} '
                      'in the tokens directory'.format(token_file, token_file))
    else:
        if os.path.splitext(token_path)[1] == '.json':
            return json.load(open(token_path))
        elif os.path.splitext(token_path)[1] == '.config':
            parser = ConfigParser()
            parser.read(token_path)
            return parser.items('Credentials')
        else:
            with open(token_path, "r") as f:
                for row in f:
                    return row