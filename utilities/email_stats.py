import os
import time
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

import util


def send_summary():
    """
    Read log file and send the result email
    :return:
    """
    root_dir = os.getcwd()
    log_file = os.path.join(root_dir, 'logs', time.strftime("%Y%m%d") + '.log')

    result_text = read_log_to_result_text(log_file)
    send_email(result_text)


def parse_line_add_result(input_line, input_dict):
    """
    Parse the log file for lines of interest and store their results
    :param input_line: a line from the log file
    :param input_dict: a dictionary to store layer-specific results
    :return: input_dict, now including additional results
    """

    # Pull only lines with the critical flag
    if input_line[0:8] == 'CRITICAL':
        split_line = input_line.split('|')

        # Format CRITICAL|logname|status|layername
        if len(split_line) == 4:

            layername = split_line[3].strip()
            result = split_line[2].strip()

            # If the layername is aleady in the dict, append the result to the list
            # Otherwise create the key
            try:
                input_dict[layername].append(result)
            except KeyError:
                input_dict[layername] = [result]

    return input_dict


def read_log_to_result_text(log):
    """
    Open the log file, parse it, and then inspect the return dict to understand if layers succeeded/failed
    :param log: path to log file
    :return: text output for an email
    """
    layer_log_dict = {}

    with open(log, 'rb') as theFile:
        for line in theFile:
            parse_line_add_result(line, layer_log_dict)

    final_dict = {'success': [], 'checked': [], 'failure': []}

    for layername, result_list in layer_log_dict.iteritems():
        if result_list == ['Starting', 'Finished']:
            final_dict['success'].append(layername)
        elif result_list == ['Starting', 'Checked']:
            final_dict['checked'].append(layername)
        else:
            final_dict['failure'].append(layername)

    text_output = 'These layers succeeded:<br>{0}<br><br>' \
                  'These layers were checked but no new data found:<br>{1}<br><br>' \
                  'These layers failed:<br>{2}' \
                  ''.format('<br>'.join(final_dict['success']), '<br>'.join(final_dict['checked']),
                            '<br>'.join(final_dict['failure']))

    return text_output


def send_email(body_text):
    """
    Send an email given a body text
    :param body_text: text to include in the email
    :return:
    """
    username = 'wriforests'

    fromaddr = "{0}@gmail.com".format(username)
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['Subject'] = "gfw-sync2 results"
    msg.attach(MIMEText(body_text, 'html'))

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(fromaddr, util.get_token(username))

    for toaddr in ["chofmann@wri.org", "mweisse@wri.org"]:
        msg['To'] = toaddr
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)

    server.quit()
