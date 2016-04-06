#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# enable debugging

import json

import gspread
from oauth2client.client import SignedJwtAssertionCredentials

import util
from utilities.settings import settings


def open_spreadsheet():

    # specify oauth2client credentials
    json_key = util.get_token('wriforests@google')
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope)

    # authorize oauth2client credentials
    gc = gspread.authorize(credentials)

    # open the metadata entry spreadsheet
    wks = gc.open(settings['metadata']['gspread']).sheet1
    # wks = gc.open_by_id("1hJ48cMrADMEJ67L5hTQbT5hhV20YCJHpN1NwjXiC3pI").sheet1

    return wks


def get_layer_names(wks):
    return wks.col_values(2)[1:]


def fetch_metadata(wks, row):

    # define metadata variables that correspond to cells in the metadata spreadsheet
    md = dict()
    md["Title"] = wks.cell(row, 3).value
    md["Translated_Title"] = wks.cell(row, 14).value
    md["Function"] = wks.cell(row, 4).value
    md["Overview"] = wks.cell(row, 12).value
    md["Translated Overview"] = wks.cell(row, 16).value
    #md["category"] = wks.cell(row, 10).value
    md["Tags"] = wks.cell(row, 17).value #.value.split(", ")
    md["Geographic Coverage"] = wks.cell(row, 6).value
    md["Date of Content"] = wks.cell(row, 9).value
    md["Frequency of Updates"] = wks.cell(row, 8).value
    #md["credits"] = wks.cell(row, 18).value
    md["Citation"] = wks.cell(row, 13).value
    md["License"] = wks.cell(row, 11).value
    md["Cautions"] = wks.cell(row, 10).value
    md["Source"] = wks.cell(row, 7).value
    md["Resolution"] = wks.cell(row, 5).value

    return md


def rebuild_cache(f):

    wks = open_spreadsheet()
    layers = get_layer_names(wks)
    md = {}
    i = 1
    for layer in layers:
        i += 1
        md[layer] = fetch_metadata(wks, i)

    with open(f, 'w') as cache:
        cache.write(json.dumps(md, ensure_ascii=True, sort_keys=True, indent=4, separators=(',', ': ')).encode('utf8'))





