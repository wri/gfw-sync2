#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import requests
import cgi
import arcpy_metadata

import settings


def update_metadata(in_fc, tech_title, gfw_env):

    api_url = settings.get_settings(gfw_env)['metadata']['api_url']
    layer_url = api_url + r'/' + tech_title

    response = requests.get(layer_url)
    api_data = json.loads(response.text)

    md = arcpy_metadata.MetadataEditor(in_fc)

    md.title = escape_html(api_data['title'])
    md.purpose = escape_html(api_data['function'])
    md.abstract = escape_html(api_data['overview'])
    md.tags = api_data['tags'].split(",")
    md.extent_description = escape_html(api_data['geographic_coverage'])
    md.last_update = escape_html(api_data['date_of_content'])
    md.update_frequency = escape_html(api_data['frequency_of_updates'])
    md.citation = escape_html(api_data['citation'])
    md.limitation = escape_html(api_data['cautions'])
    md.source = escape_html(api_data['source'])
    md.scale_resolution = escape_html(api_data['resolution'])
    md.supplemental_information = escape_html(api_data['other'])

    md.finish()


def escape_html(text_string):

    # Replace all the <p> tags-- not needed in XML
    text_string = text_string.replace('<p>', '').replace('</p>', '')
    # text_string = text_string.replace('<br>', '')
    # # udata = text_string.decode('utf-8')

    return cgi.escape(text_string)
    # return cgi.escape(text_string).encode('ascii', 'xmlcharrefreplace')




