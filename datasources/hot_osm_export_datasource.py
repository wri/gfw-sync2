import json
import os
import time
import urllib2
import logging
import arcpy

import utilities.token_util
from utilities import field_map
from datasource import DataSource


class HotOsmExportDataSource(DataSource):
    """
    HotOsmExport datasource class. Inherits from DataSource
    Takes a comma separated list of HOT OSM job uids in layerdef['source'] and re-exports them. It then merges the
    outputs (all logging roads at this point) and then dissolves. This dissolved line FC is used as an input to the
    layer.update() process
    """
    def __init__(self, layerdef):
        logging.debug('Starting hot_osm_export_datasource')
        super(HotOsmExportDataSource, self).__init__(layerdef)

        self.layerdef = layerdef
        self.job_dict = {x: {} for x in self.data_source.split(',')}

    @staticmethod
    def run_job(job_uid, job_type):
        """
        Used to kick off and monitor job progress
        :param job_uid: HOT OSM job uid
        :param job_type: reruns kicks off the job again, runs just monitors job progress
        :return: return the json output
        """
        auth_key = utilities.token_util.get_token('thomas.maschler@hot_export')
        headers = {"Content-Type": "application/json", "Authorization": "Token " + auth_key}
        url = "http://export.hotosm.org/api/{0}?job_uid={1}".format(job_type, job_uid)

        request = urllib2.Request(url)

        for key, value in headers.items():
            request.add_header(key, value)

        return json.load(urllib2.urlopen(request))

    def any_jobs_processing(self):
        """Check if any jobs are processing by looking at the statuses in the job_dict"""
        jobs_processing = {k: v for k, v in self.job_dict.iteritems() if v['status'] == 'SUBMITTED'}.keys()

        if jobs_processing:
            status = True
        else:
            status = False

        return status

    def execute_all_jobs(self):
        """Start all jobs, then wait for them to finish. If a job fails, restart it"""
        # Start all jobs
        for job_uid in self.job_dict.keys():
            self.start_job(job_uid)
            self.job_dict[job_uid]['extract_attempts'] = 1

        # Check if any jobs are still processing
        while self.any_jobs_processing():

            # Sleep because jobs are still processing
            logging.debug('Some jobs processing-- sleeping for 1 minute')
            time.sleep(60)

            # Check on all jobs again, then go back to while statement
            # in case all have finished
            for job_uid in self.job_dict.keys():
                self.get_job_info(job_uid)

                # If the job is in process or finished, don't restart it
                if self.job_dict[job_uid]['status'] in ['SUBMITTED', 'FINISHED']:
                    pass

                elif self.job_dict[job_uid]['extract_attempts'] <= 4:
                    logging.debug('restarting job {0}'.format(job_uid))
                    self.start_job(job_uid)

                    # Increment the counter so we know how many times we've attempted this extract
                    self.job_dict[job_uid]['extract_attempts'] += 1

                # If we've tried this layer 5 times, forget it
                else:
                    logging.debug("job {0} has failed {1} times. we'll skip it "
                                  "for now".format(job_uid, self.job_dict[job_uid]['extract_attempts']))

    def start_job(self, job_uid):
        data = self.run_job(job_uid,  'rerun')

        self.job_dict[job_uid]['status'] = data['status']

    def get_job_info(self, job_uid):
        """Check on the job of interest. Update the 'status' key for that job uid
        in self.job_dict based on the results"""
        data = self.run_job(job_uid, 'runs')

        if data[0]['status'] == 'SUBMITTED':
            self.job_dict[job_uid]['status'] = 'SUBMITTED'

        elif data[0]['status'] == 'COMPLETED' and data[0]['tasks'][4]['status'] == 'SUCCESS':
            self.job_dict[job_uid]["url"] = data[0]['tasks'][4]['result']['url']
            self.job_dict[job_uid]['status'] = 'FINISHED'

        else:
            self.job_dict[job_uid]['status'] = 'FAILED'

        logging.debug('job {0}: {1}, attempt {2}'.format(job_uid, self.job_dict[job_uid]['status'],
                                                         self.job_dict[job_uid]['extract_attempts']))

    def download_results(self):
        """
        Download the zip files for all complete jobs and unzip
        :return: a list of shapefiles
        """
        shp_list = []

        for job_uid in self.job_dict.keys():

            if self.job_dict[job_uid]['status'] == 'FINISHED':
                zip_file = self.download_file(self.job_dict[job_uid]["url"], self.download_workspace)

                out_job_dir = os.path.join(self.download_workspace, job_uid)

                self.unzip(zip_file, out_job_dir)

                arcpy.env.workspace = out_job_dir
                shp_name = arcpy.ListFeatureClasses()[0]

                shp_list.append(os.path.join(out_job_dir, shp_name))

        return shp_list

    @staticmethod
    def get_max_len_all_fields(fc_list):
        """ Important for field mapping-- OSM exports have different length fields in different shapefiles
        :param fc_list: a list of input shapefiles
        :return: a dict of {'fieldname' : maxLength} for each field in the input FCs
        """

        field_dict = {}

        for fc in fc_list:
            for f in arcpy.ListFields(fc):

                if not f.required:
                    try:
                        field_dict[f.name].append(f.length)
                    except KeyError:
                        field_dict[f.name] = [f.length]

        return {k: max(v) for k, v in field_dict.iteritems()}

    def process_downloaded_data(self, input_fc_list):
        """
        Defines a field map and uses it to merge the input feature classes (logging road extracts from different
        regions). Dissolves the output to get rid of duplicate features.
        :param input_fc_list:
        :return:
        """

        if len(input_fc_list) == 1:
            single_part_fc = input_fc_list[0]

        else:
            field_max_dict = self.get_max_len_all_fields(input_fc_list)

            fm_dict = {k: {'out_length': v} for k, v in field_max_dict.iteritems()}
            fms = field_map.build_field_map(input_fc_list, fm_dict)

            merged_fc = os.path.join(self.download_workspace, 'merged_output.shp')
            arcpy.Merge_management(input_fc_list, merged_fc, fms)

            dissolved_fc = os.path.join(self.download_workspace, 'dissolved.shp')
            out_fields = ['osm_id', 'access', 'bridge', 'end_date', 'ferry', 'ford', 'highway', 'informal',
                          'maxspeed', 'name', 'oneway', 'opening_ho', 'operator', 'ref', 'route', 'seasonal',
                          'smoothness', 'source', 'start_date', 'surface', 'trail_visi', 'tunnel', 'width']

            arcpy.Dissolve_management(merged_fc, dissolved_fc, ';'.join(out_fields), "", "MULTI_PART", "DISSOLVE_LINES")

            single_part_fc = os.path.join(self.download_workspace, 'single_part_final.shp')
            arcpy.MultipartToSinglepart_management(dissolved_fc, single_part_fc)

        self.data_source = single_part_fc

    def get_layer(self):
        """
        Executes all jobs, downloads results, and merges them into one FC.
        Returns an updated layerdef with this merged FC as the source
        :return:
        """

        self.execute_all_jobs()

        all_unzipped_fcs = self.download_results()

        self.process_downloaded_data(all_unzipped_fcs)

        self.layerdef['source'] = self.data_source

        return self.layerdef
