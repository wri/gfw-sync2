import json
import os
import time
import urllib2
import logging
import arcpy
import sys

from utilities import util
from datasource import DataSource


class HotOsmExportDataSource(DataSource):
    def __init__(self, layerdef):
        logging.debug('starting hot_osm_export_datasource')

        super(HotOsmExportDataSource, self).__init__(layerdef)

        self.layerdef = layerdef

        self.job_dict = {x: {} for x in self.source.split(',')}

    def run_job(self, job_uid, job_type):
        auth_key = util.get_token('thomas.maschler@hot_export')
        headers = {"Content-Type": "application/json", "Authorization": "Token " + auth_key}
        url = "http://export.hotosm.org/api/{0}?job_uid={1}".format(job_type, job_uid)

        request = urllib2.Request(url)

        for key, value in headers.items():
            request.add_header(key, value)

        return urllib2.urlopen(request)

    def any_jobs_processing(self):
        jobs_processing = {k: v for k, v in self.job_dict.iteritems() if v['status'] == 'SUBMITTED'}.keys()

        if jobs_processing:
            status = True
        else:
            status = False

        return status

    # def all_jobs_finished(self):
    #     jobs_finished = {k:v for k,v in self.job_dict.iteritems() if v['status'] == 'FINISHED'}.keys()
    #
    #     if len(jobs_finished) == len(self.job_dict.keys()):
    #         status = True
    #     else:
    #         status = False
    #
    #     return status

    def execute_all_jobs(self):

        # Start all jobs
        for job_uid in self.job_dict.keys():
            self.start_job(job_uid)
            self.job_dict[job_uid]['extract_attempts'] = 1

        # # After we've started all jobs, try 5 times to successfully complete exports
        # for i in range(0, 5):

        # # If all jobs finished, break out of this loop
        # if self.all_jobs_finished():
        #     logging.debug('all jobs finished!')
        #     break
        #
        # # Not all jobs are finished
        # else:

        # Check if any jobs are still processing
        while self.any_jobs_processing():

            # Sleep because jobs are still processing
            logging.debug('Some jobs processing-- sleeping for 1 minute')
            time.sleep(60)

            # Check on all jobs again, then go back to while statement
            # in case all have finished
            for job_uid in self.job_dict.keys():
                self.get_job_info(job_uid)

        # # After all jobs have finished, leave the while loop
        # # Find the jobs that didn't succeed
        # # Restart them, and then go back to the range(0, 5) loop to
        # # wait for them to complete
        # for job_uid in self.job_dict.keys():

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
                    logging.debug("job {0} keeps failing. we'll skip it for now".format(job_uid))
                    pass

    def start_job(self, job_uid):
        data = json.load(self.run_job(job_uid,  'rerun'))

        self.job_dict[job_uid]['status'] = data['status']

    def get_job_info(self, job_uid):
        data = json.load(self.run_job(job_uid, 'runs'))

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

        for job_uid in self.job_dict.keys():

            if self.job_dict[job_uid]['status'] == 'FINISHED':
                zip_file = self.download_file(self.job_dict[job_uid]["url"], self.download_workspace)

                self.unzip(zip_file, self.download_workspace)

    def process_downloaded_data(self):

        arcpy.env.workspace = self.download_workspace
        download_fc_list = arcpy.ListFeatureClasses()

        if len(download_fc_list) == 1:
            merged_fc = download_fc_list[0]

        else:
            merged_fc = os.path.join(self.download_workspace, 'merged_output.shp')
            arcpy.Merge_management(download_fc_list, merged_fc)

        dissolved_fc = os.path.join(self.download_workspace, 'dissolved_final.shp')

        arcpy.Dissolve_management(merged_fc,
                                  dissolved_fc,
                                  "osm_id;access;bridge;end_date;ferry;ford;highway;informal;maxspeed;name;oneway;"
                                  "opening_ho;operator;ref;route;seasonal;smoothness;source;start_date;surface;"
                                  "trail_visi;tunnel;width",
                                  "",
                                  "MULTI_PART",
                                  "DISSOLVE_LINES")

        self.source = dissolved_fc

    def get_layer(self):

        self.execute_all_jobs()

        self.download_results()

        self.process_downloaded_data()

        self.layerdef['source'] = self.source

        return self.layerdef
