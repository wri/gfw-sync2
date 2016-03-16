__author__ = 'Thomas.Maschler'

import os
import arcpy
from layer import Layer
import warnings
import time
#from datetime import datetime
import cartodb
from settings import settings


class VectorLayer(Layer):
    """
    Vector layer class. Inherits from Layer
    """

    def __init__(self, layerdef):
        print 'starting vectorlayer'

        super(VectorLayer, self).__init__(layerdef)

        self._fc = None
        self.fc = self.esri_service_output

        self._dataset = None
        self.dataset = os.path.dirname(self.esri_service_output)

        self._sde_workspace = None
        self.sde_workspace = os.path.dirname(self.dataset)

        self.selection = None

        self.fields = self._get_fields()

        self._where_clause = None
        if "where_clause" in layerdef:
            self.where_clause = layerdef['where_clause']
        else:
            self.where_clause = None

        self._version = None
        self.create_version()

        self.wgs84_file = None
        self.export_file = None

    # Validate sde_workspace
    @property
    def sde_workspace(self):
        return self._sde_workspace

    @sde_workspace.setter
    def sde_workspace(self, s):
        print 'workspace: ' + s
        if not s:
            self._sde_workspace = ""
        else:
            if arcpy.Exists(s) and os.path.splitext(s)[1] == '.sde':
                self._sde_workspace = s
            else:
                warnings.warn("sde_workspace path is invalid", Warning)

    # Validate dataset
    @property
    def dataset(self):
        return self._dataset

    @dataset.setter
    def dataset(self, d):
        if not d:
            self._dataset = ""
            warnings.warn("There is no dataset specified", Warning)
        else:
            desc = arcpy.Describe(d)
            if desc.datasetType != 'FeatureDataset':
                warnings.warn("Dataset is not a FeatureDataset", Warning)
            self._dataset = d

    # Validate feature class
    @property
    def fc(self):
        return self._fc

    @fc.setter
    def fc(self, f):
        if not arcpy.Exists(f):
            warnings.warn("Feature class {0!s} does not exist".format(f), Warning)
        desc = arcpy.Describe(f)
        if desc.datasetType != 'FeatureClass':
            warnings.warn("Dataset {0!s} is not a FeatureClass".format(f), Warning)
        self._fc = f

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, v):
        arcpy.CreateVersion_management(self.sde_workspace, "sde.DEFAULT", v, "PRIVATE")
        self._version = v

    # Validate where clause
    @property
    def where_clause(self):
        return self._where_clause

    @where_clause.setter
    def where_clause(self, w):
        if self.source is not None and w != '':
                try:
                    arcpy.MakeFeatureLayer_management(self.source, self.name, w)
                    
                    #Clean up temporary feature layer after we create it
                    arcpy.Delete_management(self.name)
                except:
                    warnings.warn("Where clause '{0!s}' is invalide or delete FL failed".format(self.where_clause))
                    
        self._where_clause = w

    def project_to_wgs84(self):
        self._project_to_wgs84()

    def archive(self):
        if self.export_file is not None:
            self._archive(self.export_file, True)
        if self.wgs84_file is not None:
            self._archive(self.wgs84_file, False)

    def append_features(self, input_layer, fms=None):

        """  Append new features to Vector Layer
        :param input_layer: an arcpy layer
        :param fms: an arcpy fieldmap
        :return: Nothing
        """

        self.select()

        if fms is None:
            arcpy.Append_management(input_layer, self.selection.name, "NO_TEST")
        else:
            arcpy.Append_management(input_layer, self.selection.name, "NO_TEST", fms, "")

        #Delete temporary FC from self.select
        arcpy.Delete_management(self.name)

        return

    def create_version(self):
        self.version = self.name + "_" + str(int(time.time()))

    def delete_features(self, where_clause=None):
        """ Delete Features from Vector Layer
        :param where_clause: SQL Where statement
        :return: Nothing
        """
        self.select(where_clause)
        arcpy.DeleteFeatures_management(self.selection.name)
        
        #Delete temporary FC from self.select
        arcpy.Delete_management(self.name)

        return

    def delete_version(self, v=None):
        if v is None:
            v = self.version
        # Set the workspace environment
        arcpy.env.workspace = self.sde_workspace

        # Use a list comprehension to get a list of version names where the owner
        # is the current user and make sure sde.default is not selected.
        ver_list = [ver.name for ver in arcpy.da.ListVersions() if ver.isOwner
                   is True and ver.name.lower() != 'sde.DEFAULT']

        if v in ver_list:
            arcpy.DeleteVersion_management(self.sde_workspace, v)
        else:
            raise RuntimeError("Version {0!s} does not exist".format(v))

    def export_2_shp(self, simplify=False):
        """ Export Vector Layer to Shapefile
        :param simplify: Output features will be simplified (10 meter precision)
        :return: Nothing
        """

        export_folder = os.path.join(self.scratch_workspace, "export")
        if not os.path.exists(export_folder):
            os.mkdir(export_folder)

        self.export_file = os.path.join(export_folder, self.name + ".shp")

        if not simplify:
            arcpy.FeatureClassToShapefile_conversion([self.fc], export_folder)
        else:
            arcpy.SimplifyPolygon_cartography(self.fc,
                                              self.export_file,
                                              "POINT_REMOVE",
                                              "10 Meters",
                                              "0 Unknown",
                                              "RESOLVE_ERRORS",
                                              "KEEP_COLLAPSED_POINTS")

        if self.isWGS84(self.export_file):
            self.wgs84_file = self.export_file
        else:
            self.project_to_wgs84()

        return

    def _get_fields(self):
        return arcpy.ListFields(self.fc)

    def post_version(self):
        #TODO -- figure out why version is not being deleted
        #and check that correct version is being POSTed

        #For some reason I'm unable to invoke the DELETE_VERSION option in
        #ReconcileVersions_management. hence calling DeleteVersion right after
        arcpy.ReconcileVersions_management(self.sde_workspace, "ALL_VERSIONS", "sde.DEFAULT",  self.version, "LOCK_ACQUIRED", "NO_ABORT", "BY_OBJECT", "FAVOR_TARGET_VERSION", "POST", "KEEP_VERSION")
        arcpy.DeleteVersion_management(self.sde_workspace, self.version)

    def update(self):

        self.delete_features()

        if self.transformation:
            arcpy.env.geographicTransformations = self.transformation
        else:
            arcpy.env.geographicTransformations = ""

        if self.where_clause:
            arcpy.MakeFeatureLayer_management(self.source, "source_layer", self.where_clause)
        else:
            arcpy.MakeFeatureLayer_management(self.source, "source_layer")

        self.append_features("source_layer")

        self.update_gfwid()

        self.post_version()

        self.export_2_shp()

        self.archive()

        self.sync_cartodb(where_clause=None)

    def update_field(self, field, expression, language=None):
        if language is None:
            arcpy.CalculateField_management(self.selection.name, field, "'{0!s}'".format(expression), "PYTHON")
        else:
            arcpy.CalculateField_management(self.selection.name, field, expression, language, "")

    def update_gfwid(self):

        found = False
        for field in self.fields:
            if field.name == "gfwid":
                found = True
                break

        if not found:
            arcpy.AddField_management(self.fc, "gfwid", "TEXT", field_length="50", field_alias="GFW ID")
            self.fields = self._get_fields()

        self.select()

        arcpy.CalculateField_management(in_table=self.selection.name,
                                        field="gfwid",
                                        expression="md5(!Shape!.WKT)",
                                        expression_type="PYTHON_9.3",
                                        code_block="import hashlib\n"
                                                   "def md5(shape):\n"
                                                   "   hash = hashlib.md5()\n"
                                                   "   hash.update(shape)\n"
                                                   "   return hash.hexdigest()")

        #Delete temporary FC from self.select
        arcpy.Delete_management(self.name)

    def _select(self, where_clause=None):
        if where_clause is None and where_clause != '':
            l = arcpy.MakeFeatureLayer_management(self.fc, self.name)
            arcpy.ChangeVersion_management(self.name, 'TRANSACTIONAL', 'gfw.' + self.version, '')
        else:
            l = arcpy.MakeFeatureLayer_management(self.fc, self.name, where_clause)
            arcpy.ChangeVersion_management(self.name, 'TRANSACTIONAL', 'gfw.' + self.version, '')
        return l.getOutput(0)

    def select(self, where_clause=None):
        self.selection = self._select(where_clause)

    def sync_cartodb(self, where_clause):
        #TODO: Shapefile needs suffix _prefly
        cartodb.cartodb_sync(self.wgs84_file, self.name, where_clause)

        #TODO: Add extra procedure for imazon_sad
        #layerspec_table="layerspec_nuclear_hazard"
        #print "update layer spec max date"
        #sql = "UPDATE %s set maxdate= (SELECT max(date)+1 FROM %s) WHERE table_name='%s'" % (layerspec_table, production_table, production_table )
        #cartodb@wri-01.cartodb_sql(sql)