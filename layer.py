import os
import settings
import arcpy
import warnings
import urllib
import util
import shutil


class Layer(object):

    def __init__(self, layerdef, src=None):

        self._name = None
        self._alias = None
        self._src = None
        self._transformation = None
        self._where_clause = None

        self.type = "Layer"

        self.sets = settings.get_settings()

        self.src = src
        self._src_o = src
        self._src_t = None

        self.name = layerdef['name']
        self.alias = layerdef['alias']

        self._gdb_connection = layerdef['gdb_connection']
        self._fc = layerdef['fc']

        self.feature_class = self._get_feature_class()
        self.selection = self._select()
        self.replica = layerdef['replica']

        self.export_file = None
        self.wgs84_file = None
        self.zip_file = None
        self.zip_file_local = None
        self.archive_file = None
        self.archive_file_local = None

        self.srs = self.sets["spatial_references"]["default_srs"]

        self._temp_folder = self.sets["paths"]["scratch_workspace"]
        # self._wrong_def = "Incorrect definition of layer '%s' \r" % self.name

        self.fields = self._get_fields()

        if "transformation" in layerdef:
            self.transformation = layerdef['transformation']
        else:
            self.transformation = None

        if "where_clause" in layerdef:
            self.where_clause = layerdef['where_clause']
        else:
            self.where_clause = None

    # Validate name
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, n):
        if not n:
            warnings.warn("Name cannot be empty", Warning)
        self._name = n

    # Validate alias
    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, a):
        if not a:
            warnings.warn("Alias cannot be empty", Warning)
        self._alias = a

    # Validate source
    @property
    def src(self):
        return self._src

    @src.setter
    def src(self, s):
        if s is not None:
            if not arcpy.Exists(s):
                warnings.warn("Cannot find source %s" % s, Warning)
        self._src = s

    # Validate transformation
    @property
    def transformation(self):
        return self._transformation

    @transformation.setter
    def transformation(self, t):
        if self.src is not None:
            desc = arcpy.Describe(self.src)
            from_srs = desc.spatialReference
            to_srs = arcpy.SpatialReference(self.srs)
            if from_srs.GCS != to_srs.GCS:
                if t is None:
                    warnings.warn("No transformation defined", Warning)
                else:
                    extent = desc.extent
                    transformations = arcpy.ListTransformations(from_srs, to_srs, extent)
                    if self.transformation not in transformations:
                        warnings.warn("Transformation %s: not compatible with in- and output spatial reference or extent"
                                      % self.transformation, Warning)
        self._transformation = t

    # Validate where clause
    @property
    def where_clause(self):
        return self._where_clause

    @where_clause.setter
    def where_clause(self, w):
        if self.src is not None:
            if w is not None:
                try:
                    arcpy.MakeFeatureLayer_management(self.src, self.name, w)
                except:
                    warnings.warn("Where clause '%s' is invalide" % self.where_clause)
        self._where_clause = w

    def archive(self):
        pass

    def append_features(self, input_layer, fms=None):
        if fms is None:
            arcpy.Append_management(input_layer, self.feature_class, "NO_TEST")
        else:
            arcpy.Append_management(input_layer, self.feature_class, "NO_TEST", fms, "")

    def clean_up(self):
        self._delete_local_copies()
        self._delete_local_source()
        self._delete_local_zips()
        self._delete_local_archives()

    def copy_2_s3(self):
        pass
        # if self.export_file:
        #     shutil.copy(self.export_file, s3)
        # if self.zip_file:
        #     shutil.copy(self.zip_file, s3)
        # if self.zip_file_local:
        #     shutil.copy(self.zip_file_local, s3)
        # if self.archive_file:
        #     shutil.copy(self.archive_file, s3)
        # if self.archive_file_local:
        #     shutil.copy(self.archive_file_local, s3)

    def copy_source(self):
        src_name = os.path.basename(self.src)
        source_folder = os.path.join(self._temp_folder, "source")
        if not os.path.exists(source_folder):
            os.mkdir(source_folder)
        self._src_t = os.path.join(source_folder, src_name)
        shutil.copy(self.src, self._src_t)
        self.src = self._src_t

    def delete_features(self, where_clause=None):
        self.select(where_clause)
        arcpy.DeleteFeatures_management(self.selection.name)

    def _delete_local_archives(self):
        pass

    def _delete_local_copies(self):
        if self.export_file is not None:
            os.remove(self.export_file)
            self.export_file = None
        if self.wgs84_file is not None:
            os.remove(self.wgs84_file)
            self.wgs84_file = None

    def _delete_local_source(self):
        if self._src_t is not None and self._src_t == self.src:
            os.remove(self._src_t)
            self._src_t = None
        self.src = self._src_o

    def _delete_local_zips(self):
        pass

    def export_2_shp(self, wgs84=True, simplify=False):
        export_folder = os.path.join(self._temp_folder, "export")
        if not os.path.exists(export_folder):
            os.mkdir(export_folder)
        self.export_file = os.path.join(export_folder, self.name + ".shp")

        if not simplify:
            arcpy.FeatureClassToShapefile_conversion([self.feature_class], export_folder)
        else:
            arcpy.SimplifyPolygon_cartography(self.feature_class,
                                              self.export_file,
                                              "POINT_REMOVE",
                                              "10 Meters",
                                              "0 Unknown",
                                              "RESOLVE_ERRORS",
                                              "KEEP_COLLAPSED_POINTS")
        if wgs84:
            wgs84_folder = os.path.join(export_folder, "wgs84")
            if not os.path.exists(wgs84_folder):
                os.mkdir(wgs84_folder)
            self.wgs84_file = os.path.join(wgs84_folder, self.name + ".shp")
            arcpy.Project_management(self.export_file, self.wgs84_file, "WGS_1984")

    def _get_feature_class(self):
        conn = self._get_gdb_connection()
        return os.path.join(conn, self._fc)

    def _get_fields(self):
        return arcpy.ListFields(self.feature_class)

    def _get_gdb_connection(self):
        return self.sets["gdb_connections"][self._gdb_connection]

    def overwrite_feature_class(self):
        pass

    def push_2_cartodb(self):
        pass

    def rebuild_cache(self):
        pass

    def _select(self, where_clause=None):
        try:
            arcpy.Delete_management(self.name)
        except:
            pass

        if where_clause is None:
            l = arcpy.MakeFeatureLayer_management(self.feature_class, self.name)
        else:
            l = arcpy.MakeFeatureLayer_management(self.feature_class, self.name, where_clause)
        return l.getOutput(0)

    def select(self, where_clause=None):
        self.selection = self._select(where_clause)

    def sync(self):
        pass

    def update(self):

        self.delete_features()

        if self.transformation:
            arcpy.env.geographicTransformations = self.transformation
        else:
            arcpy.env.geographicTransformations = ""

        if self.where_clause:
            arcpy.MakeFeatureLayer_management(self.src, "source_layer", self.where_clause)
        else:
            arcpy.MakeFeatureLayer_management(self.src, "source_layer")

        self.append_features("source_layer")

    def update_field(self, field, expression, language=None):
        if language is None:
            arcpy.CalculateField_management(self.selection.name, field, "'%s'" % expression, "PYTHON")
        else:
            arcpy.CalculateField_management(self.selection.name, field, expression, language, "")

    def update_gfwid(self):

        found = False
        for field in self.fields:
            if field.name == "gfwid":
                found = True
                break

        if not found:
            arcpy.AddField_management(self.feature_class, "gfwid", "TEXT", field_length="50", field_alias="GFW ID")
            self.fields = self._get_fields()

        arcpy.CalculateField_management(in_table=self.feature_class,
                                        field="gfwid",
                                        expression="md5(!Shape!.WKT)",
                                        expression_type="PYTHON_9.3",
                                        code_block="import hashlib\n"
                                                   "def md5(shape):\n"
                                                   "   hash = hashlib.md5()\n"
                                                   "   hash.update(shape)\n"
                                                   "   return hash.hexdigest()")

    def update_metadata(self):
        pass

    def zip(self):
        pass


class S3Layer(Layer):

    def __init__(self, layerdef):

        self._wrong_def = "Incorrect definition of layer '%s' \r" % self.name

        self.bucket = layerdef['bucket']
        self.folder = layerdef['folder']

        self._shp = "%s.shp" % self.name
        self._drive = self.sets["bucket_drives"][self.bucket]
        self._path = os.path.join(self._drive, self.folder)
        self._shp_path = os.path.join(self._path, self._shp)

        self.src = self._shp_path

        super(S3Layer, self).__init__(layerdef, self.src)

        self.type = "S3Layer"

    # Validate bucket
    @property
    def bucket(self):
        return self.bucket

    @bucket.setter
    def bucket(self, b):
        if not b:
            warnings.warn("%sBucket cannot be empty" % self._wrong_def, Warning)
        if b not in self.sets["bucket_drives"]:
            warnings.warn("%sBucket '%s' not registered in config file" % (self._wrong_def, b), Warning)
        self.bucket = b

    # Validate folder
    @property
    def folder(self):
        return self.folder

    @folder.setter
    def folder(self, f):
        if not f:
            warnings.warn("%s\nFolder cannot be empty" % self._wrong_def, Warning)
        if not os.path.exists(os.path.join(self._drive, f)):
            warnings.warn("%s\nPath '%s' does not exist" % (self._wrong_def, os.path.join(self._drive, f)), Warning)
        self.folder = f


class WDPALayer(Layer):
    def __init__(self, layerdef):

        self._url = None
        self.url = layerdef['url']
        self._src_gdb = None
        self.src = None

        super(WDPALayer, self).__init__(layerdef, self.src)
        self.type = "WDPALayer"

    # Validate folder
    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, u):
        try:
            urllib.urlopen(u)  # TODO: Check if there is a better way to check URL
        except:
            warnings.warn("%s\nURL '%s' does not exist" % (self._wrong_def, u), Warning)
        self._url = u

    def _download(self, url):

        name = os.path.basename(url)
        zip_name = "%s.zip" % name
        zip_path = os.path.join(self._temp_folder, zip_name)
        gdb = os.path.join(self._temp_folder, "%s.gdb" % name)

        if os.path.exists(zip_path):
            os.remove(zip_path)
        if arcpy.Exists(gdb):
            arcpy.Delete_management(gdb)

        urllib.urlretrieve(url, zip_path)

        util.unzip(zip_path, self._temp_folder)
        os.remove(zip_path)
        return gdb

    def _get_src(self):
        self._src_gdb = self._download(self.url)
        arcpy.env.workspace = self._src_gdb
        fc_list = arcpy.ListFeatureClasses()

        for fc in fc_list:
            desc = arcpy.Describe(fc)
            if desc.shapeType == 'Polygon':
                return os.path.join(self._src_gdb, fc)

    def update(self):
        self.src = self._get_src()
        # ...

    def __del__(self):
        arcpy.Delete_management(self._src_gdb)

