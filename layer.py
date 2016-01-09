import os
from settings import settings
import arcpy
import warnings
import shutil


class Layer(object):

    def __init__(self, layerdef):
        """ A general Layer class
        :param layerdef: A Layer definition dictionary
        :param src:
        :return:
        """

        self.type = "Layer"

        self._name = None
        self.name = layerdef['name']

        self._alias = None
        self.alias = layerdef['alias']

        self._workspace = None
        self.workspace = layerdef['workspace']

        self._scratch_workspace = None
        self.scratch_workspace = os.path.join(settings['paths']['scratch_workspace'], self.name)

        self._src = None
        self.src = layerdef['src']


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

    # Validate Scratch workspace
    @property
    def scratch_workspace(self):
        return self._scratch_workspace

    @scratch_workspace.setter
    def scratch_workspace(self, s):
        if os.path.exists(s):
            shutil.rmtree(s)
        os.mkdir(s)
        self._scratch_workspace = s

    # Validate workspace
    @property
    def workspace(self):
        return self._workspace

    @workspace.setter
    def workspace(self, w):
        if not w:
            warnings.warn("Workspace cannot be empty", Warning)
        else:
            desc = arcpy.Describe(w)
            if desc.dataType != "Workspace" or desc.dataType != "Folder":
                warnings.warn("Not a Workspace", Warning)
        self._workspace = w

    # Validate source
    @property
    def src(self):
        return self._src

    @src.setter
    def src(self, s):
        if s is not None:
            if not arcpy.Exists(s):
                warnings.warn("Cannot find source %s" % s, Warning)
            else:
                drive = os.path.splitdrive(s)[0]
                if drive in settings["bucket_drives"].values():
                    out_data = os.path.join(self.scratch_workspace, os.path.basename(s))
                    arcpy.Copy_management(s, out_data)
                    s = out_data
        self._src = s

    def archive(self):
        pass

    def update_metadata(self):
        pass



