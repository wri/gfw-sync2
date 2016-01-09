from vector_layer import VectorLayer

class S3Layer(VectorLayer):

    def __init__(self, layerdef):

        self._wrong_def = "Incorrect definition of layer '%s' \r" % self.name

        self.bucket = layerdef['bucket']
        self.folder = layerdef['folder']

        self._shp = "%s.shp" % self.name
        self._drive = settings["bucket_drives"][self.bucket]
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
        if b not in settings["bucket_drives"]:
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
