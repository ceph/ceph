import os


class MultipleZPoolsError(Exception):

    def __init__(self, zp_name):
        self.pv_name = zp_name

    def __str__(self):
        msg = "Got more than 1 result looking for zpool: %s" % self.zp_name
        return msg


class MultipleZFSError(Exception):

    def __init__(self, zv_name, zv_path):
        self.zv_name = zv_name
        self.zv_path = zv_path

    def __str__(self):
        msg = "Got more than 1 result looking for %s with path: %s" % (self.zv_name, self.zv_path)
        return msg
