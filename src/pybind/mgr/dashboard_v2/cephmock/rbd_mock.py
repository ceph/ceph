# -*- coding: utf-8 -*-


RBD_FEATURE_LAYERING = "RBD_FEATURE_LAYERING"
RBD_FEATURE_STRIPINGV2 = "RBD_FEATURE_STRIPINGV2"
RBD_FEATURE_EXCLUSIVE_LOCK = "RBD_FEATURE_EXCLUSIVE_LOCK"
RBD_FEATURE_OBJECT_MAP = "RBD_FEATURE_OBJECT_MAP"
RBD_FEATURE_FAST_DIFF = "RBD_FEATURE_FAST_DIFF"
RBD_FEATURE_DEEP_FLATTEN = "RBD_FEATURE_DEEP_FLATTEN"
RBD_FEATURE_JOURNALING = "RBD_FEATURE_JOURNALING"
RBD_FEATURE_DATA_POOL = "RBD_FEATURE_DATA_POOL"
RBD_FEATURE_OPERATIONS = "RBD_FEATURE_OPERATIONS"


class RBD(object):
    def __init__(self, *args, **kwargs):
        pass

    def list(self, ioctx):
        pass


class Image(object):
    def __init__(self, *args, **kwargs):
        pass

    def stat(self):
        pass

    def features(self):
        pass
