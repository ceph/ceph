import argparse


class LVPath(object):
    """
    A simple validator to ensure that a logical volume is specified like::

        <vg name>/<lv name>

    Because for LVM it is better to be specific on what group does an lv
    belongs to.
    """

    def __call__(self, string):
        error = None
        try:
            vg, lv = string.split('/')
        except ValueError:
            error = "Logical volume must be specified as 'volume_group/logical_volume' but got: %s" % string
            raise argparse.ArgumentError(None, error)

        if not vg:
            error = "Didn't specify a volume group like 'volume_group/logical_volume', got: %s" % string
        if not lv:
            error = "Didn't specify a logical volume like 'volume_group/logical_volume', got: %s" % string

        if error:
            raise argparse.ArgumentError(None, error)
        return string
