class CRD(object):
    """Works similar to a Mock object"""
    def __getattr__(self, item):
        try:
            return getattr(super(CRD, self), item)
        except AttributeError:
            res = CRD()
            setattr(self, item, res)
            return res

    def _to_dict(self):
        return {k: (v._to_dict() if isinstance(v, CRD) else v)
                for k, v
                in self.__dict__.items() if not k.startswith('_')}


CephCluster = CRD