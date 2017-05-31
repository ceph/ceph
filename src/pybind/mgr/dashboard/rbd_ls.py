
import rados
import rbd
from remote_view_cache import RemoteViewCache


class RbdLs(RemoteViewCache):
    def __init__(self, module_inst, pool):
        super(RbdLs, self).__init__(module_inst)

        self.pool = pool

        self.ioctx = None
        self.rbd = None

    def _init(self):
        from mgr_module import ceph_state
        ctx_capsule = ceph_state.get_context()

        self.log.info("Constructing Rados")
        cluster = rados.Rados(context=ctx_capsule)
        cluster.connect()
        self.log.info("Constructing IOCtx")
        self.ioctx = cluster.open_ioctx("rbd")

        self.log.info("Constructing RBD")
        self.rbd = rbd.RBD()

    def _get(self):
        self.log.debug("rbd.list")
        names = self.rbd.list(self.ioctx)
        result = []
        for name in names:
            i = rbd.Image(self.ioctx, name)
            stat = i.stat()
            stat['name'] = name
            result.append(stat)
        return result
