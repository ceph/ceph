
import rbd
import rados
from types import OsdMap
from remote_view_cache import RemoteViewCache

class RbdPoolLs(RemoteViewCache):
    def _get(self):
        from mgr_module import ceph_state
        ctx_capsule = ceph_state.get_context()

        osd_map = self._module.get_sync_object(OsdMap).data
        osd_pools = [pool['pool_name'] for pool in osd_map['pools']]

        rbd_pools = []
        for pool in osd_pools:
            self.log.debug("Constructing IOCtx " + pool)
            try:
                ioctx = self._module.rados.open_ioctx(pool)
                ioctx.stat("rbd_directory")
                rbd_pools.append(pool)
            except (rados.PermissionError, rados.ObjectNotFound):
                self.log.debug("No RBD directory in " + pool)
            except:
                self.log.exception("Failed to open pool " + pool)

        return rbd_pools

class RbdLs(RemoteViewCache):
    def __init__(self, module_inst, pool):
        super(RbdLs, self).__init__(module_inst)

        self.pool = pool

        self.ioctx = None
        self.rbd = None

    def _init(self):
        self.log.debug("Constructing IOCtx")
        self.ioctx = self._module.rados.open_ioctx(self.pool)

        self.log.debug("Constructing RBD")
        self.rbd = rbd.RBD()

    def _get(self):
        self.log.debug("rbd.list")
        names = self.rbd.list(self.ioctx)
        result = []
        for name in names:
            i = rbd.Image(self.ioctx, name)
            stat = i.stat()
            stat['name'] = name

            try:
                parent_info = i.parent_info()
                parent = "{}@{}".format(parent_info[0], parent_info[1])
                if parent_info[0] != self.pool:
                    parent = "{}/{}".format(parent_info[0], parent)
                stat['parent'] = parent
            except rbd.ImageNotFound:
                pass
            result.append(stat)
        return result
