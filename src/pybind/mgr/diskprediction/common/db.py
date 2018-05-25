"""
Ceph database API

"""
from __future__ import absolute_import

import json
import rbd
from mgr_module import CommandResult


RBD_FEATURES_NAME_MAPPING = {
    rbd.RBD_FEATURE_LAYERING: "layering",
    rbd.RBD_FEATURE_STRIPINGV2: "striping",
    rbd.RBD_FEATURE_EXCLUSIVE_LOCK: "exclusive-lock",
    rbd.RBD_FEATURE_OBJECT_MAP: "object-map",
    rbd.RBD_FEATURE_FAST_DIFF: "fast-diff",
    rbd.RBD_FEATURE_DEEP_FLATTEN: "deep-flatten",
    rbd.RBD_FEATURE_JOURNALING: "journaling",
    rbd.RBD_FEATURE_DATA_POOL: "data-pool",
    rbd.RBD_FEATURE_OPERATIONS: "operations",
}


class DB_API(object):
    def __init__(self, module_obj):
        self.module = module_obj

    def _format_bitmask(self, features):
        """
        Formats the bitmask:
        >>> _format_bitmask(45)
        ['deep-flatten', 'exclusive-lock', 'layering', 'object-map']
        """
        names = [val for key, val in RBD_FEATURES_NAME_MAPPING.items()
                 if key & features == key]
        return sorted(names)

    @classmethod
    def _rbd_disk_usage(cls, image, snaps, whole_object=True):
        class DUCallback(object):
            def __init__(self):
                self.used_size = 0

            def __call__(self, offset, length, exists):
                if exists:
                    self.used_size += length
        snap_map = {}
        prev_snap = None
        total_used_size = 0
        for _, size, name in snaps:
            image.set_snap(name)
            du_callb = DUCallback()
            image.diff_iterate(0, size, prev_snap, du_callb,
                               whole_object=whole_object)
            snap_map[name] = du_callb.used_size
            total_used_size += du_callb.used_size
            prev_snap = name
        return total_used_size, snap_map

    def _rbd_image(self, ioctx, pool_name, image_name):
        with rbd.Image(ioctx, image_name) as img:
            stat = img.stat()
            stat['name'] = image_name
            stat['id'] = img.id()
            stat['pool_name'] = pool_name
            features = img.features()
            stat['features'] = features
            stat['features_name'] = self._format_bitmask(features)

            # the following keys are deprecated
            del stat['parent_pool']
            del stat['parent_name']
            stat['timestamp'] = "{}Z".format(img.create_timestamp()
                                             .isoformat())
            stat['stripe_count'] = img.stripe_count()
            stat['stripe_unit'] = img.stripe_unit()
            stat['data_pool'] = None
            try:
                parent_info = img.parent_info()
                stat['parent'] = {
                    'pool_name': parent_info[0],
                    'image_name': parent_info[1],
                    'snap_name': parent_info[2]
                }
            except rbd.ImageNotFound:
                # no parent image
                stat['parent'] = None
            # snapshots
            stat['snapshots'] = []
            for snap in img.list_snaps():
                snap['timestamp'] = "{}Z".format(
                    img.get_snap_timestamp(snap['id']).isoformat())
                snap['is_protected'] = img.is_protected_snap(snap['name'])
                snap['used_bytes'] = None
                snap['children'] = []
                img.set_snap(snap['name'])
                for child_pool_name, child_image_name in img.list_children():
                    snap['children'].append({
                        'pool_name': child_pool_name,
                        'image_name': child_image_name
                    })
                stat['snapshots'].append(snap)
            # disk usage
            if 'fast-diff' in stat['features_name']:
                snaps = [(s['id'], s['size'], s['name'])
                         for s in stat['snapshots']]
                snaps.sort(key=lambda s: s[0])
                snaps += [(snaps[-1][0]+1 if snaps else 0, stat['size'], None)]
                total_prov_bytes, snaps_prov_bytes = self._rbd_disk_usage(
                    img, snaps, True)
                stat['total_disk_usage'] = total_prov_bytes
                for snap, prov_bytes in snaps_prov_bytes.items():
                    if snap is None:
                        stat['disk_usage'] = prov_bytes
                        continue
                    for ss in stat['snapshots']:
                        if ss['name'] == snap:
                            ss['disk_usage'] = prov_bytes
                            break
            else:
                stat['total_disk_usage'] = None
                stat['disk_usage'] = None
            return stat

    def get_rbd_list(self, pool_name=None):
        if pool_name:
            pools = [pool_name]
        else:
            pools = []
            for data in self.get_osd_pools():
                pools.append(data['pool_name'])
        result = []
        for pool in pools:
            rbd_inst = rbd.RBD()
            with self.module.rados.open_ioctx(str(pool)) as ioctx:
                names = rbd_inst.list(ioctx)
                for name in names:
                    try:
                        stat = self._rbd_image(ioctx, pool_name, name)
                    except rbd.ImageNotFound:
                        continue
                    result.append(stat)
        return result

    def get_pg_summary(self):
        return self.module.get('pg_summary')

    def get_df_stats(self):
        return self.module.get('df').get('stats', {})

    def get_object_pg_info(self, pool_name, object_name):
        result = CommandResult("")
        data_jaon = {}
        self.module.send_command(
            result, 'mon', '', json.dumps({
                'prefix': 'osd map',
                'format': 'json',
                'pool': pool_name,
                'object': object_name,
            }), '')
        ret, outb, outs = result.wait()
        try:
            data_jaon = json.loads(outb)
            self.module.log.debug("outs: %s" % data_jaon)
        except Exception as e:
            self.module.log.error("error: %s" % str(e))
        return data_jaon

    def get_rbd_info(self, pool_name, image_name):
        with self.module.rados.open_ioctx(pool_name) as ioctx:
            try:
                stat = self._rbd_image(ioctx, pool_name, image_name)
                if stat.get('id'):
                    objects = self.get_pool_objects(pool_name, stat.get('id'))
                    if objects:
                        stat['objects'] = objects
                        stat['pgs'] = list()
                    for obj_name in objects:
                        pgs_data = self.get_object_pg_info(pool_name, obj_name)
                        stat['pgs'].extend([pgs_data])
            except rbd.ImageNotFound:
                stat = {}
        return stat

    def get_pool_objects(self, pool_name, image_id=None):
        # list_objects
        objects = []
        with self.module.rados.open_ioctx(pool_name) as ioctx:
            object_iterator = ioctx.list_objects()
            while True:
                try:
                    rados_object = object_iterator.next()
                    if image_id is None:
                        objects.append(str(rados_object.key))
                    else:
                        v = str(rados_object.key).split('.')
                        if len(v) >= 2 and v[1] == image_id:
                            objects.append(str(rados_object.key))
                except StopIteration:
                    break
        return objects

    def get_global_total_size(self):
        total_bytes = \
            self.module.get('df').get('stats', {}).get('total_bytes')
        total_size = float(total_bytes) / (1024 * 1024 * 1024)
        return round(total_size)

    def get_global_avail_size(self):
        total_avail_bytes = \
            self.module.get('df').get('stats', {}).get('total_avail_bytes')
        total_avail_size = float(total_avail_bytes) / (1024 * 1024 * 1024)
        return round(total_avail_size, 2)

    def get_global_raw_used_size(self):
        total_used_bytes = \
            self.module.get('df').get('stats', {}).get('total_used_bytes')
        total_raw_used_size = float(total_used_bytes) / (1024 * 1024 * 1024)
        return round(total_raw_used_size, 2)

    def get_global_raw_used_percent(self):
        total_bytes = \
            self.module.get('df').get('stats').get('total_bytes')
        total_used_bytes = \
            self.module.get('df').get('stats').get('total_used_bytes')
        if total_bytes and total_used_bytes:
            total_used_percent = \
                float(total_used_bytes) / float(total_bytes) * 100
        else:
            total_used_percent = 0.0
        return round(total_used_percent, 2)

    def get_osd_data(self):
        return self.module.get('config').get('osd_data', '')

    def get_osd_journal(self):
        return self.module.get('config').get('osd_journal', '')

    def get_osd_metadata(self, osd_id=None):
        if osd_id is not None:
            return self.module.get('osd_metadata')[str(osd_id)]
        return self.module.get('osd_metadata')

    def get_osd_epoch(self):
        return self.module.get('osd_map').get('epoch', 0)

    def get_osds(self):
        return self.module.get('osd_map').get('osds', [])

    def get_max_osd(self):
        return self.module.get('osd_map').get('max_osd', '')

    def get_osd_pools(self):
        return self.module.get('osd_map').get('pools', [])

    def get_pool_bytes_used(self, pool_id):
        bytes_used = None
        pools = self.module.get('df').get('pools', [])
        for pool in pools:
            if pool_id == pool['id']:
                bytes_used = pool['stats']['bytes_used']
        return bytes_used

    def get_cluster_id(self):
        return self.module.get('mon_map').get('fsid')

    def get_health_status(self):
        health = json.loads(self.module.get('health')['json'])
        return health.get('status')

    def get_mons(self):
        return self.module.get('mon_map').get('mons', [])

    def get_mon_status(self):
        mon_status = json.loads(self.module.get('mon_status')['json'])
        return mon_status

    def get_osd_smart(self, osd_id):
        result = CommandResult("")
        data_jaon = {}
        if not str(osd_id).isdigit():
            self.module.log("invalid osd id %s" % osd_id)
            return data_jaon
        self.module.send_command(result, 'osd', "%s" % osd_id, json.dumps({
            'prefix': 'smart',
            'format': 'json',
        }), '')
        ret, outb, outs = result.wait()
        try:
            data_jaon = json.loads(outb)
            self.module.log.debug("outs: %s" % data_jaon)
        except Exception as e:
            self.module.log.error("error: %s" % str(e))
        return data_jaon

    def get_file_systems(self):
        return self.module.get('fs_map').get('filesystems', [])

    def get_pg_stats(self):
        return self.module.get('pg_dump').get('pg_stats', [])

    def get_all_perf_counters(self):
        return self.module.get_all_perf_counters()

    def get(self, data_name):
        return self.module.get(data_name)

    def get_all_information(self):
        result = dict()
        result['osd_map'] = self.module.get('osd_map')
        result['osd_map_tree'] = self.module.get('osd_map_tree')
        result['osd_map_crush'] = self.module.get('osd_map_crush')
        result['config'] = self.module.get('config')
        result['mon_map'] = self.module.get('mon_map')
        result['fs_map'] = self.module.get('fs_map')
        result['osd_metadata'] = self.module.get('osd_metadata')
        result['pg_summary'] = self.module.get('pg_summary')
        result['pg_dump'] = self.module.get('pg_dump')
        result['io_rate'] = self.module.get('io_rate')
        result['df'] = self.module.get('df')
        result['osd_stats'] = self.module.get('osd_stats')
        result['health'] = self.get_health_status()
        result['mon_status'] = self.get_mon_status()
        return result
