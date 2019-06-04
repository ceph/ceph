# -*- coding: utf-8 -*-
# pylint: disable=unused-argument
# pylint: disable=too-many-statements,too-many-branches
from __future__ import absolute_import

import math
from functools import partial
from itertools import islice
from datetime import datetime
from collections import OrderedDict

import cProfile
import cherrypy

import rbd

from . import ApiController, RESTController, Task, UpdatePermission, \
              DeletePermission, CreatePermission, ReadPermission
from .. import mgr, logger
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.rbd import RbdConfiguration, format_bitmask, format_features
from ..tools import ViewCache, str_to_bool
from ..services.exception import handle_rados_error, handle_rbd_error, \
                                 serialize_dashboard_exception

from ..helpers.ttl_cache import ttl_cache

from .resource import Resource
from .attribute import Attribute, StaticAttribute, PrivateAttribute

TTL_IMG = 30
TTL_STAT = 5
TTL_POOL = 30
TTL_SNAP = 30

def try_except(func, exc, exc_func=None):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (exc) as e:
            if exc_func:
                return exc_func(e, *args, **kwargs)
            else:
                pass
    return wrapper


class RbdImage(Resource):
    name = StaticAttribute(key=True)
    pool_name = StaticAttribute(key=True)

    _img = PrivateAttribute(lambda s: rbd.Image(s.get_ioctx(s.pool_name), s.name), cache_ttl=TTL_IMG)

    id = Attribute(lambda s: s._img.id())

    _stat = PrivateAttribute(lambda s: s._img.stat(), cache_ttl=TTL_STAT)
    size = Attribute(lambda s: s._stat['size'])
    num_objs = Attribute(lambda s: s._stat['num_objs'])
    obj_size = Attribute(lambda s: s._stat['obj_size'])

    features_name = Attribute(lambda s: format_bitmask(s._img.features()))
    timestamp = Attribute(lambda s: "{}Z".format(s._img.create_timestamp().isoformat()))

    stripe_count =  Attribute(lambda s: s._img.stripe_count())
    stripe_unit =  Attribute(lambda s: s._img.stripe_unit())
    data_pool_name = Attribute(lambda s: mgr.rados.pool_reverse_lookup(s._img.data_pool_id()))

    _parent_info = PrivateAttribute(try_except(lambda s: s._img.parent_info(), rbd.ImageNotFound),
                                    cache_ttl=TTL_IMG)
    parent_pool_name = Attribute(try_except(lambda s: s._parent_info[0], TypeError))
    parent_image_name = Attribute(try_except(lambda s: s._parent_info[1], TypeError))
    parent_snap_name = Attribute(try_except(lambda s: s._parent_info[2], TypeError))

    snapshots = Attribute(lambda s: s.get_snapshots(), cache_ttl=TTL_SNAP)

    total_disk_usage = Attribute(lambda s: s.get_disk_usage()[0])
    disk_usage = Attribute(lambda s: s.get_disk_usage()[1])


    @ttl_cache(ttl=TTL_IMG)
    def get_disk_usage(self):
        img_flags = self._img.flags()
        if 'fast-diff' in self.features_name and not rbd.RBD_FLAG_FAST_DIFF_INVALID & img_flags:
            snaps = [(s['id'], s['size'], s['name']) for s in self.snapshots]
            snaps.sort(key=lambda s: s[0])
            snaps += [(snaps[-1][0]+1 if snaps else 0, self.size, None)]
            total_prov_bytes, snaps_prov_bytes = Rbd._rbd_disk_usage(self._img, snaps, True)
            total_disk_usage = total_prov_bytes
            for snap, prov_bytes in snaps_prov_bytes.items():
                if snap is None:
                    disk_usage = prov_bytes
                    continue
                for ss in self.snapshots:
                    if ss['name'] == snap:
                        disk_usage = prov_bytes
                        break
        else:
            total_disk_usage = None
            disk_usage = None
        return (total_disk_usage, disk_usage)


    def get_snapshot_children(self, snap):
        self._img.set_snap(snap['name'])
        return [dict(
            pool_name=pool_name,
            image_name=image_name,
        ) for pool_name, image_name in self._img.list_children()]

    def get_snapshots(self):
        return [dict(
            id=snap['id'],
            size=snap['size'],
            name=snap['name'],
            namespace=snap['namespace'],
            timestamp="{}Z".format(
                self._img.get_snap_timestamp(snap['id']).isoformat()),
            is_protected=self._img.is_protected_snap(snap['name']),
            used_bytes=None,
            children=self.get_snapshot_children(snap),
        ) for snap in self._img.list_snaps()]


    @classmethod
    @ttl_cache(ttl=TTL_POOL)
    def get_ioctx(cls, pool_name):
        return mgr.rados.open_ioctx(pool_name)


# pylint: disable=not-callable
def RbdTask(name, metadata, wait_for):
    def composed_decorator(func):
        func = handle_rados_error('pool')(func)
        func = handle_rbd_error()(func)
        return Task("rbd/{}".format(name), metadata, wait_for,
                    partial(serialize_dashboard_exception, include_http_status=True))(func)
    return composed_decorator


def _rbd_call(pool_name, func, *args, **kwargs):
    with mgr.rados.open_ioctx(pool_name) as ioctx:
        func(ioctx, *args, **kwargs)


def _rbd_image_call(pool_name, image_name, func, *args, **kwargs):
    def _ioctx_func(ioctx, image_name, func, *args, **kwargs):
        with rbd.Image(ioctx, image_name) as img:
            func(ioctx, img, *args, **kwargs)

    return _rbd_call(pool_name, _ioctx_func, image_name, func, *args, **kwargs)


def _sort_features(features, enable=True):
    """
    Sorts image features according to feature dependencies:

    object-map depends on exclusive-lock
    journaling depends on exclusive-lock
    fast-diff depends on object-map
    """
    ORDER = ['exclusive-lock', 'journaling', 'object-map', 'fast-diff']

    def key_func(feat):
        try:
            return ORDER.index(feat)
        except ValueError:
            return id(feat)

    features.sort(key=key_func, reverse=not enable)


@ApiController('/block/image', Scope.RBD_IMAGE)
class Rbd(RESTController):

    RESOURCE_ID = "pool_name/image_name"

    # set of image features that can be enable on existing images
    ALLOW_ENABLE_FEATURES = {"exclusive-lock", "object-map", "fast-diff", "journaling"}

    # set of image features that can be disabled on existing images
    ALLOW_DISABLE_FEATURES = {"exclusive-lock", "object-map", "fast-diff", "deep-flatten",
                              "journaling"}

    def __init__(self):
        super(Rbd, self).__init__()
        self.rbd_inst = rbd.RBD()

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

    @classmethod
    def _rbd_image(cls, ioctx, pool_name, image_name):
        with rbd.Image(ioctx, image_name) as img:
            stat = img.stat()
            stat['name'] = image_name
            stat['id'] = img.id()
            stat['pool_name'] = pool_name
            features = img.features()
            stat['features'] = features
            stat['features_name'] = format_bitmask(features)

            # the following keys are deprecated
            del stat['parent_pool']
            del stat['parent_name']

            stat['timestamp'] = "{}Z".format(img.create_timestamp()
                                             .isoformat())

            stat['stripe_count'] = img.stripe_count()
            stat['stripe_unit'] = img.stripe_unit()

            data_pool_name = CephService.get_pool_name_from_id(
                img.data_pool_id())
            if data_pool_name == pool_name:
                data_pool_name = None
            stat['data_pool'] = data_pool_name

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
            img_flags = img.flags()
            if 'fast-diff' in stat['features_name'] and \
                    not rbd.RBD_FLAG_FAST_DIFF_INVALID & img_flags:
                snaps = [(s['id'], s['size'], s['name'])
                         for s in stat['snapshots']]
                snaps.sort(key=lambda s: s[0])
                snaps += [(snaps[-1][0]+1 if snaps else 0, stat['size'], None)]
                total_prov_bytes, snaps_prov_bytes = cls._rbd_disk_usage(
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

            stat['configuration'] = RbdConfiguration(pool_ioctx=ioctx, image_name=image_name).list()

            return stat

    def _rbd_pool_list(self, pool_name):
        with mgr.rados.open_ioctx(pool_name) as ioctx:
            return [self._rbd_image_basic_2(ioctx, pool_name, name) for name in
                    self.rbd_inst.list(ioctx)]

    def get_images_from_pool(self, pool_name):
        with mgr.rados.open_ioctx(pool_name) as ioctx:
            return self.rbd_inst.list(ioctx)

    def get_images(self, pools):
        return [(pool, img) for pool in pools for img in
                self.get_images_from_pool(pool)]

    @staticmethod
    @ttl_cache(ttl=TTL_POOL, maxsize=1)
    def get_pools():
        return [p['pool_name'] for p in CephService.get_pool_list('rbd')]

    @handle_rbd_error()
    @handle_rados_error('pool')
    def list(self, pool_name=None,
             limit="10", offset="0",
             fields=None,
             sort=None,
             filter=None):
        pr = cProfile.Profile()
        pr.enable()

        limit = int(limit)
        offset = int(offset)
        if fields:
            fields = fields.split(",")

        if pool_name:
            pools = [pool_name]
        else:
            pools = self.get_pools()

        images = self.get_images(pools)

        if sort:
            images = sorted(
                [(pool, img, RbdImage(name=img, pool_name=pool)[sort]) for (pool, img) in images],
                key=lambda x: x[2]
            )

        ret = list(islice(
            (RbdImage(name=img[1], pool_name=img[0]).to_dict(fields) for img in images),
            offset* + limit,
            (offset + 1)*limit
        ))

        pr.disable()
        pr.dump_stats('/ceph/build/profile.stats')

        return ret

    @handle_rbd_error()
    @handle_rados_error('pool')
    def get(self, pool_name, image_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        try:
            return self._rbd_image(ioctx, pool_name, image_name)
        except rbd.ImageNotFound:
            raise cherrypy.HTTPError(404)

    @RbdTask('create',
             {'pool_name': '{pool_name}', 'image_name': '{name}'}, 2.0)
    def create(self, name, pool_name, size, obj_size=None, features=None,
               stripe_unit=None, stripe_count=None, data_pool=None, configuration=None):

        size = int(size)

        def _create(ioctx):
            rbd_inst = rbd.RBD()

            # Set order
            l_order = None
            if obj_size and obj_size > 0:
                l_order = int(round(math.log(float(obj_size), 2)))

            # Set features
            feature_bitmask = format_features(features)

            rbd_inst.create(ioctx, name, size, order=l_order, old_format=False,
                            features=feature_bitmask, stripe_unit=stripe_unit,
                            stripe_count=stripe_count, data_pool=data_pool)
            RbdConfiguration(pool_ioctx=ioctx, image_name=name).set_configuration(configuration)

        _rbd_call(pool_name, _create)

    @RbdTask('delete', ['{pool_name}', '{image_name}'], 2.0)
    def delete(self, pool_name, image_name):
        rbd_inst = rbd.RBD()
        return _rbd_call(pool_name, rbd_inst.remove, image_name)

    @RbdTask('edit', ['{pool_name}', '{image_name}', '{name}'], 4.0)
    def set(self, pool_name, image_name, name=None, size=None, features=None, configuration=None):
        def _edit(ioctx, image):
            rbd_inst = rbd.RBD()
            # check rename image
            if name and name != image_name:
                rbd_inst.rename(ioctx, image_name, name)

            # check resize
            if size and size != image.size():
                image.resize(size)

            # check enable/disable features
            if features is not None:
                curr_features = format_bitmask(image.features())
                # check disabled features
                _sort_features(curr_features, enable=False)
                for feature in curr_features:
                    if feature not in features and feature in self.ALLOW_DISABLE_FEATURES:
                        f_bitmask = format_features([feature])
                        image.update_features(f_bitmask, False)
                # check enabled features
                _sort_features(features)
                for feature in features:
                    if feature not in curr_features and feature in self.ALLOW_ENABLE_FEATURES:
                        f_bitmask = format_features([feature])
                        image.update_features(f_bitmask, True)

            RbdConfiguration(pool_ioctx=ioctx, image_name=image_name).set_configuration(
                configuration)

        return _rbd_image_call(pool_name, image_name, _edit)

    @RbdTask('copy',
             {'src_pool_name': '{pool_name}',
              'src_image_name': '{image_name}',
              'dest_pool_name': '{dest_pool_name}',
              'dest_image_name': '{dest_image_name}'}, 2.0)
    @RESTController.Resource('POST')
    def copy(self, pool_name, image_name, dest_pool_name, dest_image_name,
             snapshot_name=None, obj_size=None, features=None, stripe_unit=None,
             stripe_count=None, data_pool=None, configuration=None):

        def _src_copy(s_ioctx, s_img):
            def _copy(d_ioctx):
                # Set order
                l_order = None
                if obj_size and obj_size > 0:
                    l_order = int(round(math.log(float(obj_size), 2)))

                # Set features
                feature_bitmask = format_features(features)

                if snapshot_name:
                    s_img.set_snap(snapshot_name)

                s_img.copy(d_ioctx, dest_image_name, feature_bitmask, l_order,
                           stripe_unit, stripe_count, data_pool)
                RbdConfiguration(pool_ioctx=d_ioctx, image_name=dest_image_name).set_configuration(
                    configuration)

            return _rbd_call(dest_pool_name, _copy)

        return _rbd_image_call(pool_name, image_name, _src_copy)

    @RbdTask('flatten', ['{pool_name}', '{image_name}'], 2.0)
    @RESTController.Resource('POST')
    @UpdatePermission
    def flatten(self, pool_name, image_name):

        def _flatten(ioctx, image):
            image.flatten()

        return _rbd_image_call(pool_name, image_name, _flatten)

    @RESTController.Collection('GET')
    def default_features(self):
        rbd_default_features = mgr.get('config')['rbd_default_features']
        return format_bitmask(int(rbd_default_features))

    @RbdTask('trash/move', ['{pool_name}', '{image_name}'], 2.0)
    @RESTController.Resource('POST')
    def move_trash(self, pool_name, image_name, delay=0):
        """Move an image to the trash.
        Images, even ones actively in-use by clones,
        can be moved to the trash and deleted at a later time.
        """
        rbd_inst = rbd.RBD()
        return _rbd_call(pool_name, rbd_inst.trash_move, image_name, delay)

    @RESTController.Resource()
    @ReadPermission
    def configuration(self, pool_name, image_name):
        return RbdConfiguration(pool_name, image_name).list()


@ApiController('/block/image/{pool_name}/{image_name}/snap', Scope.RBD_IMAGE)
class RbdSnapshot(RESTController):

    RESOURCE_ID = "snapshot_name"

    @RbdTask('snap/create',
             ['{pool_name}', '{image_name}', '{snapshot_name}'], 2.0)
    def create(self, pool_name, image_name, snapshot_name):
        def _create_snapshot(ioctx, img, snapshot_name):
            img.create_snap(snapshot_name)

        return _rbd_image_call(pool_name, image_name, _create_snapshot,
                               snapshot_name)

    @RbdTask('snap/delete',
             ['{pool_name}', '{image_name}', '{snapshot_name}'], 2.0)
    def delete(self, pool_name, image_name, snapshot_name):
        def _remove_snapshot(ioctx, img, snapshot_name):
            img.remove_snap(snapshot_name)

        return _rbd_image_call(pool_name, image_name, _remove_snapshot,
                               snapshot_name)

    @RbdTask('snap/edit',
             ['{pool_name}', '{image_name}', '{snapshot_name}'], 4.0)
    def set(self, pool_name, image_name, snapshot_name, new_snap_name=None,
            is_protected=None):
        def _edit(ioctx, img, snapshot_name):
            if new_snap_name and new_snap_name != snapshot_name:
                img.rename_snap(snapshot_name, new_snap_name)
                snapshot_name = new_snap_name
            if is_protected is not None and \
                    is_protected != img.is_protected_snap(snapshot_name):
                if is_protected:
                    img.protect_snap(snapshot_name)
                else:
                    img.unprotect_snap(snapshot_name)

        return _rbd_image_call(pool_name, image_name, _edit, snapshot_name)

    @RbdTask('snap/rollback',
             ['{pool_name}', '{image_name}', '{snapshot_name}'], 5.0)
    @RESTController.Resource('POST')
    @UpdatePermission
    def rollback(self, pool_name, image_name, snapshot_name):
        def _rollback(ioctx, img, snapshot_name):
            img.rollback_to_snap(snapshot_name)
        return _rbd_image_call(pool_name, image_name, _rollback, snapshot_name)

    @RbdTask('clone',
             {'parent_pool_name': '{pool_name}',
              'parent_image_name': '{image_name}',
              'parent_snap_name': '{snapshot_name}',
              'child_pool_name': '{child_pool_name}',
              'child_image_name': '{child_image_name}'}, 2.0)
    @RESTController.Resource('POST')
    def clone(self, pool_name, image_name, snapshot_name, child_pool_name,
              child_image_name, obj_size=None, features=None, stripe_unit=None, stripe_count=None,
              data_pool=None, configuration=None):
        """
        Clones a snapshot to an image
        """

        def _parent_clone(p_ioctx):
            def _clone(ioctx):
                # Set order
                l_order = None
                if obj_size and obj_size > 0:
                    l_order = int(round(math.log(float(obj_size), 2)))

                # Set features
                feature_bitmask = format_features(features)

                rbd_inst = rbd.RBD()
                rbd_inst.clone(p_ioctx, image_name, snapshot_name, ioctx,
                               child_image_name, feature_bitmask, l_order,
                               stripe_unit, stripe_count, data_pool)

                RbdConfiguration(pool_ioctx=ioctx, image_name=child_image_name).set_configuration(
                    configuration)

            return _rbd_call(child_pool_name, _clone)

        _rbd_call(pool_name, _parent_clone)


@ApiController('/block/image/trash', Scope.RBD_IMAGE)
class RbdTrash(RESTController):
    RESOURCE_ID = "pool_name/image_id"
    rbd_inst = rbd.RBD()

    @ViewCache()
    def _trash_pool_list(self, pool_name):
        with mgr.rados.open_ioctx(pool_name) as ioctx:
            images = self.rbd_inst.trash_list(ioctx)
            result = []
            for trash in images:
                trash['pool_name'] = pool_name
                trash['deletion_time'] = "{}Z".format(trash['deletion_time'].isoformat())
                trash['deferment_end_time'] = "{}Z".format(trash['deferment_end_time'].isoformat())
                result.append(trash)
            return result

    def _trash_list(self, pool_name=None):
        if pool_name:
            pools = [pool_name]
        else:
            pools = [p['pool_name'] for p in CephService.get_pool_list('rbd')]

        result = []
        for pool in pools:
            # pylint: disable=unbalanced-tuple-unpacking
            status, value = self._trash_pool_list(pool)
            result.append({'status': status, 'value': value, 'pool_name': pool})
        return result

    @handle_rbd_error()
    @handle_rados_error('pool')
    def list(self, pool_name=None):
        """List all entries from trash."""
        return self._trash_list(pool_name)

    @handle_rbd_error()
    @handle_rados_error('pool')
    @RbdTask('trash/purge', ['{pool_name}'], 2.0)
    @RESTController.Collection('POST', query_params=['pool_name'])
    @DeletePermission
    def purge(self, pool_name=None):
        """Remove all expired images from trash."""
        now = "{}Z".format(datetime.now().isoformat())
        pools = self._trash_list(pool_name)

        for pool in pools:
            for image in pool['value']:
                if image['deferment_end_time'] < now:
                    _rbd_call(pool['pool_name'], self.rbd_inst.trash_remove, image['id'], 0)

    @RbdTask('trash/restore', ['{pool_name}', '{image_id}', '{new_image_name}'], 2.0)
    @RESTController.Resource('POST')
    @CreatePermission
    def restore(self, pool_name, image_id, new_image_name):
        """Restore an image from trash."""
        return _rbd_call(pool_name, self.rbd_inst.trash_restore, image_id, new_image_name)

    @RbdTask('trash/remove', ['{pool_name}', '{image_id}', '{image_name}'], 2.0)
    def delete(self, pool_name, image_id, image_name, force=False):
        """Delete an image from trash.
        If image deferment time has not expired you can not removed it unless use force.
        But an actively in-use by clones or has snapshots can not be removed.
        """
        return _rbd_call(pool_name, self.rbd_inst.trash_remove, image_id, int(str_to_bool(force)))
