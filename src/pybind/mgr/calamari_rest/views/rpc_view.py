

"""
Helpers for writing django views and rest_framework ViewSets that get
their data from cthulhu with zeroRPC
"""


from calamari_rest.manager.osd_request_factory import OsdRequestFactory
from calamari_rest.manager.pool_request_factory import PoolRequestFactory

from rest_framework import viewsets, status
from rest_framework.views import APIView

from rest_framework.response import Response

from calamari_rest.types import OsdMap, SYNC_OBJECT_STR_TYPE, OSD, OSD_MAP, POOL, CLUSTER, CRUSH_RULE, ServiceId,\
    NotFound, SERVER

from rest import global_instance as rest_plugin

from rest import logger
log = logger()


class DataObject(object):
    """
    A convenience for converting dicts from the backend into
    objects, because django_rest_framework expects objects
    """
    def __init__(self, data):
        self.__dict__.update(data)


class MgrClient(object):
    cluster_monitor = None

    def __init__(self):
        self._request_factories = {
            OSD: OsdRequestFactory,
            POOL: PoolRequestFactory
        }

    def get_sync_object(self, object_type, path=None):
        return rest_plugin().get_sync_object(object_type, path)

    def get_metadata(self, svc_type, svc_id):
        return rest_plugin().get_metadata(svc_type, svc_id)

    def get(self, object_type, object_id):
        """
        Get one object from a particular cluster.
        """

        if object_type == OSD:
            return self._osd_resolve(object_id)
        elif object_type == POOL:
            return self._pool_resolve(object_id)
        else:
            raise NotImplementedError(object_type)

    def get_valid_commands(self, object_type, object_ids):
        """
        Determine what commands can be run on OSD object_ids
        """
        if object_type != OSD:
            raise NotImplementedError(object_type)

        try:
            valid_commands = self.get_request_factory(
                object_type).get_valid_commands(object_ids)
        except KeyError as e:
            raise NotFound(object_type, str(e))

        return valid_commands

    def _osd_resolve(self, osd_id):
        osdmap = self.get_sync_object(OsdMap)

        try:
            return osdmap.osds_by_id[osd_id]
        except KeyError:
            raise NotFound(OSD, osd_id)

    def _pool_resolve(self, pool_id):
        osdmap = self.get_sync_object(OsdMap)

        try:
            return osdmap.pools_by_id[pool_id]
        except KeyError:
            raise NotFound(POOL, pool_id)

    def list_requests(self, filter_args):
        state = filter_args.get('state', None)
        fsid = filter_args.get('fsid', None)
        requests = rest_plugin().requests.get_all()
        return sorted([self._dump_request(r)
                       for r in requests
                       if (state is None or r.state == state) and (fsid is None or r.fsid == fsid)],
                      lambda a, b: cmp(b['requested_at'], a['requested_at']))

    def _dump_request(self, request):
        """UserRequest to JSON-serializable form"""
        return {
            'id': request.id,
            'state': request.state,
            'error': request.error,
            'error_message': request.error_message,
            'status': request.status,
            'headline': request.headline,
            'requested_at': request.requested_at.isoformat(),
            'completed_at': request.completed_at.isoformat() if request.completed_at else None
        }

    def get_request(self, request_id):
        """
        Get a JSON representation of a UserRequest
        """
        try:
            return self._dump_request(rest_plugin().requests.get_by_id(request_id))
        except KeyError:
            raise NotFound('request', request_id)

    def cancel_request(self, request_id):
        try:
            rest_plugin().requests.cancel(request_id)
            return self.get_request(request_id)
        except KeyError:
            raise NotFound('request', request_id)

    def list(self, object_type, list_filter):
        """
        Get many objects
        """

        osd_map = self.get_sync_object(OsdMap).data
        if osd_map is None:
            return []
        if object_type == OSD:
            result = osd_map['osds']
            if 'id__in' in list_filter:
                result = [o for o in result if o['osd'] in list_filter['id__in']]
            if 'pool' in list_filter:
                try:
                    osds_in_pool = self.get_sync_object(OsdMap).osds_by_pool[list_filter['pool']]
                except KeyError:
                    raise NotFound("Pool {0} does not exist".format(list_filter['pool']))
                else:
                    result = [o for o in result if o['osd'] in osds_in_pool]

            return result
        elif object_type == POOL:
            return osd_map['pools']
        elif object_type == CRUSH_RULE:
            return osd_map['crush']['rules']
        else:
            raise NotImplementedError(object_type)

    def request_delete(self, obj_type, obj_id):
        return self._request('delete', obj_type, obj_id)

    def request_create(self, obj_type, attributes):
        return self._request('create', obj_type, attributes)

    def request_update(self, command, obj_type, obj_id, attributes):
        return self._request(command, obj_type, obj_id, attributes)

    def request_apply(self, obj_type, obj_id, command):
        return self._request(command, obj_type, obj_id)

    def update(self, object_type, object_id, attributes):
        """
        Modify an object in a cluster.
        """

        if object_type == OSD:
            # Run a resolve to throw exception if it's unknown
            self._osd_resolve(object_id)
            if 'id' not in attributes:
                attributes['id'] = object_id

            return self.request_update('update', OSD, object_id, attributes)
        elif object_type == POOL:
            self._pool_resolve(object_id)
            if 'id' not in attributes:
                attributes['id'] = object_id

            return self.request_update('update', POOL, object_id, attributes)
        elif object_type == OSD_MAP:
            return self.request_update('update_config', OSD, object_id, attributes)

        else:
            raise NotImplementedError(object_type)

    def get_request_factory(self, object_type):
        try:
            return self._request_factories[object_type]()
        except KeyError:
            raise ValueError("{0} is not one of {1}".format(object_type, self._request_factories.keys()))

    def _request(self, method, obj_type, *args, **kwargs):
        """
        Create and submit UserRequest for an apply, create, update or delete.
        """

        # nosleep during preparation phase (may touch ClusterMonitor/ServerMonitor state)
        request_factory = self.get_request_factory(obj_type)
        request = getattr(request_factory, method)(*args, **kwargs)

        if request:
            # sleeps permitted during terminal phase of submitting, because we're
            # doing I/O to the salt master to kick off
            rest_plugin().requests.submit(request)
            return {
                'request_id': request.id
            }
        else:
            return None

    def server_get(self, fqdn):
        return rest_plugin().get_server(fqdn)

    def server_list(self):
        return rest_plugin().list_servers()


from rest_framework.permissions import IsAuthenticated, BasePermission


class IsRoleAllowed(BasePermission):
    def has_permission(self, request, view):
        return True

        # TODO: reinstate read vs. read/write limitations on API keys
        has_permission = False
        # if request.user.groups.filter(name='readonly').exists():
        #     has_permission = request.method in SAFE_METHODS
        #     view.headers['Allow'] = ', '.join(SAFE_METHODS)
        # elif request.user.groups.filter(name='read/write').exists():
        #     has_permission = True
        # elif request.user.is_superuser:
        #     has_permission = True
        #
        # return has_permission

class RPCView(APIView):
    serializer_class = None
    log = log
    permission_classes = [IsAuthenticated, IsRoleAllowed]

    def get_authenticators(self):
        return rest_plugin().get_authenticators()

    def __init__(self, *args, **kwargs):
        super(RPCView, self).__init__(*args, **kwargs)
        self.client = MgrClient()

    @property
    def help(self):
        return self.__doc__

    @property
    def help_summary(self):
        return ""

    def handle_exception(self, exc):
        try:
            return super(RPCView, self).handle_exception(exc)
        except NotFound as e:
                return Response(str(e), status=status.HTTP_404_NOT_FOUND)

    def metadata(self, request):
        ret = super(RPCView, self).metadata(request)

        actions = {}
        # TODO: get the fields marked up with whether they are:
        # - [allowed|required|forbidden] during [creation|update] (6 possible kinds of field)
        # e.g. on a pool
        # id is forbidden during creation and update
        # pg_num is required during create and optional during update
        # pgp_num is optional during create or update
        # nothing is required during update
        if hasattr(self, 'update'):
            if self.serializer_class:
                actions['PATCH'] = self.serializer_class().metadata()
        if hasattr(self, 'create'):
            if self.serializer_class:
                actions['POST'] = self.serializer_class().metadata()
        ret['actions'] = actions

        return ret


class RPCViewSet(viewsets.ViewSetMixin, RPCView):
    pass
