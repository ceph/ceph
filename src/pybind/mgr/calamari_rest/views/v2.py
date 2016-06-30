from collections import defaultdict
import json
import logging
import shlex

from django.http import Http404
from rest_framework.exceptions import ParseError, APIException, PermissionDenied
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from django.contrib.auth.decorators import login_required


from calamari_rest.serializers.v2 import PoolSerializer, CrushRuleSetSerializer, CrushRuleSerializer, \
    ServerSerializer, SaltKeySerializer, RequestSerializer, \
    ClusterSerializer, EventSerializer, LogTailSerializer, OsdSerializer, ConfigSettingSerializer, MonSerializer, OsdConfigSerializer, \
    CliSerializer
#from calamari_rest.views.database_view_set import DatabaseViewSet
from calamari_rest.views.exceptions import ServiceUnavailable
#from calamari_rest.views.paginated_mixin import PaginatedMixin
#from calamari_rest.views.remote_view_set import RemoteViewSet
from calamari_rest.views.rpc_view import RPCViewSet, DataObject
from calamari_rest.types import CRUSH_RULE, POOL, OSD, USER_REQUEST_COMPLETE, USER_REQUEST_SUBMITTED, \
    OSD_IMPLEMENTED_COMMANDS, MON, OSD_MAP, SYNC_OBJECT_TYPES, ServiceId, severity_from_str, SEVERITIES, \
    OsdMap, Config, MonMap, MonStatus


class Event(object):
    pass

from rest import logger
log = logger()


#class RequestViewSet(RPCViewSet, PaginatedMixin):
class RequestViewSet(RPCViewSet):
    """
Calamari server requests, tracking long-running operations on the Calamari server.  Some
API resources return a ``202 ACCEPTED`` response with a request ID, which you can use with
this resource to learn about progress and completion of an operation.  This resource is
paginated.

May optionally filter by state by passing a ``?state=<state>`` GET parameter, where
state is one of 'complete', 'submitted'.

The returned records are ordered by the 'requested_at' attribute, in descending order (i.e.
the first page of results contains the most recent requests).

To cancel a request while it is running, send an empty POST to ``request/<request id>/cancel``.
    """
    serializer_class = RequestSerializer

    def cancel(self, request, request_id):
        user_request = DataObject(self.client.cancel_request(request_id))
        return Response(self.serializer_class(user_request).data)

    def retrieve(self, request, **kwargs):
        request_id = kwargs['request_id']
        user_request = DataObject(self.client.get_request(request_id))
        return Response(self.serializer_class(user_request).data)

    def list(self, request, **kwargs):
        fsid = kwargs.get('fsid', None)
        filter_state = request.GET.get('state', None)
        valid_states = [USER_REQUEST_COMPLETE, USER_REQUEST_SUBMITTED]
        if filter_state is not None and filter_state not in valid_states:
            raise ParseError("State must be one of %s" % ", ".join(valid_states))

        requests = self.client.list_requests({'state': filter_state, 'fsid': fsid})
        if False:
            # FIXME reinstate pagination, broke in DRF 2.x -> 3.x
            return Response(self._paginate(request, requests))
        else:
            return Response(requests)


class CrushRuleViewSet(RPCViewSet):
    """
A CRUSH ruleset is a collection of CRUSH rules which are applied
together to a pool.
    """
    serializer_class = CrushRuleSerializer

    def list(self, request):
        rules = self.client.list(CRUSH_RULE, {})
        osds_by_rule_id = self.client.get_sync_object(OsdMap, ['osds_by_rule_id'])
        for rule in rules:
            rule['osd_count'] = len(osds_by_rule_id[rule['rule_id']])
        return Response(CrushRuleSerializer([DataObject(r) for r in rules], many=True).data)


class CrushRuleSetViewSet(RPCViewSet):
    """
A CRUSH rule is used by Ceph to decide where to locate placement groups on OSDs.
    """
    serializer_class = CrushRuleSetSerializer

    def list(self, request):
        rules = self.client.list(CRUSH_RULE, {})
        osds_by_rule_id = self.client.get_sync_object(OsdMap, ['osds_by_rule_id'])
        rulesets_data = defaultdict(list)
        for rule in rules:
            rule['osd_count'] = len(osds_by_rule_id[rule['rule_id']])
            rulesets_data[rule['ruleset']].append(rule)

        rulesets = [DataObject({
            'id': rd_id,
            'rules': [DataObject(r) for r in rd_rules]
        }) for (rd_id, rd_rules) in rulesets_data.items()]

        return Response(CrushRuleSetSerializer(rulesets, many=True).data)


class SaltKeyViewSet(RPCViewSet):
    """
Ceph servers authentication with the Calamari using a key pair.  Before
Calamari accepts messages from a server, the server's key must be accepted.
    """
    serializer_class = SaltKeySerializer

    def list(self, request):
        return Response(self.serializer_class(self.client.minion_status(None), many=True).data)

    def partial_update(self, request, minion_id):
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid(request.method):
            self._partial_update(minion_id, serializer.get_data())
            return Response(status=status.HTTP_204_NO_CONTENT)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def _partial_update(self, minion_id, data):
        valid_status = ['accepted', 'rejected']
        if 'status' not in data:
            raise ParseError({'status': "This field is mandatory"})
        elif data['status'] not in valid_status:
            raise ParseError({'status': "Must be one of %s" % ",".join(valid_status)})
        else:
            key = self.client.minion_get(minion_id)
            transition = [key['status'], data['status']]
            if transition == ['pre', 'accepted']:
                self.client.minion_accept(minion_id)
            elif transition == ['pre', 'rejected']:
                self.client.minion_reject(minion_id)
            else:
                raise ParseError({'status': ["Transition {0}->{1} is invalid".format(
                    transition[0], transition[1]
                )]})

    def _validate_list(self, request):
        keys = request.DATA
        if not isinstance(keys, list):
            raise ParseError("Bulk PATCH must send a list")
        for key in keys:
            if 'id' not in key:
                raise ParseError("Items in bulk PATCH must have 'id' attribute")

    def list_partial_update(self, request):
        self._validate_list(request)

        keys = request.DATA
        log.debug("KEYS %s" % keys)
        for key in keys:
            self._partial_update(key['id'], key)

        return Response(status=status.HTTP_204_NO_CONTENT)

    def destroy(self, request, minion_id):
        self.client.minion_delete(minion_id)
        return Response(status=status.HTTP_204_NO_CONTENT)

    def list_destroy(self, request):
        self._validate_list(request)
        keys = request.DATA
        for key in keys:
            self.client.minion_delete(key['id'])

        return Response(status=status.HTTP_204_NO_CONTENT)

    def retrieve(self, request, minion_id):
        return Response(self.serializer_class(self.client.minion_get(minion_id)).data)


class PoolDataObject(DataObject):
    """
    Slightly dressed up version of the raw pool from osd dump
    """

    FLAG_HASHPSPOOL = 1
    FLAG_FULL = 2

    @property
    def hashpspool(self):
        return bool(self.flags & self.FLAG_HASHPSPOOL)

    @property
    def full(self):
        return bool(self.flags & self.FLAG_FULL)


class RequestReturner(object):
    """
    Helper for ViewSets that sometimes need to return a request handle
    """
    def _return_request(self, request):
        if request:
            return Response(request, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(status=status.HTTP_304_NOT_MODIFIED)


class NullableDataObject(DataObject):
    """
    A DataObject which synthesizes Nones for any attributes it doesn't have
    """
    def __getattr__(self, item):
        if not item.startswith('_'):
            return self.__dict__.get(item, None)
        else:
            raise AttributeError


class ConfigViewSet(RPCViewSet):
    """
Configuration settings from a Ceph Cluster.
    """
    serializer_class = ConfigSettingSerializer

    def list(self, request):
        ceph_config = self.client.get_sync_object(Config).data
        settings = [DataObject({'key': k, 'value': v}) for (k, v) in ceph_config.items()]
        return Response(self.serializer_class(settings, many=True).data)

    def retrieve(self, request, key):
        ceph_config = self.client.get_sync_object(Config).data
        try:
            setting = DataObject({'key': key, 'value': ceph_config[key]})
        except KeyError:
            raise Http404("Key '%s' not found" % key)
        else:
            return Response(self.serializer_class(setting).data)


def _config_to_bool(config_val):
    return {'true': True, 'false': False}[config_val.lower()]


class PoolViewSet(RPCViewSet, RequestReturner):
    """
Manage Ceph storage pools.

To get the default values which will be used for any fields omitted from a POST, do
a GET with the ?defaults argument.  The returned pool object will contain all attributes,
but those without static defaults will be set to null.

    """
    serializer_class = PoolSerializer

    def _defaults(self):
        # Issue overlapped RPCs first
        ceph_config = self.client.get_sync_object(Config)
        rules = self.client.list(CRUSH_RULE, {})

        if not ceph_config:
            return Response("Cluster configuration unavailable", status=status.HTTP_503_SERVICE_UNAVAILABLE)

        if not rules:
            return Response("No CRUSH rules exist, pool creation is impossible",
                            status=status.HTTP_503_SERVICE_UNAVAILABLE)

        # Ceph does not reliably inform us of a default ruleset that exists, so we check
        # what it tells us against the rulesets we know about.
        ruleset_ids = sorted(list(set([r['ruleset'] for r in rules])))
        if int(ceph_config['osd_pool_default_crush_rule']) in ruleset_ids:
            # This is the ceph<0.80 setting
            default_ruleset = ceph_config['osd_pool_default_crush_rule']
        elif int(ceph_config.get('osd_pool_default_crush_replicated_ruleset', -1)) in ruleset_ids:
            # This is the ceph>=0.80
            default_ruleset = ceph_config['osd_pool_default_crush_replicated_ruleset']
        else:
            # Ceph may have an invalid default set which
            # would cause undefined behaviour in pool creation (#8373)
            # In this case, pick lowest numbered ruleset as default
            default_ruleset = ruleset_ids[0]

        defaults = NullableDataObject({
            'size': int(ceph_config['osd_pool_default_size']),
            'crush_ruleset': int(default_ruleset),
            'min_size': int(ceph_config['osd_pool_default_min_size']),
            'hashpspool': _config_to_bool(ceph_config['osd_pool_default_flag_hashpspool']),
            # Crash replay interval is zero by default when you create a pool, but when ceph creates
            # its own data pool it applies 'osd_default_data_pool_replay_window'.  If we add UI for adding
            # pools to a filesystem, we should check that those data pools have this set.
            'crash_replay_interval': 0,
            'quota_max_objects': 0,
            'quota_max_bytes': 0
        })

        return Response(PoolSerializer(defaults).data)

    def list(self, request):
        if 'defaults' in request.GET:
            return self._defaults()

        pools = [PoolDataObject(p) for p in self.client.list(POOL, {})]
        return Response(PoolSerializer(pools, many=True).data)

    def retrieve(self, request, pool_id):
        pool = PoolDataObject(self.client.get(POOL, int(pool_id)))
        return Response(PoolSerializer(pool).data)

    def create(self, request):
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid(request.method):
            response = self._validate_semantics(None, serializer.get_data())
            if response is not None:
                return response

            create_response = self.client.create(POOL, serializer.get_data())

            # TODO: handle case where the creation is rejected for some reason (should
            # be passed an errors dict for a clean failure, or a zerorpc exception
            # for a dirty failure)
            assert 'request_id' in create_response
            return Response(create_response, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def update(self, request, pool_id):
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid(request.method):
            response = self._validate_semantics(pool_id, serializer.get_data())
            if response is not None:
                return response

            return self._return_request(self.client.update(POOL, int(pool_id), serializer.get_data()))
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, pool_id):
        delete_response = self.client.delete(POOL, int(pool_id), status=status.HTTP_202_ACCEPTED)
        return Response(delete_response, status=status.HTTP_202_ACCEPTED)

    def _validate_semantics(self, pool_id, data):
        errors = defaultdict(list)
        self._check_name_unique(data, errors)
        self._check_crush_ruleset(data, errors)
        self._check_pgp_less_than_pg_num(data, errors)
        self._check_pg_nums_dont_decrease(pool_id, data, errors)
        self._check_pg_num_inside_config_bounds(data, errors)

        if errors.items():
            if 'name' in errors:
                return Response(errors, status=status.HTTP_409_CONFLICT)
            else:
                return Response(errors, status=status.HTTP_400_BAD_REQUEST)

    def _check_pg_nums_dont_decrease(self, pool_id, data, errors):
        if pool_id is not None:
            detail = self.client.get(POOL, int(pool_id))
            for field in ['pg_num', 'pgp_num']:
                expanded_field = 'pg_placement_num' if field == 'pgp_num' else 'pg_num'
                if field in data and data[field] < detail[expanded_field]:
                    errors[field].append('must be >= than current {field}'.format(field=field))

    def _check_crush_ruleset(self, data, errors):
        if 'crush_ruleset' in data:
            rules = self.client.list(CRUSH_RULE, {})
            rulesets = set(r['ruleset'] for r in rules)
            if data['crush_ruleset'] not in rulesets:
                errors['crush_ruleset'].append("CRUSH ruleset {0} not found".format(data['crush_ruleset']))

    def _check_pg_num_inside_config_bounds(self, data, errors):
        ceph_config = self.client.get_sync_object(Config).data
        if not ceph_config:
            return Response("Cluster configuration unavailable", status=status.HTTP_503_SERVICE_UNAVAILABLE)
        if 'pg_num' in data and data['pg_num'] > int(ceph_config['mon_max_pool_pg_num']):
            errors['pg_num'].append('requested pg_num must be <= than current limit of {max}'.format(max=ceph_config['mon_max_pool_pg_num']))

    def _check_pgp_less_than_pg_num(self, data, errors):
        if 'pgp_num' in data and 'pg_num' in data and data['pg_num'] < data['pgp_num']:
            errors['pgp_num'].append('must be >= to pg_num')

    def _check_name_unique(self, data, errors):
        if 'name' in data and data['name'] in [x.pool_name for x in [PoolDataObject(p) for p in self.client.list(POOL, {})]]:
            errors['name'].append('Pool with name {name} already exists'.format(name=data['name']))


class OsdViewSet(RPCViewSet, RequestReturner):
    """
Manage Ceph OSDs.

Apply ceph commands to an OSD by doing a POST with no data to
api/v2/cluster/<fsid>/osd/<osd_id>/command/<command>
where <command> is one of ("scrub", "deep-scrub", "repair")

e.g. Initiate a scrub on OSD 0 by POSTing {} to api/v2/cluster/<fsid>/osd/0/command/scrub

Filtering is available on this resource:

::

    # Pass a ``pool`` URL parameter set to a pool ID to filter by pool, like this:
    /api/v2/cluster/<fsid>/osd?pool=1

    # Pass a series of ``id__in[]`` parameters to specify a list of OSD IDs
    # that you wish to receive.
    /api/v2/cluster/<fsid>/osd?id__in[]=2&id__in[]=3

    """
    serializer_class = OsdSerializer

    def list(self, request):
        return self._list(request)

    def _list(self, request):
        # Get data needed for filtering
        list_filter = {}

        if 'pool' in request.GET:
            try:
                pool_id = int(request.GET['pool'])
            except ValueError:
                return Response("Pool ID must be an integer", status=status.HTTP_400_BAD_REQUEST)
            list_filter['pool'] = pool_id

        if 'id__in[]' in request.GET:
            try:
                ids = request.GET.getlist("id__in[]")
                list_filter['id__in'] = [int(i) for i in ids]
            except ValueError:
                return Response("Invalid OSD ID in list", status=status.HTTP_400_BAD_REQUEST)

        # Get data
        osds = self.client.list(OSD, list_filter)
        osd_to_pools = self.client.get_sync_object(OsdMap, ['osd_pools'])
        crush_nodes = self.client.get_sync_object(OsdMap, ['osd_tree_node_by_id'])
        osd_metadata = self.client.get_sync_object(OsdMap, ['osd_metadata'])

        osd_id_to_hostname = dict(
            [(int(osd_id), osd_meta["hostname"]) for osd_id, osd_meta in
             osd_metadata.items()])

        # Get data depending on OSD list
        osd_commands = self.client.get_valid_commands(OSD, [x['osd'] for x in osds])

        # Build OSD data objects
        for o in osds:
            # An OSD being in the OSD map does not guarantee its presence in the CRUSH
            # map, as "osd crush rm" and "osd rm" are separate operations.
            try:
                o.update({'reweight': float(crush_nodes[o['osd']]['reweight'])})
            except KeyError:
                log.warning("No CRUSH data available for OSD {0}".format(o['osd']))
                o.update({'reweight': 0.0})

            o['server'] = osd_id_to_hostname.get(o['osd'], None)

        for o in osds:
            o['pools'] = osd_to_pools[o['osd']]

        for o in osds:
            o.update(osd_commands[o['osd']])

        return Response(self.serializer_class([DataObject(o) for o in osds], many=True).data)

    def retrieve(self, request, osd_id):
        osd = self.client.get_sync_object(OsdMap, ['osds_by_id', int(osd_id)])
        crush_node = self.client.get_sync_object(OsdMap, ['osd_tree_node_by_id', int(osd_id)])
        osd['reweight'] = float(crush_node['reweight'])

        osd_metadata = self.client.get_sync_object(OsdMap, ['osd_metadata'])

        osd_id_to_hostname = dict(
            [(int(osd_id), osd_meta["hostname"]) for osd_id, osd_meta in
             osd_metadata.items()])


        osd['server'] = osd_id_to_hostname.get(osd['osd'], None)

        pools = self.client.get_sync_object(OsdMap, ['osd_pools', int(osd_id)])
        osd['pools'] = pools

        osd_commands = self.client.get_valid_commands(OSD, [int(osd_id)])
        osd.update(osd_commands[int(osd_id)])

        return Response(self.serializer_class(DataObject(osd)).data)

    def update(self, request, osd_id):
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid(request.method):
            return self._return_request(self.client.update(OSD, int(osd_id),
                                                           serializer.get_data()))
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def apply(self, request, osd_id, command):
        if command in self.client.get_valid_commands(OSD, [int(osd_id)]).get(int(osd_id)).get('valid_commands'):
            return Response(self.client.apply(OSD, int(osd_id), command), status=202)
        else:
            return Response('{0} not valid on {1}'.format(command, osd_id), status=403)

    def get_implemented_commands(self, request):
        return Response(OSD_IMPLEMENTED_COMMANDS)

    def get_valid_commands(self, request, osd_id=None):
        osds = []
        if osd_id is None:
            osds = self.client.get_sync_object(OsdMap, ['osds_by_id']).keys()
        else:
            osds.append(int(osd_id))

        return Response(self.client.get_valid_commands(OSD, osds))

    def validate_command(self, request, osd_id, command):
        valid_commands = self.client.get_valid_commands(OSD, [int(osd_id)]).get(int(osd_id)).get('valid_commands')

        return Response({'valid': command in valid_commands})


class OsdConfigViewSet(RPCViewSet, RequestReturner):
    """
Manage flags in the OsdMap
    """
    serializer_class = OsdConfigSerializer

    def osd_config(self, request):
        osd_map = self.client.get_sync_object(OsdMap, ['flags'])
        return Response(osd_map)

    def update(self, request):

        serializer = self.serializer_class(data=request.DATA)
        if not serializer.is_valid(request.method):
            return Response(serializer.errors, status=403)

        response = self.client.update(OSD_MAP, None, serializer.get_data())

        return self._return_request(response)


class SyncObject(RPCViewSet):
    """
These objects are the raw data received by the Calamari server from the Ceph cluster,
such as the cluster maps
    """

    def retrieve(self, request, sync_type):
        return Response(self.client.get_sync_object(sync_type))

    def describe(self, request):
        return Response([s.str for s in SYNC_OBJECT_TYPES])


class DebugJob(RPCViewSet, RequestReturner):
    """
For debugging and automated testing only.
    """
    def create(self, request, fqdn):
        cmd = request.DATA['cmd']
        args = request.DATA['args']

        # Avoid this debug interface being an arbitrary execution mechanism.
        if not cmd.startswith("ceph.selftest"):
            raise PermissionDenied("Command '%s' is not a self test command".format(cmd))

        return self._return_request(self.client.debug_job(fqdn, cmd, args))


class ServerViewSet(RPCViewSet):
    """
Servers that we've learned about via the daemon metadata reported by
Ceph OSDs, MDSs, mons.
    """
    serializer_class = ServerSerializer

    def retrieve(self, request, fqdn):
        return Response(
            self.serializer_class(
                DataObject(self.client.server_get(fqdn))).data
        )

    def list(self, request):
        servers = self.client.server_list()
        return Response(self.serializer_class(
            [DataObject(s) for s in servers],
            many=True).data)


if False:
    class EventViewSet(DatabaseViewSet, PaginatedMixin):
        """
    Events generated by Calamari server in response to messages from
    servers and Ceph clusters.  This resource is paginated.

    Note that events are not visible synchronously with respect to
    all other API resources.  For example, you might read the OSD
    map, see an OSD is down, then quickly read the events and find
    that the event about the OSD going down is not visible yet (though
    it would appear very soon after).

    The ``severity`` attribute mainly follows a typical INFO, WARN, ERROR
    hierarchy.  However, we have an additional level between INFO and WARN
    called RECOVERY.  Where something going bad in the system is usually
    a WARN message, the opposite state transition is usually a RECOVERY
    message.

    This resource supports "more severe than" filtering on the severity
    attribute.  Pass the desired severity threshold as a URL parameter
    in a GET, such as ``?severity=RECOVERY`` to show everything but INFO.

        """
        serializer_class = EventSerializer

        @property
        def queryset(self):
            return self.session.query(Event).order_by(Event.when.desc())

        def _filter_by_severity(self, request, queryset=None):
            if queryset is None:
                queryset = self.queryset
            severity_str = request.GET.get("severity", "INFO")
            try:
                severity = severity_from_str(severity_str)
            except KeyError:
                raise ParseError("Invalid severity '%s', must be on of %s" % (severity_str,
                                                                              ",".join(SEVERITIES.values())))

            return queryset.filter(Event.severity <= severity)

        def list(self, request):
            return Response(self._paginate(request, self._filter_by_severity(request)))

        def list_cluster(self, request):
            return Response(self._paginate(request, self._filter_by_severity(request, self.queryset.filter_by(fsid=fsid))))

        def list_server(self, request, fqdn):
            return Response(self._paginate(request, self._filter_by_severity(request, self.queryset.filter_by(fqdn=fqdn))))


if False:
    class LogTailViewSet(RemoteViewSet):
        """
    A primitive remote log viewer.

    Logs are retrieved on demand from the Ceph servers, so this resource will return a 503 error if no suitable
    server is available to get the logs.

    GETs take an optional ``lines`` parameter for the number of lines to retrieve.
        """
        serializer_class = LogTailSerializer

        def get_cluster_log(self, request):
            """
            Retrieve the cluster log from one of a cluster's mons (expect it to be in /var/log/ceph/ceph.log)
            """

            # Number of lines to get
            lines = request.GET.get('lines', 40)

            # Resolve FSID to name
            name = self.client.get_cluster(fsid)['name']

            # Execute remote operation synchronously
            result = self.run_mon_job("log_tail.tail", ["ceph/{name}.log".format(name=name), lines])

            return Response({'lines': result})

        def list_server_logs(self, request, fqdn):
            return Response(sorted(self.run_job(fqdn, "log_tail.list_logs", ["."])))

        def get_server_log(self, request, fqdn, log_path):
            lines = request.GET.get('lines', 40)
            return Response({'lines': self.run_job(fqdn, "log_tail.tail", [log_path, lines])})


class MonViewSet(RPCViewSet):
    """
Ceph monitor services.

Note that the ID used to retrieve a specific mon using this API resource is
the monitor *name* as opposed to the monitor *rank*.

The quorum status reported here is based on the last mon status reported by
the Ceph cluster, and also the status of each mon daemon queried by Calamari.

For debugging mons which are failing to join the cluster, it may be
useful to show users data from the /status sub-url, which returns the
"mon_status" output from the daemon.

    """
    serializer_class = MonSerializer

    def _get_mons(self):
        monmap_mons = self.client.get_sync_object(MonMap).data['mons']
        mon_status = self.client.get_sync_object(MonStatus).data

        for mon in monmap_mons:
            mon['in_quorum'] = mon['rank'] in mon_status['quorum']
            mon['server'] = self.client.get_metadata("mon", mon['name'])['hostname']
            mon['leader'] = mon['rank'] == mon_status['quorum'][0]

        return monmap_mons

    def retrieve(self, request, mon_id):
        mons = self._get_mons()
        try:
            mon = [m for m in mons if m['name'] == mon_id][0]
        except IndexError:
            raise Http404("Mon '%s' not found" % mon_id)

        return Response(self.serializer_class(DataObject(mon)).data)

    def list(self, request):
        mons = self._get_mons()
        return Response(
            self.serializer_class([DataObject(m) for m in mons],
                                  many=True).data)


if False:
    class CliViewSet(RemoteViewSet):
        """
    Access the `ceph` CLI tool remotely.

    To achieve the same result as running "ceph osd dump" at a shell, an
    API consumer may POST an object in either of the following formats:

    ::

        {'command': ['osd', 'dump']}

        {'command': 'osd dump'}


    The response will be a 200 status code if the command executed, regardless
    of whether it was successful, to check the result of the command itself
    read the ``status`` attribute of the returned data.

    The command will be executed on the first available mon server, retrying
    on subsequent mon servers if no response is received.  Due to this retry
    behaviour, it is possible for the command to be run more than once in
    rare cases; since most ceph commands are idempotent this is usually
    not a problem.
        """
        serializer_class = CliSerializer

        def create(self, request):
            # Validate
            try:
                command = request.DATA['command']
            except KeyError:
                raise ParseError("'command' field is required")
            else:
                if not (isinstance(command, basestring) or isinstance(command, list)):
                    raise ParseError("'command' must be a string or list")

            # Parse string commands to list
            if isinstance(command, basestring):
                command = shlex.split(command)

            name = self.client.get_cluster(fsid)['name']
            result = self.run_mon_job("ceph.ceph_command", [name, command])
            log.debug("CliViewSet: result = '%s'" % result)

            if not isinstance(result, dict):
                # Errors from salt like "module not available" come back as strings
                raise APIException("Remote error: %s" % str(result))

            return Response(self.serializer_class(DataObject(result)).data)
