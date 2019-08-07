# Integrate with the kubernetes events API. 
# This module sends events to Kubernetes, and also captures/tracks all events
# in the rook-ceph namespace so kubernetes activity like pod restarts,
# imagepulls etc can be seen from within the ceph cluster itself.
#
# To interact with the events API, the mgr service to access needs to be 
# granted additional permissions
# e.g. kubectl -n rook-ceph edit clusterrole rook-ceph-mgr-cluster-rules
#
# These are the changes needed;
# - apiGroups:
#   - ""
#   resources:
#   - events
#   verbs:
#   - create
#   - patch
#   - list
#   - get
#   - watch

# LogEntry class dynamically builds an object from a set of args,
# disable the linter for the no-member error
# pylint: disable=no-member


import os
import sys
import time
import json
import threading

from datetime import datetime, timedelta, tzinfo
from urllib3.exceptions import MaxRetryError
from collections import OrderedDict

import rados
from mgr_module import MgrModule
from ceph_argparse import json_command, run_in_thread

try:
    import queue
except ImportError:
    # python 2.7.5
    import Queue as queue
finally:
    # python 2.7.15 or python3
    event_queue = queue.Queue()

try:
    from kubernetes import client, config, watch
    from kubernetes.client.rest import ApiException
except ImportError:
    kubernetes_imported = False
    client = None
    config = None
else:
    kubernetes_imported = True
    # load kubernetes config
    config.load_incluster_config()

DEFAULT_LOG_LEVEL = 'info'
DEFAULT_HEARTBEAT_INTERVAL_SECS = 3600  # 1hr default
DEFAULT_CONFIG_CHECK_SECS = 10

ZERO = timedelta(0)


def text_suffix(num):
    """Define a text suffix based on a value i.e. turn host into hosts"""
    return '' if num == 1 else 's'


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO

class HealthCheck(object):
    """Transform a healthcheck msg into it's component parts"""

    def __init__(self, msg):

        # msg looks like
        # 2019-07-17 02:59:12.714672 mon.a [WRN] Health check failed: Reduced data availability: 100 pgs inactive (PG_AVAILABILITY)
        # 2019-07-17 03:16:39.103929 mon.a [INF] Health check cleared: OSDMAP_FLAGS (was: nodown flag(s) set)
        # 2019-07-29 04:28:13.123868 mon.a [WRN] Health check failed: nodown flag(s) set (OSDMAP_FLAGS)

        self.msg = None
        self.state = None
        self.name = None
        self.text = None
        self.fulltext = None
        self.valid = False
        
        if 'health check' in msg.lower():
            self.valid = True

            self.msg = msg
            tokens = msg.split()
            self.state = tokens[3][1:-1]
            self.fulltext =' '.join(tokens[4:])
            msg_tokens = self.fulltext.split()
            
            if self.state == 'INF':
                self.text = ' '.join(msg_tokens[3:])
                self.name = msg_tokens[3]
            else:   # WRN or ERR
                self.text = ' '.join(msg_tokens[3:-1])
                self.name = tokens[-1][1:-1]


class LogEntry(object):
    """Generic 'log' object"""

    reason_map = {
        "audit": "Audit",
        "cluster": "HealthCheck",
        "config": "ClusterChange",
        "heartbeat":"Heartbeat",
        "startup": "Started"
    }

    def __init__(self, *args, **kwargs):

        # self.update(**kwargs) - python3 syntax
        for k,v in kwargs.items():
            setattr(self, k, v)
        

        if 'health check ' in self.msg.lower():
            self.healthcheck = HealthCheck(self.msg)
        else:
            self.healthcheck = None
    
    def __str__(self):
        return "source={}, msg_type={}, msg={}, level={}, tstamp={}".format(self.source,self.msg_type,self.msg,self.level,self.tstamp)

    @property
    def cmd(self):
        """Look at the msg string and extract the command content"""

        # msg looks like 'from=\'client.205306 \' entity=\'client.admin\' cmd=\'[{"prefix": "osd set", "key": "nodown"}]\': finished'
        if self.msg_type != 'audit':
            return None
        else:
            _m=self.msg[:-10].replace("\'","").split("cmd=")
            _s='"cmd":{}'.format(_m[1])
            cmds_list = json.loads('{' + _s + '}')['cmd']

            # TODO. Assuming only one command was issued for now
            _c = cmds_list[0]
            return "{} {}".format(_c['prefix'], _c.get('key', ''))
    
    @property
    def event_type(self):
        return 'Normal' if self.level == 'INF' else 'Warning'
    
    @property
    def event_reason(self):
        return self.reason_map[self.msg_type]

    @property
    def event_name(self):
        if self.msg_type == 'heartbeat':
            return 'mgr.Heartbeat'
        elif self.healthcheck:
            return 'mgr.health.{}'.format(self.healthcheck.name)
        elif self.msg_type == 'audit':
            return 'mgr.audit.{}'.format(self.cmd).replace(' ', '_')
        elif self.msg_type == 'config':
            return 'mgr.ConfigurationChange'
        elif self.msg_type == 'startup':
            return "mgr.k8sevents-module"
        else:
            return None
   
    @property
    def event_entity(self):
        if self.msg_type == 'audit':
            return self.msg.replace("\'","").split('entity=')[1].split(' ')[0]
        else:
            return None
    
    @property
    def event_msg(self):
        if self.msg_type == 'audit':
            return "Client '{}' issued: ceph {}".format(self.event_entity, self.cmd)

        elif self.healthcheck:
            return self.healthcheck.text
        else:
            return self.msg

class RookCeph(object):
    """Establish environment defaults when interacting with rook-ceph"""

    pod_name = os.environ['POD_NAME']
    host = os.environ['NODE_NAME']
    namespace = os.environ.get('POD_NAMESPACE', 'rook-ceph')
    cluster_name = os.environ.get('ROOK_CEPH_CLUSTER_CRD_NAME', 'rook-ceph')
    api = client.CoreV1Api()


class NamespaceWatcher(RookCeph, threading.Thread):
    """Watch events in a given namespace 
    
    Using the watch package we can listen to event traffic in the namespace to 
    get an idea of what kubernetes related events surround the ceph cluster. The
    thing to bear in mind is that events have a TTL enforced by the kube-apiserver
    so this stream will only really show activity inside this retention window.
    """
    daemon = True

    def __init__(self, logger, namespace=None):
        super(NamespaceWatcher, self).__init__()
        self.log = logger
        if namespace:                       # override the default
            self.namespace = namespace
        self.events = OrderedDict()
        self.lock = threading.Lock()
        self.health = "OK"
        self.active = None
        self.resource_version = None

    def fetch(self):
        # clear the cache on every call to fetch
        self.events.clear()
        try:
            resp = self.api.list_namespaced_event(self.namespace)
        # TODO - test for auth problem to be more specific in the except clause
        except:
            self.active = False
            self.health = "[WRN] Unable to access events API (list_namespaced_event call failed)"
            self.log.warning(self.health)
        else:
            self.active = True
            self.resource_version = resp.metadata.resource_version

            # TODO this fetches the events, but storing them like this is not preserving the 
            # creation timestamp of the event itself - it's just using arrival order
            
            for item in resp.items:
                self.events[item.metadata.name] = item
            self.log.warning('[INF] added {} events'.format(len(resp.items)))

    def run(self):
        self.fetch()
        if self.active:
            self.log.warning("[INF] Namespace event watcher started")

            func = getattr(self.api, "list_namespaced_event")

            try:
                w = watch.Watch()
                # execute generator to continually watch resource for changes
                for item in w.stream(func, namespace=self.namespace, resource_version=self.resource_version, watch=True):
                    obj = item['object']

                    with self.lock:

                        if item['type'] in ['ADDED', 'MODIFIED']:
                            # self.log.warning("[DBG] ADD: {}".format(item))
                            self.events[obj.metadata.name] = obj

                        elif item['type'] == 'DELETED':
                            # self.log.warning("[DBG] DEL: {}".format(item))
                            del self.events[obj.metadata.name]
                        
            # TODO test the exception for auth problem (403?)
 
            # Attribute error is generated when urllib3 on the system is old and doesn't have a
            # read_chunked method
            except AttributeError as e:
                self.health = ("[WRN] : Unable to 'watch' events API in namespace {} - "
                            "incompatible urllib3? ({})".format(self.namespace, e))
                self.active = False
                self.log.warning(self.health)
            except BaseException:
                self.health = "[ERR] Unexpected {} Exception @ line {}".format(sys.exc_info()[0].__name__, 
                                                                               sys.exc_info()[2].tb_lineno)
                self.active = False

            self.log.warning("[WRN] Namespace event watcher stopped")
            if self.health != 'OK':
                self.log.error(self.health)


class KubernetesEvent(RookCeph):

    def __init__(self, log_entry, unique_name=True):
        super(KubernetesEvent, self).__init__()

        self.event_name = log_entry.event_name
        self.event_msg = log_entry.event_msg
        self.event_type = log_entry.event_type
        self.event_reason = log_entry.event_reason
        self.unique_name = unique_name

        self.api_status = 200
        self.count = 1
        self.first_timestamp = None
        self.last_timestamp = None

    @property
    def event_body(self):
        if self.unique_name:
            obj_meta = client.V1ObjectMeta(name="{}".format(self.event_name)) 
        else:
            obj_meta = client.V1ObjectMeta(generate_name="{}".format(self.event_name))

        # field_path is needed to prevent problems in the namespacewatcher when
        # deleted event are received
        obj_ref = client.V1ObjectReference(kind="CephCluster",
                                           field_path='spec.containers{mgr}', 
                                           name=self.event_name, 
                                           namespace=self.namespace)

        event_source = client.V1EventSource(component="ceph-mgr", 
                                            host=self.host)
        return  client.V1Event(
                    involved_object=obj_ref, 
                    metadata=obj_meta, 
                    message=self.event_msg, 
                    count=self.count, 
                    type=self.event_type,
                    reason=self.event_reason,
                    source=event_source, 
                    first_timestamp=self.first_timestamp,
                    last_timestamp=self.last_timestamp
                )

    def write(self):

        now=datetime.now(UTC())

        self.first_timestamp = now
        self.last_timestamp = now

        try:
            self.api.create_namespaced_event(self.namespace, self.event_body)
        except MaxRetryError:
            # k8s config has not be defined properly 
            self.api_status = 403       # Forbidden
        except ApiException as e:
            self.api_status = e.status
            if e.status == 409:
                # 409 means the event is there already, so read it back (v1Event object returned)
                # this could happen if the event has been created, and then the k8sevent module
                # disabled and reenabled - i.e. the internal event tracking no longer matches k8s
                response = self.api.read_namespaced_event(self.event_name, self.namespace)
                # response looks like
                # {'action': None,
                # 'api_version': 'v1',
                # 'count': 1,
                # 'event_time': None,
                # 'first_timestamp': datetime.datetime(2019, 7, 18, 5, 24, 59, tzinfo=tzlocal()),
                # 'involved_object': {'api_version': None,
                #                     'field_path': None,
                #                     'kind': 'CephCluster',
                #                     'name': 'ceph-mgr.k8sevent-module',
                #                     'namespace': 'rook-ceph',
                #                     'resource_version': None,
                #                     'uid': None},
                # 'kind': 'Event',
                # 'last_timestamp': datetime.datetime(2019, 7, 18, 5, 24, 59, tzinfo=tzlocal()),
                # 'message': 'Ceph log -> event tracking started',
                # 'metadata': {'annotations': None,
                #             'cluster_name': None,
                #             'creation_timestamp': datetime.datetime(2019, 7, 18, 5, 24, 59, tzinfo=tzlocal()),
                #             'deletion_grace_period_seconds': None,
                #             'deletion_timestamp': None,
                #             'finalizers': None,
                #             'generate_name': 'ceph-mgr.k8sevent-module',
                #             'generation': None,
                #             'initializers': None,
                #             'labels': None,
                #             'name': 'ceph-mgr.k8sevent-module5z7kq',
                #             'namespace': 'rook-ceph',
                #             'owner_references': None,
                #             'resource_version': '1195832',
                #             'self_link': '/api/v1/namespaces/rook-ceph/events/ceph-mgr.k8sevent-module5z7kq',
                #             'uid': '62fde5f1-a91c-11e9-9c80-6cde63a9debf'},
                # 'reason': 'Started',
                # 'related': None,
                # 'reporting_component': '',
                # 'reporting_instance': '',
                # 'series': None,
                # 'source': {'component': 'ceph-mgr', 'host': 'minikube'},
                # 'type': 'Normal'}

                # conflict event already exists
                # read it
                # update : count and last_timestamp and msg

                self.count = response.count + 1
                self.first_timestamp = response.first_timestamp
                try:
                    self.api.patch_namespaced_event(self.event_name, self.namespace, self.event_body)
                except ApiException as e:
                    self.api_status = e.status
                else:
                    self.api_status = 200

        else:
            self.api_status = 200

    @property
    def api_success(self):
        return self.api_status == 200

    def update(self, log_entry):
        self.event_msg = log_entry.event_msg
        self.event_type = log_entry.event_type
        self.last_timestamp = datetime.now(UTC())
        self.count += 1

        try:
            self.api.patch_namespaced_event(self.event_name, self.namespace, self.event_body)
        except ApiException as e:
            if e.status == 404:
                # tried to patch 404 indicates it's TTL has expired
                try:
                    self.api.create_namespaced_event(self.namespace, self.event_body)
                except ApiException as e:
                    self.api_status = e.status
                else:
                    self.api_status = 200
        else:
            self.api_status = 200 


class EventProcessor(threading.Thread):
    """Handle a global queue used to track events we want to send/update to kubernetes"""

    daemon = True
    can_run = True

    def __init__(self, logger):
        self.events = dict()
        self.log = logger
        self.health = 'OK'
        threading.Thread.__init__(self)

    def startup(self):
        """Log an event to show we're active"""
        
        event = KubernetesEvent(
            LogEntry(
                source='self',
                msg='Ceph log -> event tracking started',
                msg_type='startup',
                level='INF',
                tstamp=None
            ),
            unique_name=False
        )
        
        event.write()
        return event.api_success
    
    @property
    def ok(self):
        return self.startup()

    def process(self, log_object):
        
        self.log.warning("processing {}".format(str(log_object)))
        # self.log.warning("name would be {}".format(log_object.event_name))
        event_out = False
        unique_name = True

        if log_object.msg_type == 'audit':
            # audit traffic : operator commands
            if log_object.msg.endswith('finished'):
                self.log.warning("finished msg seen")
                event_out = True
            else:
                # NO OP - ignoring 'dispatch' log records
                return
        
        elif log_object.msg_type == 'cluster':
            # cluster messages : health checks
            if log_object.event_name:
                event_out = True

        elif log_object.msg_type in ['config', 'heartbeat']:
            # configuration checker and heartbeat messages
            event_out = True
            unique_name = False
        else:
            # unknown msgtype?
            pass

        if event_out:
            # we don't cache non-unique event names
            if not unique_name or log_object.event_name not in self.events.keys():
                event = KubernetesEvent(log_entry=log_object,
                                        unique_name=unique_name)
                event.write()
                self.log.warning("[DBG] event(unique={}) creation ended : {}".format(unique_name, event.api_status))
                if event.api_success and unique_name:
                    self.events[log_object.event_name] = event
            else:
                event = self.events[log_object.event_name]
                event.update(log_object)
                self.log.warning("[DBG] event update ended : {}".format(event.api_status))

        else:
            self.log.warning("[DBG] ignored message : {}".format(log_object.msg))

    def run(self):
        self.log.warning("[INF] Ceph event processing thread started")
        while True:

            try:
                log_object = event_queue.get(block=False)
            except queue.Empty:
                pass
            else:
                try:
                    self.process(log_object)
                except BaseException:
                    self.health = "[ERR] Unexpected {} Exception @ line {}".format(sys.exc_info()[0].__name__, 
                                                                                   sys.exc_info()[2].tb_lineno)
                    break
            
            if not self.can_run:
                break

            time.sleep(0.5)

        self.log.warning("[WRN] Ceph event processing thread stopped")
        if self.health != 'OK':
            self.log.error(self.health)

class ListDiff(object):
    def __init__(self, before, after):
        self.before = set(before)
        self.after = set(after)

    @property
    def removed(self):
        return list(self.before - self.after)

    @property
    def added(self):
        return list(self.after - self.before)


class CephConfigWatcher(threading.Thread):
    """Detect configuration changes within the cluster and generate human readable events"""

    daemon = True

    def __init__(self, mgr):
        self.mgr = mgr
        self.server_map = dict()
        self.osd_map = dict()
        self.pool_map = dict()
        self.service_map = dict()
        self.health = "OK"
        
        self.heartbeat_interval_secs = self.mgr.get_localized_module_option(
            'heartbeat_interval_secs', DEFAULT_HEARTBEAT_INTERVAL_SECS)
        self.config_check_secs = self.mgr.get_localized_module_option(
            'config_check_secs', DEFAULT_CONFIG_CHECK_SECS)

        threading.Thread.__init__(self)
    
    @property
    def raw_capacity(self):
        # Note. if the osd's are not online the capacity field will be 0
        return sum([self.osd_map[osd]['capacity'] for osd in self.osd_map])
    
    @property
    def num_servers(self):
        return len(self.server_map.keys())

    @property
    def num_osds(self):
        return len(self.osd_map.keys())
    
    @property
    def num_pools(self):
        return len(self.pool_map.keys())

    def fetch_servers(self):
        """Return a server summary, and service summary"""
        servers = self.mgr.list_servers()
        server_map = dict()         # host -> services
        service_map = dict()        # service -> host
        for server_info in servers:
            services = dict()
            for svc in server_info['services']:
                if svc.get('type') in services.keys():
                    services[svc.get('type')].append(svc.get('id'))
                else:
                    services[svc.get('type')] = list([svc.get('id')])
                # maintain the service xref map service -> host and version
                service_map[(svc.get('type'), str(svc.get('id')))] = server_info.get('hostname', '')
            server_map[server_info.get('hostname')] = services

        return server_map, service_map
    
    def fetch_pools(self):
        interesting = ["type", "size", "min_size"]
        # pools = [{'pool': 1, 'pool_name': 'replicapool', 'flags': 1, 'flags_names': 'hashpspool', 
        #           'type': 1, 'size': 3, 'min_size': 1, 'crush_rule': 1, 'object_hash': 2, 'pg_autoscale_mode': 'warn', 
        #           'pg_num': 100, 'pg_placement_num': 100, 'pg_placement_num_target': 100, 'pg_num_target': 100, 'pg_num_pending': 100, 
        #           'last_pg_merge_meta': {'ready_epoch': 0, 'last_epoch_started': 0, 'last_epoch_clean': 0, 'source_pgid': '0.0', 
        #           'source_version': "0'0", 'target_version': "0'0"}, 'auid': 0, 'snap_mode': 'selfmanaged', 'snap_seq': 0, 'snap_epoch': 0,
        #           'pool_snaps': [], 'quota_max_bytes': 0, 'quota_max_objects': 0, 'tiers': [], 'tier_of': -1, 'read_tier': -1, 
        #           'write_tier': -1, 'cache_mode': 'none', 'target_max_bytes': 0, 'target_max_objects': 0, 
        #           'cache_target_dirty_ratio_micro': 400000, 'cache_target_dirty_high_ratio_micro': 600000, 
        #           'cache_target_full_ratio_micro': 800000, 'cache_min_flush_age': 0, 'cache_min_evict_age': 0, 
        #           'erasure_code_profile': '', 'hit_set_params': {'type': 'none'}, 'hit_set_period': 0, 'hit_set_count': 0, 
        #           'use_gmt_hitset': True, 'min_read_recency_for_promote': 0, 'min_write_recency_for_promote': 0, 
        #           'hit_set_grade_decay_rate': 0, 'hit_set_search_last_n': 0, 'grade_table': [], 'stripe_width': 0, 
        #           'expected_num_objects': 0, 'fast_read': False, 'options': {}, 'application_metadata': {'rbd': {}}, 
        #           'create_time': '2019-08-02 02:23:01.618519', 'last_change': '19', 'last_force_op_resend': '0', 
        #           'last_force_op_resend_prenautilus': '0', 'last_force_op_resend_preluminous': '0', 'removed_snaps': '[]'}]
        pools = self.mgr.get('osd_map')['pools']
        pool_map = dict()
        for pool in pools:
            pool_map[pool.get('pool_name')] = {k:pool.get(k) for k in interesting}
        return pool_map


    def fetch_osd_map(self, service_map):
        """Create an osd map"""
        stats = self.mgr.get('osd_stats')

        osd_map = dict()

        devices = self.mgr.get('osd_map_crush')['devices']
        for dev in devices:
            osd_id = str(dev['id']) 
            osd_map[osd_id] = dict(
                deviceclass=dev.get('class'),
                capacity=0,
                hostname=service_map['osd', osd_id]
                )
        
        for osd_stat in stats['osd_stats']:
            osd_id = str(osd_stat.get('osd'))
            osd_map[osd_id]['capacity'] = osd_stat['statfs']['total']

        return osd_map

    def push_events(self, changes):
        """Add config change to the global queue to generate an event in kubernetes"""
        for change in changes:
            event_queue.put(change)

    def send_hearbeat_msg(self):
        """Send a health heartbeat event"""
        if self.num_osds > 0 and self.raw_capacity == 0:
            # we have osds, but the stats aren't available yet, so skip the heartbeat
            return

        health_str = self.mgr.get('health')['json']
        if json.loads(health_str)['status'] == 'HEALTH_OK':
            health_text = 'Healthy'
            level = 'INF'
        else:
            health_text = 'Unhealthy'
            level = 'WRN'

        health_msg = ("Cluster state: {}. {} host{}, {} pool{}, {} OSDs. Raw "
                      "Capacity {}B".format(health_text, self.num_servers, text_suffix(self.num_servers),
                                            self.num_pools, text_suffix(self.num_pools),
                                            self.num_osds, MgrModule.to_pretty_iec(self.raw_capacity)))

        event_queue.put(
            LogEntry(
                source="config",
                msg_type="heartbeat",
                msg=health_msg,
                level=level,
                tstamp=None
            )
        )

    def _generate_logentry(self, msg):
        return LogEntry(
                source="config",
                msg_type="config",
                msg=msg,
                level='INF',
                tstamp=None
        )
    
    def _check_hosts(self, server_map):
        changes = list()
        if set(self.server_map.keys()) == set(server_map.keys()):
            # no hosts changes
            pass
        else:
            # host changes detected, find out what
            diff = ListDiff(self.server_map.keys(), server_map.keys())
            host_msg = "Host '{}' has been {} the cluster"
            for new in diff.added:
                changes.append(self._generate_logentry(
                                    msg=host_msg.format(new, 'added to'))
                )

            for removed in diff.removed:
                changes.append(self._generate_logentry(
                                    msg=host_msg.format(removed, 'removed from'))
                )

        return changes

    def _check_osds(self,server_map, osd_map):
        changes = list()
        before_osds = list()
        for svr in self.server_map:
            before_osds.extend(self.server_map[svr].get('osd',[]))

        after_osds = list()
        for svr in server_map:
            after_osds.extend(server_map[svr].get('osd',[]))
        
        if set(before_osds) == set(after_osds):
            # no change in osd id's
            pass
        else:
            # osd changes detected
            osd_msg = "Ceph OSD '{}' ({} @ {}B) has been {} host {}"

            diff = ListDiff(before_osds, after_osds)
            for new in diff.added:
                changes.append(self._generate_logentry(
                                    msg=osd_msg.format(
                                            new, 
                                            osd_map[new]['deviceclass'],
                                            MgrModule.to_pretty_iec(osd_map[new]['capacity']),
                                            'added to',
                                            osd_map[new]['hostname']))
                )

            for removed in diff.removed:
                changes.append(self._generate_logentry(
                                    msg=osd_msg.format(
                                        removed,
                                        osd_map[removed]['deviceclass'],
                                        MgrModule.to_pretty_iec(osd_map[removed]['capacity']),
                                        'removed from',
                                        osd_map[removed]['hostname']))
                )

        return changes

    def _check_pools(self, pool_map):
        changes = list()

        # self.mgr.log.warning("DBG - pool map looks like {}".format(pool_map))
        if self.pool_map.keys() == pool_map.keys():
            # no pools added/removed
            pass
        else:
            # self.mgr.log.warning("[DBG] pool changes detected")
            # Pool changes
            diff = ListDiff(self.pool_map.keys(), pool_map.keys())
            pool_msg = "Pool '{}' has been {} the cluster"
            for new in diff.added:
                changes.append(self._generate_logentry(
                                    msg=pool_msg.format(new, 'added to'))
                )

            for removed in diff.removed:
                changes.append(self._generate_logentry(
                                    msg=pool_msg.format(removed, 'removed from'))
                )

        # check pool configuration changes
        for pool_name in pool_map:
            if not self.pool_map.get(pool_name, dict()):
                # pool didn't exist before so just skip the checks
                continue

            if pool_map[pool_name] == self.pool_map[pool_name]:
                # no changes - dicts match in key and value
                continue
            else:
                # determine the change and add it to the change list
                size_diff = pool_map[pool_name]['size'] - self.pool_map[pool_name]['size']
                if size_diff != 0:
                    if size_diff < 0:
                        msg = "Data protection level of pool '{}' reduced to {} copies".format(pool_name,
                                                                                            pool_map[pool_name]['size'])
                        level = 'WRN'
                    else:
                        msg = "Data protection level of pool '{}' increased to {} copies".format(pool_name,
                                                                                                pool_map[pool_name]['size'])
                        level = 'INF'

                    changes.append(LogEntry(source="config",
                                msg_type="config",
                                msg=msg,
                                level=level,
                                tstamp=None)
                                )

                if pool_map[pool_name]['min_size'] != self.pool_map[pool_name]['min_size']:
                    changes.append(LogEntry(source="config",
                                msg_type="config",
                                msg="Minimum acceptable number of replicas in pool '{}' has changed".format(pool_name),
                                level='WRN',
                                tstamp=None)
                                )

        return changes

    def get_changes(self, server_map, osd_map, pool_map):
        """Detect changes in maps between current observation and the last"""

        changes = list()

        changes.extend(self._check_hosts(server_map))
        changes.extend(self._check_osds(server_map, osd_map))
        changes.extend(self._check_pools(pool_map))

        # TODO
        # Could generate an event if a ceph daemon has moved hosts
        # (assumes the ceph metadata host information is valid - which may not be the case)

        return changes

    def run(self):
        self.mgr.log.warning("[INF] Ceph configuration watcher started")

        self.server_map, self.service_map = self.fetch_servers()
        self.pool_map = self.fetch_pools()
        # self.mgr.log.warning("[DBG] server map {}".format(self.server_map))
        # self.mgr.log.warning("[DBG] service map {}".format(self.service_map))
        self.osd_map = self.fetch_osd_map(self.service_map)
        # self.mgr.log.warning("[DBG] osd map {}".format(self.osd_map))

        ctr = 0
        
        while True:

            try:
                start_time = time.time()
                server_map, service_map = self.fetch_servers()
                pool_map = self.fetch_pools()
                osd_map = self.fetch_osd_map(service_map)

                changes = self.get_changes(server_map, osd_map, pool_map)
                if changes:
                    self.push_events(changes)

                self.osd_map = osd_map
                self.pool_map = pool_map
                self.server_map = server_map
                self.service_map = service_map

                checks_duration = time.time() - start_time
                if checks_duration > self.config_check_secs:
                    new_interval = self.config_check_secs * 2
                    self.mgr.log.warning("[WRN] k8sevents check interval warning. "
                                         "Current checks {}s, interval is {}. "
                                         "Increasing interval to {}".format(int(checks_duration),
                                                                            self.config_check_secs,
                                                                            new_interval))
                    self.config_check_secs = new_interval

                time.sleep(self.config_check_secs)    
                ctr += self.config_check_secs
                if ctr%self.heartbeat_interval_secs == 0:
                    ctr = 0
                    self.send_hearbeat_msg()
            except BaseException:
                self.health = "[ERR] Unexpected {} Exception @ line {}".format(sys.exc_info()[0].__name__, 
                                                                               sys.exc_info()[2].tb_lineno)
                break

        self.mgr.log.warning("[WRN] Ceph configuration watcher stopped")
        if self.health != 'OK':
            self.mgr.log.error(self.health)

class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "k8sevents namespace",
            "desc": "Show all Kuberenetes events from the rook-ceph namespace",
            "perm": "r"
        },
        {
            "cmd": "k8sevents ceph",
            "desc": "Show only Ceph related events tracked & sent to the kubernetes cluster",
            "perm": "r"
        }
    ]
    MODULE_OPTIONS = [
        {'name': 'log_level'},
        {'name': 'heartbeat_interval_secs'},
        {'name': 'config_check_secs'}
    ]

    def __init__(self, *args, **kwargs):
        self.run = True
        self.event_processor = None
        self.config_watcher = None
        self.ns_watcher = None
        super(Module, self).__init__(*args, **kwargs)

    def fetch_events(self):
        """Interface to expose current events to another mgr module"""
        # TODO
        return dict()

    def show_all_events(self):
        """Show all events we're holding from the ceph namespace - most recent 1st"""

        max_name_length = max([len(k) for k in self.ns_watcher.events])
        max_msg_length = max([len(self.ns_watcher.events[k].message) for k in self.ns_watcher.events])
        fmt = "{:<20}  {:>5}  {:<" + str(max_msg_length) + "}  {:<" + str(max_name_length) + "}\n"
        s = fmt.format("Last Seen", "Count", "Message", "Event Object Name")

        for event_name in sorted(self.ns_watcher.events, 
                                 key = lambda name: self.ns_watcher.events[name].last_timestamp,
                                 reverse=True):

            event = self.ns_watcher.events[event_name]

            s += fmt.format(
                    datetime.strftime(event.last_timestamp,"%Y/%m/%d %H:%M:%S"),
                    event.count,
                    event.message,
                    event_name
            )

        return 0, "", s

    def show_ceph_events(self):
        if len(self.event_processor.events.keys()) > 0:
            s = "\n".join(self.event_processor.events.keys())
        else:
            s = "No events\n"
        return 0, "", s

    def handle_command(self, inbuf, cmd):
        # TODO Should we implement dynamic options for the monitoring?
        if cmd["prefix"] == "k8sevents namespace":
            return self.show_all_events()
        elif cmd["prefix"] == "k8sevents ceph":
            return self.show_ceph_events()
        else:
            raise NotImplementedError(cmd["prefix"])

    @staticmethod
    def can_run():
        """Determine whether the pre-reqs for the module are in place"""

        if not kubernetes_imported:
            return False, "kubernetes python client is unavailable"
        return True, ""

    def log_callback(self, arg, line, channel, name, who, stamp_sec, stamp_nsec, seq, level, msg):
        """Receive the ceph log entry and add it to the global queue"""

        if sys.version_info[0] >= 3:
            channel = channel.decode('utf-8')

        # self.log.error("qsize={},line={},channel={},name={},who={},"
        #                "stamp_sec={},seq={},level={},msg={}".format(event_queue.qsize(),line,channel,name,who,stamp_sec,
        #                seq,level,msg))
        
        event_queue.put(
            LogEntry(
                source='log',
                msg_type=channel,
                msg=line.decode('utf-8'),
                level=level[1:-1].decode('utf-8'),
                tstamp=stamp_sec
            )
        )

    def watch_ceph_log(self):
        """Initiate the log_monitor2 call to mimic ceph -w behavior"""

        level = self.get_localized_module_option(
            'log_level', DEFAULT_LOG_LEVEL)

        # create a daemon thread for monitor_log2
        run_in_thread(self.rados.monitor_log2, level, self.log_callback, 0)

    def serve(self):

        self.event_processor = EventProcessor(logger=self.log)
        self.config_watcher = CephConfigWatcher(self)
        self.ns_watcher = NamespaceWatcher(logger=self.log)

        if self.event_processor.ok:
            self.log.warning("[INF] Ceph Log processor thread starting")
            self.event_processor.start()        # start log consumer thread
            self.log.warning("[INF] Ceph log watcher thread starting")
            self.watch_ceph_log()               # start the log producer thread
            self.log.warning("[INF] Ceph config watcher thread starting")
            self.config_watcher.start()
            self.log.warning("[INF] Rook-ceph namespace events watcher starting")
            self.ns_watcher.start()

            while True:
                # stay alive
                time.sleep(0.5)

        else:
            self.log.warning('[WRN] Unable to access kubernetes event API - check RBAC rules')
            self.log.warning("[WRN] k8sevents module exiting")
            self.run = False

    def shutdown(self):
        self.run = False
        self.log.error("Shutting down k8sevents module")
        self.event_processor.can_run = False

        if self._rados:
            self._rados.shutdown()
