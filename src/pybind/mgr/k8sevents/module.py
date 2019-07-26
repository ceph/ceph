# capture ceph cluster changes and events and push them to the kubernetes
# events API
#
# This module requires an rbac policy in kubernetes to allow the mgr service
# to interact with the events api endpoints
# - apiGroups:
#  - ""
#  resources:
#  - events
#  verbs:
#  - create
#  - update

# LogEntry class dynamically builds an object from a dict, so we need to 
# disable the linter for the no-member error
# pylint: disable=no-member


import os
import sys
import time
import json
import threading

from datetime import datetime, timedelta, tzinfo
from urllib3.exceptions import MaxRetryError

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
    from kubernetes import client, config
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

ZERO = timedelta(0)


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO

class HealthCheck(object):
    def __init__(self, msg):
        """ transform a healthcheck msg into it's component parts """
        # msg looks like
        # 2019-07-17 02:59:12.714672 mon.a [WRN] Health check failed: Reduced data availability: 100 pgs inactive (PG_AVAILABILITY)
        # 2019-07-17 03:16:39.103929 mon.a [INF] Health check cleared: OSDMAP_FLAGS (was: nodown flag(s) set)

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
                self.text = ' '.join(msg_tokens[3:-2])
                self.name = tokens[-1][1:-1]


class LogEntry(object):
    """ Log Object for Ceph log records """
    reason_map = {
        "audit": "ConfigChange",
        "cluster": "HealthCheck"
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
        """ Look at the msg string and extract the command content """

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
        if self.msg_type == 'audit':
            return 'ceph-mgr.audit.{}'.format(self.cmd).replace(' ', '_')
        
        elif self.healthcheck:
            return 'ceph-mgr.health.{}'.format(self.healthcheck.name)
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
            return None


class KubernetesEvent(object):
    pod_name = os.environ['POD_NAME']
    host = os.environ['NODE_NAME']
    namespace = os.environ.get('POD_NAMESPACE', 'rook-ceph')
    cluster_name = os.environ.get('ROOK_CEPH_CLUSTER_CRD_NAME', 'rook-ceph')
    api = client.CoreV1Api()

    def __init__(self, name, msg, msg_type, msg_reason, unique_name=True):
        self.unique_name = unique_name
        self.event_name = name
        self.event_msg = msg
        self.event_type = msg_type
        self.event_reason = msg_reason
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

        obj_ref = client.V1ObjectReference(kind="CephCluster", 
                                           name=self.event_name, 
                                           namespace=self.namespace)
        event_source = client.V1EventSource(component="ceph-mgr", 
                                            host=self.host)
        return  client.V1Event(involved_object=obj_ref, 
                               metadata=obj_meta, 
                               message=self.event_msg, 
                               count=self.count, 
                               type=self.event_type,
                               reason=self.event_reason,
                               source=event_source, 
                               first_timestamp=self.first_timestamp,
                               last_timestamp=self.last_timestamp)

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

    def update(self, msg=None):
        if msg:
            self.event_msg = msg
        
        self.last_timestamp = datetime.now(UTC())
        self.count += 1

        try:
            self.api.patch_namespaced_event(self.event_name, self.namespace, self.body)
        except ApiException as e:
            if e.status == 404:
                # tried to patch but it's rolled out of k8s events
                try:
                    self.api.create_namespaced_event(self.namespace, self.event_body)
                except ApiException as e:
                    self.api_status = e.status
                else:
                    self.api_status = 200
        else:
            self.api_status = 200 


class EventProcessor(threading.Thread):
    daemon = True
    can_run = True

    def __init__(self, logger):
        self.events = dict()
        self.log = logger
        threading.Thread.__init__(self)

    def startup(self):
        event = KubernetesEvent(name="ceph-mgr.k8sevent-module",
                                msg="Ceph log -> event tracking started",
                                msg_type="Normal",
                                msg_reason="Started",
                                unique_name=False)
        event.write()
        return event.api_success
    
    @property
    def ok(self):
        return self.startup()

    def process(self, log_object):
        
        self.log.warning("processing {}".format(str(log_object)))
        # self.log.warning("name would be {}".format(log_object.event_name))
        event_out = False

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
            if log_object.healthcheck:
                self.log.warning("cluster health check message detected")
                event_out = True
        else:
            # unknown msgtype?
            pass

        if event_out:
            # TODO
            # Why track the events? Just attempt a write, and if it fails update it
            if log_object.event_name in self.events.keys():
                self.log.warning("need to update an event")
                self.events[log_object.event_name].update(log_object.event_msg)
            else:
                self.log.warning("adding an event")
                event = KubernetesEvent(name=log_object.event_name,
                                        msg=log_object.event_msg,
                                        msg_type=log_object.event_type,
                                        msg_reason=log_object.event_reason)
                event.write()
                if event.api_success:
                    self.events[log_object.event_name] = event
        else:
            self.log.warning("ignoring log message {}".format(log_object.msg))

    def run(self):
        self.log.warning("[INF] Ceph Log processor thread started")
        while True:

            try:
                log_object = event_queue.get(block=False)
            except queue.Empty:
                pass
            else:
                self.process(log_object)
            
            if not self.can_run:
                break

            time.sleep(0.5)

        self.log.error("[INF] Ceph log processor thread exited")

class CephConfigWatcher(threading.Thread):
    """ Catch configuration changes within the cluster and generate events """
    daemon = True

    def __init__(self, mgr):
        self.mgr = mgr
        self.server_map = dict()
        self.osd_map = dict()
        self.service_lookup = dict()
        threading.Thread.__init__(self)
    
    @property
    def raw_capacity(self):
        return sum([self.osd_map[osd]['capacity'] for osd in self.osd_map])
    
    @property
    def num_servers(self):
        return len(self.server_map.keys())

    @property
    def num_osds(self):
        return len(self.osd_map.keys())

    def fetch_servers(self):
        servers = self.mgr.list_servers()
        server_map = dict()
        for server_info in servers:
            service_map = dict()
            for svc in server_info['services']:
                if svc.get('type') in service_map.keys():
                    service_map[svc.get('type')].append(svc.get('id'))
                else:
                    service_map[svc.get('type')] = list([svc.get('id')])
                # maintain the service xref map service -> host and version
                self.service_lookup[(svc.get('type'), str(svc.get('id')))] = server_info.get('hostname')
            server_map[server_info.get('hostname')] = service_map

        return server_map
    
    def fetch_osd_map(self):
        stats = self.mgr.get('osd_stats')
        osd_map = dict()

        devices = self.mgr.get('osd_map_crush')['devices']
        for dev in devices:
            osd_id = str(dev['id']) 
            osd_map[osd_id] = dict(
                deviceclass=dev.get('class'),
                capacity=0,
                hostname=self.service_lookup['osd', osd_id]
                )
        
        for osd_stat in stats['osd_stats']:
            osd_id = str(osd_stat.get('osd'))
            osd_map[osd_id]['capacity'] = osd_stat['statfs']['total']

        return osd_map

    def run(self):
        self.mgr.log.warning("[INF] Ceph configuration watcher started")
        # TODO
        # 1. every hour send a health event - ok/warning/error
        # 2. send event if nodes added/removed from the cluster
        # 3. send event if osd count increases/decreases
        # 4. send event if service location changes
        self.server_map = self.fetch_servers()
        self.mgr.log.warning("[DBG] {}".format(self.service_lookup))
        self.osd_map = self.fetch_osd_map()
        self.mgr.log.warning("[INF] {} hosts, {} OSDs, {}B Raw Capacity".format(self.num_servers, 
                                                                               self.num_osds,
                                                                               MgrModule.to_pretty_iec(self.raw_capacity)))
        self.mgr.log.warning("[DBG] server map : {}".format(self.server_map))
        self.mgr.log.warning("[DBG] osd map : {}".format(self.osd_map))

        while True:
            self.server_map = self.fetch_servers()
            self.osd_map = self.fetch_osd_map()

            self.mgr.log.error("[DBG] service map : {}".format(self.server_map))
            self.mgr.log.error("[DBG] osd_map : {}".format(self.osd_map))
            self.mgr.log.warning("[DBG] {} hosts, {} OSDs, {}B Raw Capacity".format(self.num_servers, 
                                                                        self.num_osds,
                                                                        MgrModule.to_pretty_iec(self.raw_capacity)))
            time.sleep(10)    


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "dumpevents",
            "desc": "Show the vars created",
            "perm": "r"
        }
    ]
    MODULE_OPTIONS = [
        {'name': 'log_level'}
    ]

    def __init__(self, *args, **kwargs):
        self.run = True
        self.event_processor = None
        self.config_watcher = None
        super(Module, self).__init__(*args, **kwargs)

    def handle_command(self, inbuf, cmd):
        return 0, json.dumps(self.event_processor.events), ""

    @staticmethod
    def can_run():
        """ Determine whether the pre-reqs for the module are in place """

        if not kubernetes_imported:
            return False, "kubernetes python client is unavailable"
        return True, ""

    def log_callback(self, arg, line, channel, name, who, stamp_sec, stamp_nsec, seq, level, msg):
        """ Receive the ceph log entry and add it to the global queue """

        if sys.version_info[0] >= 3:
            channel = channel.decode('utf-8')

        # TODO DEBUG ONLY LINE
        self.log.error("qsize={},line={},channel={},name={},who={},"
                       "stamp_sec={},seq={},level={},msg={}".format(event_queue.qsize(),line,channel,name,who,stamp_sec,
                       seq,level,msg))
        
        log_object = LogEntry(
            source='log',
            msg_type=channel,
            msg=line.decode('utf-8'),
            level=level[1:-1].decode('utf-8'),
            tstamp=stamp_sec
        )

        event_queue.put(log_object)

    def watch_ceph_log(self):
        """ Initiate the log_monitor2 call to mimic ceph -w behavior """

        level = self.get_localized_module_option(
            'log_level', DEFAULT_LOG_LEVEL)

        # thread established is a daemon thread
        run_in_thread(self.rados.monitor_log2, level, self.log_callback, 0)

    def serve(self):

        self.event_processor = EventProcessor(logger=self.log)
        self.config_watcher = CephConfigWatcher(self)
        if self.event_processor.ok:
            self.log.warning("[INF] Ceph Log processor thread starting")
            self.event_processor.start()        # start log consumer thread
            self.log.warning("[INF] Ceph log watcher thread starting")
            self.watch_ceph_log()               # start the log producer thread
            self.log.warning("[INF] Ceph config watcher thread starting")
            self.config_watcher.start()

            while True:

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
