"""
This module wrap's Rook + Kubernetes APIs to expose the calls
needed to implement an orchestrator module.  While the orchestrator
module exposes an async API, this module simply exposes blocking API
call methods.

This module is runnable outside of ceph-mgr, useful for testing.
"""

import urlparse
import logging
import json

# Optional kubernetes imports to enable MgrModule.can_run
# to behave cleanly.
try:
    from kubernetes.client.rest import ApiException
except ImportError:
    ApiException = None

ROOK_SYSTEM_NS = "rook-ceph-system"
ROOK_API_VERSION = "v1alpha1"
ROOK_API_NAME = "ceph.rook.io/%s" % ROOK_API_VERSION

log = logging.getLogger('rook')


class ApplyException(Exception):
    """
    For failures to update the Rook CRDs, usually indicating
    some kind of interference between our attempted update
    and other conflicting activity.
    """


class RookCluster(object):
    def __init__(self, k8s, cluster_name):
        self.cluster_name = cluster_name
        self.k8s = k8s

    @property
    def rook_namespace(self):
        # For the moment, assume Rook NS always equal to cluster name
        # (this is also assumed some places in Rook source, may
        #  be formalized at some point)
        return self.cluster_name

    def init_rook(self):
        """
        Create a passive Rook configuration for this Ceph cluster.  This
        will prompt Rook to start watching for other resources within
        the cluster (e.g. Filesystem CRDs), but no other action will happen.
        """

        # TODO: complete or remove this functionality: if Rook wasn't
        # already running, then we would need to supply it with
        # keys and ceph.conf as well as creating the cluster CRD

        cluster_crd = {
            "apiVersion": ROOK_API_NAME,
            "kind": "Cluster",
            "metadata": {
                "name": self.cluster_name,
                "namespace": self.cluster_name
            },
            "spec": {
                "backend": "ceph",
                "hostNetwork": True
            }
        }

        self.rook_api_post("clusters", body=cluster_crd)

    def rook_url(self, path):
        prefix = "/apis/ceph.rook.io/%s/namespaces/%s/" % (
            ROOK_API_VERSION, self.rook_namespace)
        return urlparse.urljoin(prefix, path)

    def rook_api_call(self, verb, path, **kwargs):
        full_path = self.rook_url(path)
        log.debug("[%s] %s" % (verb, full_path))

        return self.k8s.api_client.call_api(
            full_path,
            verb,
            auth_settings=['BearerToken'],
            response_type="object",
            _return_http_data_only=True,
            _preload_content=True,
            **kwargs)

    def rook_api_get(self, path, **kwargs):
        return self.rook_api_call("GET", path, **kwargs)

    def rook_api_patch(self, path, **kwargs):
        return self.rook_api_call("PATCH", path,
                                  header_params={"Content-Type": "application/json-patch+json"},
                                  **kwargs)

    def rook_api_post(self, path, **kwargs):
        return self.rook_api_call("POST", path, **kwargs)

    def get_discovered_devices(self, nodenames=None):
        # TODO: replace direct k8s calls with Rook API calls
        # when they're implemented
        label_selector = "app=rook-discover"
        if nodenames is not None:
            # FIXME: is there a practical or official limit on the
            # number of entries in a label selector
            label_selector += ", rook.io/node in ({0})".format(
                ", ".join(nodenames))

        try:
            result = self.k8s.list_namespaced_config_map(
                ROOK_SYSTEM_NS,
                label_selector=label_selector)
        except ApiException as e:
            log.warn("Failed to fetch device metadata: {0}".format(e))
            raise

        nodename_to_devices = {}
        for i in result.items:
            drives = json.loads(i.data['devices'])
            nodename_to_devices[i.metadata.labels['rook.io/node']] = drives

        return nodename_to_devices

    def describe_pods(self, service_type, service_id):
        # Go query the k8s API about deployment, containers related to this
        # filesystem

        # Inspect the Rook YAML, to decide whether this filesystem
        # is Ceph-managed or Rook-managed
        # TODO: extend Orchestrator interface to describe whether FS
        # is manageable by us or not

        # Example Rook Pod labels for a mgr daemon:
        # Labels:         app=rook-ceph-mgr
        #                 pod-template-hash=2171958073
        #                 rook_cluster=rook
        # And MDS containers additionally have `rook_filesystem` label

        # Label filter is rook_cluster=<cluster name>
        #                 rook_file_system=<self.fs_name>

        label_filter = "rook_cluster={0},app=rook-ceph-{1}".format(
            self.cluster_name, service_type)
        if service_type == "mds":
            label_filter += ",rook_file_system={0}".format(service_id)
        elif service_type == "osd":
            # Label added in https://github.com/rook/rook/pull/1698
            label_filter += ",ceph-osd-id={0}".format(service_id)
        elif service_type == "mon":
            # label like mon=rook-ceph-mon0
            label_filter += ",mon={0}".format(service_id)
        elif service_type == "mgr":
            # TODO: get Rook to label mgr pods
            pass
        elif service_type == "rgw":
            # TODO: rgw
            pass

        pods = self.k8s.list_namespaced_pod(
            self.rook_namespace,
            label_selector=label_filter)

        # import json
        # print json.dumps(pods.items[0])

        pods_summary = []

        for p in pods.items:
            d = p.to_dict()
            # p['metadata']['creationTimestamp']
            # p['metadata']['nodeName']
            pods_summary.append({
                "name": d['metadata']['name'],
                "nodename": d['spec']['node_name'],
                "labels": d['metadata']['labels']
            })
            pass

        return pods_summary

    def add_filesystem(self, spec):
        # TODO use spec.placement
        # TODO use spec.min_size (and use max_size meaningfully)
        # TODO warn if spec.extended has entries we don't kow how
        #      to action.

        rook_fs = {
            "apiVersion": ROOK_API_NAME,
            "kind": "Filesystem",
            "metadata": {
                "name": spec.name,
                "namespace": self.rook_namespace
            },
            "spec": {
                "preservePoolsOnRemove": True,
                "skipPoolCreation": True,
                "metadataServer": {
                    "activeCount": spec.max_size,
                    "activeStandby": True

                }
            }
        }

        try:
            self.rook_api_post(
                "filesystems/",
                body=rook_fs
            )
        except ApiException as e:
            if e.status == 409:
                log.info("Filesystem '{0}' already exists".format(spec.name))
                # Idempotent, succeed.
            else:
                raise

    def can_create_osd(self):
        current_cluster = self.rook_api_get(
            "clusters/{0}".format(self.cluster_name))
        use_all_nodes = current_cluster['spec'].get('useAllNodes', False)

        # If useAllNodes is set, then Rook will not be paying attention
        # to anything we put in 'nodes', so can't do OSD creation.
        return not use_all_nodes

    def node_exists(self, node_name):
        try:
            self.k8s.read_node(node_name)
        except ApiException as e:
            if e.status == 404:
                return False
            else:
                raise
        else:
            return True

    def add_osds(self, spec):
        """
        Rook currently (0.8) can only do single-drive OSDs, so we
        treat all drive groups as just a list of individual OSDs.
        """
        # assert isinstance(spec, orchestrator.OsdSpec)

        block_devices = spec.drive_group.devices

        assert spec.format in ("bluestore", "filestore")

        # The CRD looks something like this:
        #     nodes:
        #       - name: "gravel1.rockery"
        #         devices:
        #          - name: "sdb"
        #         storeConfig:
        #           storeType: bluestore

        current_cluster = self.rook_api_get(
            "clusters/{0}".format(self.cluster_name))

        patch = []

        # FIXME: this is all not really atomic, because jsonpatch doesn't
        # let us do "test" operations that would check if items with
        # matching names were in existing lists.

        if 'nodes' not in current_cluster['spec']['storage']:
            patch.append({
                'op': 'add', 'path': '/spec/storage/nodes', 'value': []
            })

        current_nodes = current_cluster['spec']['storage'].get('nodes', [])

        if spec.node not in [n['name'] for n in current_nodes]:
            patch.append({
                "op": "add", "path": "/spec/storage/nodes/-", "value": {
                    "name": spec.node,
                    "devices": [{'name': d} for d in block_devices],
                    "storeConfig": {
                        "storeType": spec.format
                    }
                }
            })
        else:
            # Extend existing node
            node_idx = None
            current_node = None
            for i, c in enumerate(current_nodes):
                if c['name'] == spec.node:
                    current_node = c
                    node_idx = i
                    break

            assert node_idx is not None
            assert current_node is not None

            new_devices = list(set(block_devices) - set([d['name'] for d in current_node['devices']]))

            for n in new_devices:
                patch.append({
                    "op": "add",
                    "path": "/spec/storage/nodes/{0}/devices/-".format(node_idx),
                    "value": {'name': n}
                })

        if len(patch) == 0:
            log.warning("No-op adding stateful service")
            return

        try:
            self.rook_api_patch(
                "clusters/{0}".format(self.cluster_name),
                body=patch)
        except ApiException as e:
            log.exception("API exception: {0}".format(e.message))
            raise ApplyException(
                "Failed to create OSD entries in Cluster CRD: {0}".format(
                    e.message))
