
"""
This module implements classes that wrap Rook's Ceph-related
logical entities.  These classes take a reference to a Kubernetes
API object (from the ``kubernetes`` module).
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


ROOK_SYSTEM_NS = "rook-system"
ROOK_API_VERSION = "v1alpha1"


log = logging.getLogger('rook_cluster')


class RookCluster(object):
    def __init__(self, k8s, cluster_name):
        self.cluster_name = cluster_name
        self.k8s = k8s

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
                "apiVersion": "rook.io/%s" % ROOK_API_VERSION,
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
        prefix = "/apis/rook.io/%s/namespaces/%s/" % (
                ROOK_API_VERSION, self.cluster_name)
        return urlparse.urljoin(prefix, path)

    def rook_api_call(self, verb, path, **kwargs):
        full_path = self.rook_url(path)
        log.debug("[%s] %s" % (verb, full_path))

        return self.k8s.api_client.call_api(
                full_path,
                verb,
                response_type="object",
                _return_http_data_only=True,
                _preload_content=True,
                **kwargs)

    def rook_api_get(self, path, **kwargs):
        return self.rook_api_call("GET", path, **kwargs)

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


class RookFilesystem(object):
    def __init__(self, cluster, fs_name):
        self.cluster = cluster
        self.fs_name = fs_name

    @property
    def k8s(self):
        return self.cluster.k8s

    def rook_enable(self):
        """
        Create a Rook configuration for this CephFS filesystem, such
        that Rook will run MDS containers for the filesystem.
        """
        # Check if Rook is already aware of this cluster
        try:
            r = self.cluster.rook_api_get("clusters/%s" % self.cluster.cluster_name)
        except ApiException:
            # FIXME: catch more specific error
            log.warn("Cluster not found in Rook: creating Ceph-managed cluster template")
            self.init_rook()
        else:
            log.info("Cluster found in rook.  Checking...")

            # If it has monCount > 0, it's a Rook-managed cluster, and
            # we should not be here, throw an error.

            # Else, we're all good, proceed.

            #monCount = 

    def rook_status(self):
        # Go query the k8s API about deployment, containers related to this
        # filesystem

        # Label filter is rook_cluster=<cluster name>
        #                 rook_file_system=<self.fs_name>

        #deployment = 
        #pods = 

        # Inspect the Rook YAML, to decide whether this filesystem
        # is Ceph-managed or Rook-managed

        # For those not understanding the details of all this, let's
        # generate a summary that just says whether the containers
        # are up and running
        # summary = {

        pods = self.k8s.list_namespaced_pod(
                "ceph", label_selector="rook_cluster=ceph,rook_file_system=%s" % self.fs_name)

        #import json
        #print json.dumps(pods.items[0])

        pods_summary = []

        for p in pods.items:
            d = p.to_dict()
            #p['metadata']['creationTimestamp']
            #p['metadata']['nodeName']
            pods_summary.append({
                "name": d['metadata']['name'],
                "nodename": d['spec']['node_name']
                })
            pass

        summary = {
                'pods': pods_summary
                }
        return {
                'summary': summary
                }

    def rook_teardown(self):
        # Call this before removing a Ceph filesystem
        pass
