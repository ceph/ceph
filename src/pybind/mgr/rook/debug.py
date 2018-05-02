
import logging
import json

from kubernetes import client, config

from rook_cluster import RookCluster, RookFilesystem

log = logging.getLogger("rook_cluster")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

class FSSync(object):
    def __init__(self):
        pass

    def update_rook(self):
        # For all filesystems:

        # - sync the max_mds setting to the rook continer count?
        pass

    # TODO watch containers to see when a pod is killed
    # and issue an "mds fail" when that happens

# What should creation flow look like?
#  - Persist command structure specifying both Ceph and Rook config first,
#    then safely write both out?

# Hey developer, you did write ~/.kube/config, right? - jcsp
config.load_kube_config();
# So that I can do port forwarding from my workstation - jcsp
from kubernetes.client import configuration
configuration.verify_ssl = False

k8s = client.CoreV1Api()

cluster_name = "ceph"
fs_name = "foofs"
rcluster = RookCluster(k8s, cluster_name)

if __name__ == "__main__":
    #rfs.rook_enable()

    rfs = RookFilesystem(rcluster, fs_name)
    print json.dumps(rfs.rook_status(), indent=2)


