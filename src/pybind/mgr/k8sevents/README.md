# Testing

## To test the k8sevents module  
enable the module with `ceph mgr module enable k8sevents`  
check that it's working `ceph k8sevents status`, you should see something like this;  
```
[root@ceph-mgr ~]# ceph k8sevents status
Kubernetes
- Hostname : https://localhost:30443
- Namespace: ceph
Tracker Health
- EventProcessor : OK
- CephConfigWatcher : OK
- NamespaceWatcher : OK
Tracked Events
- namespace  :   5
- ceph events:   0

```  
Now run some commands to generate healthchecks and admin level events;  
- ```ceph osd set noout```
- ```ceph osd unset noout```
- ```ceph osd pool create mypool 4 4 replicated```
- ```ceph osd pool delete mypool mypool --yes-i-really-really-mean-it```  

In addition to tracking audit, healthchecks and configuration changes if you have the environment up for >1 hr you should also see and event that shows the clusters health and configuration overview.

As well as status, you can use k8sevents to see event activity in the target kubernetes namespace
```
[root@rhcs4-3 kube]# ceph k8sevents ls 
Last Seen (UTC)       Type      Count  Message                                              Event Object Name
2019/09/20 04:33:00   Normal        1  Pool 'mypool' has been removed from the cluster      mgr.ConfigurationChangeql2hj
2019/09/20 04:32:55   Normal        1  Client 'client.admin' issued: ceph osd pool delete   mgr.audit.osd_pool_delete_
2019/09/20 04:13:23   Normal        2  Client 'mds.rhcs4-2' issued: ceph osd blacklist      mgr.audit.osd_blacklist_
2019/09/20 04:08:28   Normal        1  Ceph log -> event tracking started                   mgr.k8sevents-moduleq74k7
Total :   4
```  
or, focus on the ceph specific events(audit & healthcheck) that are being tracked by the k8sevents module.
```
[root@rhcs4-3 kube]# ceph k8sevents ceph
Last Seen (UTC)       Type      Count  Message                                              Event Object Name
2019/09/20 04:32:55   Normal        1  Client 'client.admin' issued: ceph osd pool delete   mgr.audit.osd_pool_delete_
2019/09/20 04:13:23   Normal        2  Client 'mds.rhcs4-2' issued: ceph osd blacklist      mgr.audit.osd_blacklist_
Total :   2
```

## Sending events from a standalone Ceph cluster to remote Kubernetes cluster
To test interaction from a standalone ceph cluster to a kubernetes environment, you need to make changes on the kubernetes cluster **and** on one of the mgr hosts.
### kubernetes (minikube)
We need some basic RBAC in place to define a serviceaccount(and token) that we can use to push events into kubernetes. The `rbac_sample.yaml` file provides a quick means to create the required resources. Create them with `kubectl create -f rbac_sample.yaml`
  
Once the resources are defined inside kubernetes, we need a couple of things copied over to the Ceph mgr's filesystem.
### ceph admin host
We need to run some commands against the cluster, so you'll needs access to a ceph admin host. If you don't have a dedicated admin host, you can use a mon or mgr machine. We'll need the root ca.crt of the kubernetes API, and the token associated with the service account we're using to access the kubernetes API.  

1. Download/fetch the root ca.crt for the kubernetes cluster (on minikube this can be found at ~/minikube/ca.crt)
2. Copy the ca.crt to your ceph admin host
3. Extract the token from the service account we're going to use
```
kubectl -n ceph get secrets -o jsonpath="{.items[?(@.metadata.annotations['kubernetes\.io/service-account\.name']=='ceph-mgr')].data.token}"|base64 -d > mytoken
```  
4. Copy the token to your ceph admin host
5. On the ceph admin host, enable the module with `ceph mgr module enable k8sevents`
6. Set up the configuration
```
ceph k8sevents set-access cacrt -i <path to ca.crt file>
ceph k8sevents set-access token -i <path to mytoken>
ceph k8sevents set-config server https://<kubernetes api host>:<api_port>
ceph k8sevents set-config namespace ceph
```
7. Restart the module with `ceph mgr module disable k8sevents && ceph mgr module enable k8sevents`
8. Check state with the `ceph k8sevents status` command
9. Remove the ca.crt and mytoken files from your admin host

To remove the configuration keys used for external kubernetes access, run the following command
```
ceph k8sevents clear-config  
```

## Networking
You can use the above approach with a minikube based target from a standalone ceph cluster, but you'll need to have a tunnel/routing defined from the mgr host(s) to the minikube machine to make the kubernetes API accessible to the mgr/k8sevents module. This can just be a simple ssh tunnel.    
