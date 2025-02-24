# Building the ceph-grafana container image
From Nautilus onwards, grafana is embedded into the mgr/dashboard UI and uses two discrete grafana plugins to provide visualisations within the UI. To better support disconnected installs, and provide a more tested configuration you may use the Makefile, in this directory, to (re)generate the grafana containers based on each Ceph release.

The versions of grafana, and the plugins are defined in the script so testing can be done against a known configuration.  

## Container
The current implementation uses buildah with a CentOS8 base image. 

## Dependencies
Ensure you have the following dependencies installed on your system, before attempting to build the image(s)
- podman or docker
- buildah  
- jq
- make

## Build Process
The Makefile supports the following invocations;
```
# make                         <-- create container with dashboards from master 
# make all
# make ceph_version=octopus
# make ceph_version=nautilus
```

Once complete, a ```make all``` execution will provide the following containers on your system.
```
# podman images
REPOSITORY                    TAG        IMAGE ID       CREATED          SIZE
localhost/ceph/ceph-grafana   master     606fa5444fc6   14 minutes ago   497 MB
localhost/ceph-grafana        master     606fa5444fc6   14 minutes ago   497 MB
localhost/ceph-grafana        octopus    580b089c14e7   15 minutes ago   497 MB
localhost/ceph/ceph-grafana   octopus    580b089c14e7   15 minutes ago   497 MB
localhost/ceph-grafana        nautilus   3c91712dd26f   17 minutes ago   497 MB
localhost/ceph/ceph-grafana   nautilus   3c91712dd26f   17 minutes ago   497 MB
registry.centos.org/centos    8          29d8fb6c94af   30 hours ago     223 MB

```
