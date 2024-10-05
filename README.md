# Cephadm-VDT, a customized version for Cephadm:

- This is a customized version for cephadm that use a .yaml file to create a new cluster.
- **Warning, this version is still in development, please don't use this in production environment!**

## Setting up guide:

### 1. Cloning the repository to your local machine:

```
git clone https://github.com/HieuNT-2306/ceph-vdt
cd ceph/src/cephadm
```
### 2. Install python compiler (if neccesary) and create a virtual enviroment:

```
sudo apt-get update
sudo apt-get install python3 python3-venv python3-pip
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies:

```
kubectl apply -f -
apiVersion: pkg.crossplane.io/v1
kind: Provider
metadata:
  name: linode-provider-ceph
spec:
  package: xpkg.upbound.io/linode/provider-ceph:v0.0.48
```

### 4. Running Cephadm-vdt through python3:
- Before using cephadm, you need to create a .yaml file, example:
```helm repo add crossplane-stable https://charts.crossplane.io/stable
#Bootstrap:
image: 
docker: false
data_dir: "/var/lib/ceph"
log_dir: "/var/log/ceph"
logrotate_dir: "/etc/logrotate.d"
sysctl_dir: "/etc/sysctl.d"
unit_dir: "/etc/systemd/system"
verbose: false
log_dest: 
timeout: 
#fsid: "d1eab4ce-74cd-11ef-8ba1-b9c479b5724c"
retry: 15
mon_ip: 172.26.224.220
initial_dashboard_user: admin
initial_dashboard_password: passowrd
ssl_dashboard_port: 8848
ssh_user: root
skip_mon_network: false
skip_dashboard: false
dashboard_password_noupdate: false
no_minimize_config: false
skip_ping_check: false
skip_pull: false
skip_firewalld: false
allow_overwrite: false
no_cleanup_on_failure: false
allow_fqdn_hostname: false
allow_mismatched_release: false
skip_prepare_host: false
orphan_initial_daemons: false
skip_monitoring_stack: true
with_centralized_logging: false
apply_spec: 
shared_ceph_folder: 
registry_url: 
registry_username: 
registry_password: 
registry_json: 
container_init: true
cluster_network: 
single_host_defaults: false
log_to_file: false
deploy_cephadm_agent: false
custom_prometheus_alerts: 
force: true
keep_logs: false
zap_osds: true
name: 
keyring: 
mount: 
funcs: 
  - "command_prepare_host"
  - "command_rm_cluster"
  - "command_bootstrap"
  - "command_shell"

#Setting up cluster
setup: true
hosts:
- name: host1
  ipaddresses: 172.26.224.220
  label: _admin,mgr,mon,osd,rgw 
 
services:
  monitor:
    count-per-host: 1
  manager:
    count-per-host: 1
  radosgw:
    - name: public
      port: 8888
      count-per-host: 1
    - name: private
      port: 8889
      count-per-host: 1
volume: []
no_hosts: false
dry_run: false
```
- Afterthat, apply these files with cephadm-vdt:
```
python3 -m cephadm-vdt test.yaml
```

- OR, you can use the normal cephadm with:
```
python3 -m cephadm -h
```





