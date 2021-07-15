# Teuthology Development Environment Guide

This is a brief guide how to setup teuthology development environment
on your laptop (desktop if you wish). Though everything in this guide
can be implemented as one handy script, some more details how things
work can be helpful to document.

## Introduction

Teuthology consists from the following components:

teuthology - the core framework which can run a job,
the config file which describes test environment
and task list to execute.

- paddles - a database and the api
- pulpito - web gui for paddles
- beanstalkd - the job queue

The teuthology core includes following main tools:
- teuthology-suite
- teuthology-schedule
- teuthology-worker
- teuthology (formerly teuthology-run).
- teuthology-lock - allows to lock and provision nodes
  separately from run.

## Docker

Though paddles and pulpito can be run as services using supervisord
it is often useful to have them isolated in a container.
There can be used any of available tools, but here are example for
bare docker.

### Start docker and add shared network

Add your user to docker group and start the service:

```bash
sudo usermod -aG docker $USER
sudo service docker start
```

Create paddles network for container interaction:

```bash
docker network create paddles
```

### Run postgres

Start postgres containers in order to use paddles:

```bash
mkdir $HOME/.teuthology/postgres
docker run -d -p 5432:5432 --network paddles --name paddles-postgres \
    -e POSTGRES_PASSWORD=secret \
    -e POSTGRES_USER=paddles \
    -e POSTGRES_DB=paddles \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -v $HOME/.teuthology/postgres:/var/lib/postgresql/data postgres
```

### Run paddles

Checkout paddles and build the image:

```bash
cd ~/paddles && docker build . --file Dockerfile --tag paddles
```

Run the container with previously created network:

```bash
docker run -d --network paddles --name api -p 80:8080 \
	-e PADDLES_SERVER_HOST=0.0.0.0 \
	-e PADDLES_SQLALCHEMY_URL=postgresql+psycopg2://paddles:secret@paddles-postgres/paddles \
	paddles
```

### Run pulpito

Checkout pulpito and build the image:

```bash
cd ~/pulpito && docker build . --file Dockerfile --tag pulpito
```

Run the container:

```bash
docker run -d --network paddles --name web -p 8081:8081 -e PULPITO_PADDLES_ADDRESS=http://api:8080 pulpito
```

NOTE. Restart pulpito container:

```bash
docker kill web ; docker container rm web
```

NOTE. You can check all listening ports by:

```bash
sudo lsof -i -P -n | grep LISTEN
```

NOTE. You can check database connection using:

```bash
psql -h localhost -U paddles -l
```

## Setup Libvirt for Downburst

Add libvirt host nodes:

```sql
insert into nodes (name, machine_type, is_vm, locked, up) values ('localhost', 'libvirt', false, true, true);
insert into nodes (name, machine_type, is_vm, locked, up, mac_address, vm_host_id) values ('target-00.local', 'vps', true, false, false, '52:54:00:00:00:00', (select id from nodes where name='localhost'));
insert into nodes (name, machine_type, is_vm, locked, up, mac_address, vm_host_id) values ('target-01.local', 'vps', true, false, false, '52:54:00:00:00:01', (select id from nodes where name='localhost'));
insert into nodes (name, machine_type, is_vm, locked, up, mac_address, vm_host_id) values ('target-02.local', 'vps', true, false, false, '52:54:00:00:00:02', (select id from nodes where name='localhost'));
insert into nodes (name, machine_type, is_vm, locked, up, mac_address, vm_host_id) values ('target-03.local', 'vps', true, false, false, '52:54:00:00:00:03', (select id from nodes where name='localhost'));
```
or just use the following command:

```bash
psql -h localhost -U paddles -d paddles < docs/laptop/targets.sql
```

Add libvirt config file so downburst able to use 'localhost' node to connect to:

```bash
cat > ~/.config/libvirt/libvirt.conf << END
uri_aliases = [
	'localhost=qemu:///system?no_tty=1',
]

END
```

Add your user to wheel group and allow to wheel group to passwordless access libvirt:

```bash

sudo usermod -a -G wheel $USER

```

Allow users in wheel group to manage the libvirt daemon without authentication:

```bash

sudo tee /etc/polkit-1/rules.d/50-libvirt.rules << END
polkit.addRule(function(action, subject) {
    if (action.id == "org.libvirt.unix.manage" &&
	subject.isInGroup("wheel")) {
	    return polkit.Result.YES;
    }
});

END

```

(Taken from: https://octetz.com/docs/2020/2020-05-06-linux-hypervisor-setup/)

Make sure libvirtd is running:

```bash
sudo service libvirtd start
```

NOTE. You can check you are able to access libvirt without password:

```bash

virsh -c qemu:///system list

```

Make sure libvirt front network exists, it can be defined as NAT and
include dhcp records for the target nodes:

```xml
<network>
  <name>front</name>
  <forward mode='nat'/>
  <bridge name='virbr1' stp='on' delay='0'/>
  <domain name='local' />
  <ip address='192.168.123.1' netmask='255.255.255.0'>
    <dhcp>
      <range start='192.168.123.2' end='192.168.123.99'/>
      <host mac="52:54:00:00:00:00" ip="192.168.123.100"/>
      <host mac="52:54:00:00:00:01" ip="192.168.123.101"/>
      <host mac="52:54:00:00:00:02" ip="192.168.123.102"/>
      <host mac="52:54:00:00:00:03" ip="192.168.123.103"/>
    </dhcp>
  </ip>
</network>

```
for example:

```bash
virsh -c qemu:///system net-define docs/laptop/front.xml

```

(for details, look https://jamielinux.com/docs/libvirt-networking-handbook/appendix/dhcp-host-entries.html)

Add corresponding records to your /etc/hosts:

```txt
192.168.123.100 target-00 target-00.local
192.168.123.101 target-01 target-01.local
192.168.123.102 target-02 target-02.local
192.168.123.103 target-03 target-03.local
```
you can take it from corresponding file:
```
sudo tee -a /etc/hosts < docs/laptop/hosts 
```

Make sure the front network is up:

```bash
sudo virsh net-start front
```

NOTE. The 'default' volume pool should be up and running before trying downburst or teuthology-lock.

```bash
> sudo virsh pool-list --all
 Name      State    Autostart
-------------------------------
 default   active   no
```


## Setup teuthology virtual environment


Checkout the teuthology core repo and run the bootstrap script:
```bash
git clone https://github.com/ceph/teuthology ~/teuthology
cd ~/teuthology && ./bootstrap
. virtualenv/bin/activate
```

By default the `./bootstrap` script is installing teuthology in development mode
to the `virtualenv` directory.

Create teuthology config file `~/.teuthology.yaml`:

```bash
cat > ~/.teuthology.yaml << END
# replace $HOME with whatever appropriate to your needs
# teuthology-lock
lab_domain: local
lock_server: http://localhost:80
default_machine_type: vps
# teuthology-run
results_server: http://localhost:80
# we do not need reserve_machines on localhost
reserve_machines: 0
# point to your teuthology
teuthology_path: $HOME/teuthology
# beanstalkd
queue_host: localhost
queue_port: 11300
# if you want make and test patches to ceph-cm-ansible
# ceph_cm_ansible_git_url: $HOME/ceph-cm-ansible
# customize kvm guests parameter
downburst:
  path: $HOME/downburst/virtualenv/bin/downburst
  # define discover_url if you need your custom downburst image server
  # discover_url: http://localhost:8181/images/ibs/
  machine:
    cpus: 2
    disk: 12G
    ram: 2G
    volumes:
      size: 8G
      count: 4
# add the next two if you do not use shaman
check_package_signatures: false
suite_verify_ceph_hash: false
END

```

List locks:

```bash
> teuthology-lock --brief --all
localhost       up   locked   None              "None"
target-00.local up   unlocked None              "None"
target-01.local up   unlocked None              "None"
target-02.local up   unlocked None              "None"
target-03.local up   unlocked None              "None"

```
Where the `localhost` is special purpose node where libvirt instance is running
and where the target nodes will be created.

Export the downburst discover url environment variable for your own image storage if required:

```bash
# cloud image location
export DOWNBURST_DISCOVER_URL=http://localhost:8181/images
```

NOTE. The step above is optional and is required if you are going to use custom image
location for the downburst, which is useful though when you want minimize traffic to
you computer. Refer [Create own discovery location](#create-own-discovery-location)
to know more how to create your private image storage.

Try to lock nodes now:

```bash
teuthology-lock -v --lock target-00 -m vps --os-type opensuse --os-version 15.2
teuthology-lock -v --lock-many 1 -m vps --os-type ubuntu --os-version 16.04
```

To initialize all targets you need to use `--lock` instead `--lock-many`
for the first time for each target.

(Note. It can be probably changed, but this is how it is recommended 
in teuthology adding nodes guide for the lab setup)

For further usage nodes should be unlocked with `--unlock` option.

### Run beanstalkd

For openSUSE there is no beanstalkd package as for Ubuntu, so it is needed to add corresponding repo:

```bash
zypper addrepo https://download.opensuse.org/repositories/filesystems:/ceph:/teuthology/openSUSE_Leap_15.2/x86_64/ teuthology && zypper ref
```

Install beanstalkd package and run the service:

```bash
sudo zypper in beanstalkd
sudo service beanstalkd start
```

### Run worker

Create archive and worker log directories and run the worker polling required tube.

```bash
TEUTH_HOME=$HOME/.teuthology
mkdir -p $TEUTH_HOME/www/logs/jobs
mkdir -p $TEUTH_HOME/www/logs/workers

teuthology-worker -v --tube vps --archive-dir $TEUTH_HOME/www/logs/jobs --log-dir $TEUTH_HOME/www/logs/workers
```

Schedule a dummy job:
```bash
teuthology-suite -v --ceph-repo https://github.com/ceph/ceph --suite-repo https://github.com/ceph/ceph --ceph octopus --suite dummy -d centos -D 8.2 --sha1 35adebe94e8b0a17e7b56379a8bf24e5f7b8ced4 --limit 1 -m vps -t refs/pull/1548/merge
```

## Downburst

Checkout downburst to your home, bootstrap virtualenv and enable it:
```bash
git clone https://github.com/ceph/downburst ~/downburst
pushd ~/downburst && ./bootstrap
```

### Create own discovery location

(This step is optional, use it if you want to use private image location.)

Create images directory, and download some images:

```bash
DATE=$(date +%Y%m%d)
mkdir -p $HOME/.teuthology/www/images
wget http://download.opensuse.org/distribution/leap/15.2/appliances/openSUSE-Leap-15.2-JeOS.x86_64-OpenStack-Cloud.qcow2 -O $HOME/.teuthology/www/images/opensuse-15.2-$DATE-cloudimg-amd64.img
wget http://download.opensuse.org/distribution/leap/15.1/jeos/openSUSE-Leap-15.1-JeOS.x86_64-OpenStack-Cloud.qcow2 -O $HOME/.teuthology/www/images/opensuse-15.1-$DATE-cloudimg-amd64.img
wget http://download.opensuse.org/tumbleweed/appliances/openSUSE-Tumbleweed-JeOS.x86_64-OpenStack-Cloud.qcow2 -O $HOME/.teuthology/www/images/opensuse-tumbleweed-20200810-cloudimg-amd64.img
````

Create sha512 for the image:

```bash
cd $HOME/.teuthology/www/images
sha512sum opensuse-15.2-$DATE-cloudimg-amd64.img | cut -d' ' -f1 > opensuse-15.2-$DATE-cloudimg-amd64.img.sha512
sha512sum opensuse-15.1-$DATE-cloudimg-amd64.img | cut -d' ' -f1 > opensuse-15.1-$DATE-cloudimg-amd64.img.sha512
sha512sum opensuse-tumbleweed-20200810-cloudimg-amd64.img | cut -d' ' -f1 > opensuse-tumbleweed-20200810-cloudimg-amd64.img.sha512
```

run webserver localy:

```bash
(cd $TEUTH_HOME/www && python -m SimpleHTTPServer 8181)
```

or

```bash
(cd $TEUTH_HOME/www && python3 -m http.server 8181)
```

```bash
export DOWNBURST_DISCOVER_URL=http://localhost:8181/images/
```

Make sure libvirtd is running and default network is up:

```bash
sudo service libvirtd start
sudo virsh net-start default
```

### Try out node creation


List available distro/version and available images.

```bash
downburst list
```

Start a VM for example:

```bash
downburst -v create --distro opensuse --user-data doc/examples/no-password.opensuse.user.yaml opensuse
sudo virsh net-dhcp-leases default | grep opensuse

```
