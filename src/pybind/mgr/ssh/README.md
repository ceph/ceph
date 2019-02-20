# dev environment setup

1. start vms with _only_ the ceph packages installed

In `src/pybind/mgr/ssh` run `vagrant up` to create a cluster with a monitor,
manager, and osd nodes. The osd node will have two small extra disks attached.

2. generate an `ssh_config` file for the vm hosts

Execute `vagrant ssh-config > /path/to/ssh_config` to generate a ssh
configuration file that contains hosts, usernames, and keys that will be used by
the bootstrap cluster / ssh orchestrator to establish ssh connections to the
vagrant vms.

3. install ssh orchestrator dependencies

The primary dependency is the `remoto` package that contains a Python SSH client
for connecting to remote nodes and executing commands.

Install with `dnf install python3-remoto`. The version must be >= 0.0.35. At the
time of writing this version is being packaged and is not available. To install
from source:

```
git clone https://github.com/ceph/remoto
cd remoto
python3 setup.py sdist
pip3 install --prefix=/usr dist/remoto-0.0.35.tar.gz
```

4. start the bootstrap cluster (in this case a `vstart.sh` cluster)

Start with a network binding to which the vms can route traffic:

  `vstart.sh -n -i 192.168.121.1`

The following is a manual method for finding this address. TODO: documenting a
automated/deterministic method would be very helpful.

First, ensure that your firewall settings permit each VM to communicate with the
host.  On Fedora, the `trusted` profile is sufficient: `firewall-cmd
--set-default-zone trusted` and also allows traffic on Ceph ports. Then ssh into
one of the vm nodes and ping the default gateway, which happens to be setup as
the host machine.

```
[nwatkins@smash ssh]$ vagrant ssh mon0 -c "getent hosts gateway"
192.168.121.1   gateway
```

5. setup the ssh orchestrator backend

Enable and configure the ssh orchestrator as the active backend:

```
ceph mgr module enable ssh
ceph orchestrator set backend ssh

# optional: this document assumes the orchestrator CLI is enabled
ceph mgr module enable orchestrator_cli
```

Configure the ssh orchestrator by setting the `ssh_config` option to point at
the ssh configuration file generated above:

```
ceph config set mgr mgr/ssh/ssh_config_file /path/to/config
```

The setting can be confirmed by retrieving the configuration settings:

```
[nwatkins@smash build]$ ceph config get mgr.
WHO    MASK LEVEL    OPTION                            VALUE                                             RO
mgr         advanced mgr/orchestrator_cli/orchestrator ssh                                               *
mgr         advanced mgr/ssh/ssh_config_file           /home/nwatkins/src/ceph/src/pybind/mgr/ssh/config *
```

An SSH config file can also be provided through standard input that avoids the
need to have an accessible file path. Use the following command:


```
ceph ssh set-ssh-config -i <path to ssh_config>
```

The next set of instructions we should move to the docs folder

ceph orchestrator host add osd0
ceph orchestrator host add mgr0
ceph orchestrator host add mon0
ceph orchestrator device ls
ceph orchestrator mgr update 3 mgr0 mgr1
