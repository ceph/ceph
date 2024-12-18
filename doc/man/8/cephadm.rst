:orphan:

=========================================
 cephadm -- manage the local cephadm host
=========================================

.. program:: cephadm

Synopsis
========

| **cephadm**** [-h] [--image IMAGE] [--docker] [--data-dir DATA_DIR]
|               [--log-dir LOG_DIR] [--logrotate-dir LOGROTATE_DIR]
|               [--unit-dir UNIT_DIR] [--verbose] [--timeout TIMEOUT]
|               [--retry RETRY] [--no-container-init]
|               {version,pull,inspect-image,ls,list-networks,adopt,rm-daemon,rm-cluster,run,shell,enter,ceph-volume,unit,logs,bootstrap,deploy,check-host,prepare-host,add-repo,rm-repo,install,list-images}
|               ...


| **cephadm** **pull**

| **cephadm** --image IMAGE_NAME **inspect-image**

| **cephadm** **ls** [-h] [--no-detail] [--legacy-dir LEGACY_DIR]

| **cephadm** **list-networks**

| **cephadm** **adopt** [-h] --name NAME --style STYLE [--cluster CLUSTER]
|                       [--legacy-dir LEGACY_DIR] [--config-json CONFIG_JSON]
|                       [--skip-firewalld] [--skip-pull]

| **cephadm** **rm-daemon** [-h] --name NAME --fsid FSID [--force]
|                           [--force-delete-data]

| **cephadm** **rm-cluster** [-h] --fsid FSID [--force]

| **cephadm** **run** [-h] --name NAME --fsid FSID

| **cephadm** **shell** [-h] [--fsid FSID] [--name NAME] [--config CONFIG]
                        [--keyring KEYRING] --mount [MOUNT [MOUNT ...]] [--env ENV]
                        [--] [command [command ...]]

| **cephadm** **enter** [-h] [--fsid FSID] --name NAME [command [command ...]]

| **cephadm** **ceph-volume** [-h] [--fsid FSID] [--config-json CONFIG_JSON]
                              [--config CONFIG] [--keyring KEYRING]
                              command [command ...]

| **cephadm** **unit**  [-h] [--fsid FSID] --name NAME command

| **cephadm** **logs** [-h] [--fsid FSID] --name NAME [command [command ...]]

| **cephadm** **bootstrap** [-h] [--config CONFIG] [--mon-id MON_ID]
|                           [--mon-addrv MON_ADDRV] [--mon-ip MON_IP]
|                           [--mgr-id MGR_ID] [--fsid FSID]
|                           [--log-to-file] [--single-host-defaults]
|                           [--output-dir OUTPUT_DIR]
|                           [--output-keyring OUTPUT_KEYRING]
|                           [--output-config OUTPUT_CONFIG]
|                           [--output-pub-ssh-key OUTPUT_PUB_SSH_KEY]
|                           [--skip-ssh]
|                           [--initial-dashboard-user INITIAL_DASHBOARD_USER]
|                           [--initial-dashboard-password INITIAL_DASHBOARD_PASSWORD]
|                           [--ssl-dashboard-port SSL_DASHBOARD_PORT]
|                           [--dashboard-key DASHBOARD_KEY]
|                           [--dashboard-crt DASHBOARD_CRT]
|                           [--ssh-config SSH_CONFIG]
|                           [--ssh-private-key SSH_PRIVATE_KEY]
|                           [--ssh-public-key SSH_PUBLIC_KEY]
|                           [--ssh-user SSH_USER] [--skip-mon-network]
|                           [--skip-dashboard] [--dashboard-password-noupdate]
|                           [--no-minimize-config] [--skip-ping-check]
|                           [--skip-pull] [--skip-firewalld] [--allow-overwrite]
|                           [--allow-fqdn-hostname] [--skip-prepare-host]
|                           [--orphan-initial-daemons] [--skip-monitoring-stack]
|                           [--apply-spec APPLY_SPEC]
|                           [--registry-url REGISTRY_URL]
|                           [--registry-username REGISTRY_USERNAME]
|                           [--registry-password REGISTRY_PASSWORD]
|                           [--registry-json REGISTRY_JSON]



| **cephadm** **deploy** [-h] --name NAME --fsid FSID [--config CONFIG]
|                        [--config-json CONFIG_JSON] [--keyring KEYRING]
|                        [--key KEY] [--osd-fsid OSD_FSID] [--skip-firewalld]
|                        [--tcp-ports TCP_PORTS] [--reconfig] [--allow-ptrace]

| **cephadm** **check-host** [-h] [--expect-hostname EXPECT_HOSTNAME]

| **cephadm** **prepare-host**

| **cephadm** **add-repo** [-h] [--release RELEASE] [--version VERSION]
|                          [--dev DEV] [--dev-commit DEV_COMMIT]
|                          [--gpg-url GPG_URL] [--repo-url REPO_URL]


| **cephadm** **rm-repo**

| **cephadm** **install** [-h] [packages [packages ...]]

| **cephadm** **registry-login** [-h] [--registry-url REGISTRY_URL]
|                                [--registry-username REGISTRY_USERNAME]
|                                [--registry-password REGISTRY_PASSWORD]
|                                [--registry-json REGISTRY_JSON] [--fsid FSID]

| **cephadm** **list-images**



Description
===========

:program:`cephadm` is a command line tool to manage the local host for the cephadm orchestrator.

It provides commands to investigate and modify the state of the current host.

:program:`cephadm` is not required on all hosts, but useful when investigating a particular
daemon.

Options
=======

.. option:: --image IMAGE

   container image. Can also be set via the
   "CEPHADM_IMAGE" env var (default: None)

.. option:: --docker

   use docker instead of podman (default: False)

.. option:: --data-dir DATA_DIR

   base directory for daemon data (default: /var/lib/ceph)

.. option:: --log-dir LOG_DIR

   base directory for daemon logs (default: /var/log/ceph)

.. option:: --logrotate-dir LOGROTATE_DIR

   location of logrotate configuration files (default: /etc/logrotate.d)

.. option:: --unit-dir UNIT_DIR

   base directory for systemd units (default: /etc/systemd/system)

.. option:: --verbose, -v

   Show debug-level log messages (default: False)

.. option:: --timeout TIMEOUT

   timeout in seconds (default: None)

.. option:: --retry RETRY

   max number of retries (default: 10)

.. option:: --no-container-init

   do not run podman/docker with `--init` (default: False)


Commands
========

add-repo
--------

configure local package repository to also include the ceph repository.

Arguments:

* [--release RELEASE]       use latest version of a named release (e.g., octopus)
* [--version VERSION]       use specific upstream version (x.y.z)
* [--dev DEV]               use specified bleeding edge build from git branch or tag
* [--dev-commit DEV_COMMIT] use specified bleeding edge build from git commit
* [--gpg-url GPG_URL]       specify alternative GPG key location
* [--repo-url REPO_URL]     specify alternative repo location


adopt
-----

Adopt a daemon deployed with a different deployment tool.

Arguments:

* [--name NAME, -n NAME]       daemon name (type.id)
* [--style STYLE]              deployment style (legacy, ...)
* [--cluster CLUSTER]          cluster name
* [--legacy-dir LEGACY_DIR]    base directory for legacy daemon data
* [--config-json CONFIG_JSON]  Additional configuration information in JSON format
* [--skip-firewalld]           Do not configure firewalld
* [--skip-pull]                do not pull the latest image before adopting

Configuration:

When starting the shell, cephadm looks for configuration in the following order.
Only the first values found are used:

1. An explicit, user provided path to a config file (``-c/--config`` option)
2. Config file for daemon specified with ``--name`` parameter (``/var/lib/ceph/<fsid>/<daemon-name>/config``)
3. ``/var/lib/ceph/<fsid>/config/ceph.conf`` if it exists
4. The config file for a ``mon`` daemon (``/var/lib/ceph/<fsid>/mon.<mon-id>/config``) if it exists
5. Finally: fallback to the default file ``/etc/ceph/ceph.conf``


bootstrap
---------

Bootstrap a cluster on the local host. It deploys a MON and a MGR and then also automatically
deploys the monitoring stack on this host (see --skip-monitoring-stack) and calls
``ceph orch host add $(hostname)`` (see --skip-ssh).

Arguments:

* [--config CONFIG, -c CONFIG]    ceph conf file to incorporate
* [--mon-id MON_ID]               mon id (default: local hostname)
* [--mon-addrv MON_ADDRV]         mon IPs (e.g., [v2:localipaddr:3300,v1:localipaddr:6789])
* [--mon-ip MON_IP]               mon IP
* [--mgr-id MGR_ID]               mgr id (default: randomly generated)
* [--fsid FSID]                   cluster FSID
* [--log-to-file]                 configure cluster to log to traditional log files
* [--single-host-defaults]        configure cluster to run on a single host
* [--output-dir OUTPUT_DIR]       directory to write config, keyring, and pub key files
* [--output-keyring OUTPUT_KEYRING] location to write keyring file with new cluster admin and mon keys
* [--output-config OUTPUT_CONFIG] location to write conf file to connect to new cluster
* [--output-pub-ssh-key OUTPUT_PUB_SSH_KEY] location to write the cluster's public SSH key
* [--skip-ssh                     skip setup of ssh key on local host
* [--initial-dashboard-user INITIAL_DASHBOARD_USER] Initial user for the dashboard
* [--initial-dashboard-password INITIAL_DASHBOARD_PASSWORD] Initial password for the initial dashboard user
* [--ssl-dashboard-port SSL_DASHBOARD_PORT] Port number used to connect with dashboard using SSL
* [--dashboard-key DASHBOARD_KEY] Dashboard key
* [--dashboard-crt DASHBOARD_CRT] Dashboard certificate
* [--ssh-config SSH_CONFIG] SSH config
* [--ssh-private-key SSH_PRIVATE_KEY] SSH private key
* [--ssh-public-key SSH_PUBLIC_KEY] SSH public key
* [--ssh-user SSH_USER]           set user for SSHing to cluster hosts, passwordless sudo will be needed for non-root users'
* [--skip-mon-network]            set mon public_network based on bootstrap mon ip
* [--skip-dashboard]              do not enable the Ceph Dashboard
* [--dashboard-password-noupdate] stop forced dashboard password change
* [--no-minimize-config]          do not assimilate and minimize the config file
* [--skip-ping-check]             do not verify that mon IP is pingable
* [--skip-pull]                   do not pull the latest image before bootstrapping
* [--skip-firewalld]              Do not configure firewalld
* [--allow-overwrite]             allow overwrite of existing --output-* config/keyring/ssh files
* [--allow-fqdn-hostname]         allow hostname that is fully-qualified (contains ".")
* [--skip-prepare-host]           Do not prepare host
* [--orphan-initial-daemons]      Do not create initial mon, mgr, and crash service specs
* [--skip-monitoring-stack]       Do not automatically provision monitoring stack] (prometheus, grafana, alertmanager, node-exporter)
* [--apply-spec APPLY_SPEC]       Apply cluster spec after bootstrap (copy ssh key, add hosts and apply services)
* [--registry-url REGISTRY_URL]   url of custom registry to login to. e.g. docker.io, quay.io
* [--registry-username REGISTRY_USERNAME] username of account to login to on custom registry
* [--registry-password REGISTRY_PASSWORD] password of account to login to on custom registry
* [--registry-json REGISTRY_JSON] JSON file containing registry login info (see registry-login command documentation)


ceph-volume
-----------

Run ceph-volume inside a container::

    cephadm ceph-volume inventory

Positional arguments:
* [command]               command

Arguments:

* [--fsid FSID]                    cluster FSID
* [--config-json CONFIG_JSON]      JSON file with config and (client.bootstrap-osd) key
* [--config CONFIG, -c CONFIG]     ceph conf file
* [--keyring KEYRING, -k KEYRING]  ceph.keyring to pass through to the container


check-host
----------

check host configuration to be suitable for a Ceph cluster.

Arguments:

* [--expect-hostname EXPECT_HOSTNAME] Check that hostname matches an expected value


deploy
------

deploy a daemon on the local host. Used by the orchestrator CLI::

    cephadm shell -- ceph orch apply <type> ...

Arguments:

* [--name NAME]               daemon name (type.id)
* [--fsid FSID]               cluster FSID
* [--config CONFIG, -c CONFIG] config file for new daemon
* [--config-json CONFIG_JSON] Additional configuration information in JSON format
* [--keyring KEYRING]         keyring for new daemon
* [--key KEY]                 key for new daemon
* [--osd-fsid OSD_FSID]       OSD uuid, if creating an OSD container
* [--skip-firewalld]          Do not configure firewalld
* [--tcp-ports                List of tcp ports to open in the host firewall
* [--reconfig]                Reconfigure a previously deployed daemon
* [--allow-ptrace]            Allow SYS_PTRACE on daemon container


enter
-----

Run an interactive shell inside a running daemon container::

    cephadm enter --name mgr.myhost.ysubfo

Positional arguments:
* [command]               command

Arguments:

* [--fsid FSID]           cluster FSID
* [--name NAME, -n NAME]  daemon name (type.id)

install
-------

install ceph package(s)

Positional arguments:

* [packages]    packages


inspect-image
-------------

Inspect local Ceph container image. From Reef onward, requires specifying
the image to inspect with ``--image``::

    cephadm --image IMAGE_NAME inspect-image

list-networks
-------------

list IP networks


ls
--

list daemon instances known to cephadm on **this** host::

    $ cephadm ls
    [
        {
            "style": "cephadm:v1",
            "name": "mgr.storage-14b-1.ysubfo",
            "fsid": "5110cb22-8332-11ea-9148-0894ef7e8bdc",
            "enabled": true,
            "state": "running",
            "container_id": "8562de72370a3836473ecfff8a22c9ccdd99815386b4692a2b30924fb5493c44",
            "container_image_name": "docker.io/ceph/ceph:v15",
            "container_image_id": "bc83a388465f0568dab4501fb7684398dca8b50ca12a342a57f21815721723c2",
            "version": "15.2.1",
            "started": "2020-04-21T01:16:41.831456",
            "created": "2020-04-21T01:16:41.775024",
            "deployed": "2020-04-21T01:16:41.415021",
            "configured": "2020-04-21T01:16:41.775024"
        },
    ...

Arguments:

* [--no-detail]             Do not include daemon status
* [--legacy-dir LEGACY_DIR] Base directory for legacy daemon data

logs
----

print journald logs for a daemon container::

    cephadm logs --name mgr.myhost.ysubfo

This is similar to::

    journalctl -u mgr.myhost.ysubfo

Can also specify additional journal arguments::

    cephadm logs --name mgr.myhost.ysubfo -- -n 20 # last 20 lines
    cephadm logs --name mgr.myhost.ysubfo -- -f # follow the log


Positional arguments:

* [command]               command (optional)

Arguments:

* [--fsid FSID]           cluster FSID
* [--name NAME, -n NAME]  daemon name (type.id)


prepare-host
------------

prepare a host for cephadm use

Arguments:

* [--expect-hostname EXPECT_HOSTNAME] Set hostname


pull
----

Pull the ceph image::

    cephadm pull

registry-login
--------------

Give cephadm login information for an authenticated registry (url, username and password).
Cephadm will attempt to log the calling host into that registry::

      cephadm registry-login --registry-url [REGISTRY_URL] --registry-username [USERNAME]
                             --registry-password [PASSWORD]

Can also use a JSON file containing the login info formatted as::

      {
       "url":"REGISTRY_URL",
       "username":"REGISTRY_USERNAME",
       "password":"REGISTRY_PASSWORD"
      }

and turn it in with command::

      cephadm registry-login --registry-json [JSON FILE]

Arguments:

* [--registry-url REGISTRY_URL]   url of registry to login to. e.g. docker.io, quay.io
* [--registry-username REGISTRY_USERNAME] username of account to login to on registry
* [--registry-password REGISTRY_PASSWORD] password of account to login to on registry
* [--registry-json REGISTRY_JSON] JSON file containing login info for custom registry
* [--fsid FSID]                   cluster FSID

rm-daemon
---------

Remove a specific daemon instance

Arguments:

* [--name NAME, -n NAME]  daemon name (type.id)
* [--fsid FSID]           cluster FSID
* [--force]               proceed, even though this may destroy valuable data
* [--force-delete-data]   delete valuable daemon data instead of making a backup


rm-cluster
----------

remove all daemons for a cluster

Arguments:

* [--fsid FSID]  cluster FSID
* [--force]      proceed, even though this may destroy valuable data

rm-repo
-------

remove package repository configuration

run
---

run a ceph daemon, in a container, in the foreground

Arguments:

* [--name NAME, -n NAME]  daemon name (type.id)
* [--fsid FSID]           cluster FSID


shell
-----

Run an interactive shell::

    cephadm shell

Or one specific command inside a container::

    cephadm shell -- ceph orch ls


Positional arguments:

* [command]               command (optional)

Arguments:

* [--fsid FSID]                   cluster FSID
* [--name NAME, -n NAME]          daemon name (type.id)
* [--config CONFIG, -c CONFIG]    ceph.conf to pass through to the container
* [--keyring KEYRING, -k KEYRING] ceph.keyring to pass through to the container
* [--mount MOUNT, -m MOUNT]       mount a file or directory under /mnt in the container
* [--env ENV, -e ENV]             set environment variable


unit
----

Operate on the daemon's systemd unit.

Positional arguments:

* [command]               systemd command (start, stop, restart, enable, disable, ...)

Arguments:

* [--fsid FSID]           cluster FSID
* [--name NAME, -n NAME]  daemon name (type.id)


list-images
-----------

List the default container images for all services in ini format. The output can be modified with custom images and passed to --config flag during bootstrap.


Availability
============

:program:`cephadm` is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the documentation at http://docs.ceph.com/ for more information.


See also
========

:doc:`ceph-volume <ceph-volume>`\(8),
