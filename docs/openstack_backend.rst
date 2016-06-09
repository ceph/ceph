.. _openstack-backend:

OpenStack backend
=================

The ``teuthology-openstack`` command is a wrapper around
``teuthology-suite`` that transparently creates the teuthology cluster
using OpenStack virtual machines.

Prerequisites
-------------

An OpenStack tenant with access to the nova and cinder API. If the
cinder API is not available, some jobs won't run because they expect
volumes attached to each instance.

Setup OpenStack at OVH
----------------------

Each instance has a public IP by default.

* `create an account <https://www.ovh.com/fr/support/new_nic.xml>`_
* get $HOME/openrc.sh from `the horizon dashboard <https://horizon.cloud.ovh.net/project/access_and_security/?tab=access_security_tabs__api_access_tab>`_

Setup
-----

* Get and configure teuthology::

    $ git clone http://github.com/ceph/teuthology
    $ cd teuthology ; ./bootstrap install
    $ source virtualenv/bin/activate

Get OpenStack credentials and test it
-------------------------------------

* follow the `OpenStack API Quick Start <http://docs.openstack.org/api/quick-start/content/index.html#cli-intro>`_
* source $HOME/openrc.sh
* verify the OpenStack client works::

    $ nova list
    +----+------------+--------+------------+-------------+-------------------------+
    | ID | Name       | Status | Task State | Power State | Networks                |
    +----+------------+--------+------------+-------------+-------------------------+
    +----+------------+--------+------------+-------------+-------------------------+
* create a passwordless ssh public key with::

    $ openstack keypair create myself > myself.pem
    +-------------+-------------------------------------------------+
    | Field       | Value                                           |
    +-------------+-------------------------------------------------+
    | fingerprint | e0:a3:ab:5f:01:54:5c:1d:19:40:d9:62:b4:b3:a1:0b |
    | name        | myself                                          |
    | user_id     | 5cf9fa21b2e9406b9c4108c42aec6262                |
    +-------------+-------------------------------------------------+
    $ chmod 600 myself.pem

Usage
-----

* Create a passwordless ssh public key::

    $ openstack keypair create myself > myself.pem
    $ chmod 600 myself.pem

* Run the dummy suite. It does nothing useful but shows all works as
  expected. Note that the first time it is run, it can take a long
  time (from a few minutes to half an hour or so) because it downloads
  and uploads a cloud image to the OpenStack provider. ::

    $ teuthology-openstack --key-filename myself.pem --key-name myself --suite dummy
    Job scheduled with name ubuntu-2015-07-24_09:03:29-dummy-master---basic-openstack and ID 1
    2015-07-24 09:03:30,520.520 INFO:teuthology.suite:ceph sha1: dedda6245ce8db8828fdf2d1a2bfe6163f1216a1
    2015-07-24 09:03:31,620.620 INFO:teuthology.suite:ceph version: v9.0.2-829.gdedda62
    2015-07-24 09:03:31,620.620 INFO:teuthology.suite:teuthology branch: master
    2015-07-24 09:03:32,196.196 INFO:teuthology.suite:ceph-qa-suite branch: master
    2015-07-24 09:03:32,197.197 INFO:teuthology.repo_utils:Fetching from upstream into /home/ubuntu/src/ceph-qa-suite_master
    2015-07-24 09:03:33,096.096 INFO:teuthology.repo_utils:Resetting repo at /home/ubuntu/src/ceph-qa-suite_master to branch master
    2015-07-24 09:03:33,157.157 INFO:teuthology.suite:Suite dummy in /home/ubuntu/src/ceph-qa-suite_master/suites/dummy generated 1 jobs (not yet filtered)
    2015-07-24 09:03:33,158.158 INFO:teuthology.suite:Scheduling dummy/{all/nop.yaml}
    2015-07-24 09:03:34,045.045 INFO:teuthology.suite:Suite dummy in /home/ubuntu/src/ceph-qa-suite_master/suites/dummy scheduled 1 jobs.
    2015-07-24 09:03:34,046.046 INFO:teuthology.suite:Suite dummy in /home/ubuntu/src/ceph-qa-suite_master/suites/dummy -- 0 jobs were filtered out.

    2015-07-24 11:03:34,104.104 INFO:teuthology.openstack:
    web interface: http://167.114.242.13:8081/
    ssh access   : ssh ubuntu@167.114.242.13 # logs in /usr/share/nginx/html

* Visit the web interface (the URL is displayed at the end of the
  teuthology-openstack output) to monitor the progress of the suite.

* The virtual machine running the suite will persist for forensic
  analysis purposes. To destroy it run::

    $ teuthology-openstack --key-filename myself.pem --key-name myself --teardown

* The test results can be uploaded to a publicly accessible location
  with the ``--upload`` flag::

    $ teuthology-openstack --key-filename myself.pem --key-name myself \
                           --suite dummy --upload
    

Troubleshooting
---------------

Debian Jessie users may face the following error::

   NameError: name 'PROTOCOL_SSLv3' is not defined

The `workaround
<https://github.com/mistio/mist.io/issues/434#issuecomment-86484952>`_
suggesting to replace ``PROTOCOL_SSLv3`` with ``PROTOCOL_SSLv23`` in
the ssl.py has been reported to work.

Running the OpenStack backend integration tests
-----------------------------------------------

The easiest way to run the integration tests is to first run a dummy suite::

    $ teuthology-openstack --key-name myself --suite dummy
    ...
    ssh access   : ssh ubuntu@167.114.242.13

This will create a virtual machine suitable for the integration
test. Login wih the ssh access displayed at the end of the
``teuthology-openstack`` command and run the following::

    $ pkill -f teuthology-worker
    $ cd teuthology ; pip install "tox>=1.9"
    $ tox -v -e openstack-integration
    integration/openstack-integration.py::TestSuite::test_suite_noop PASSED
    ...
    ========= 9 passed in 2545.51 seconds ========
    $ tox -v -e openstack
    integration/test_openstack.py::TestTeuthologyOpenStack::test_create PASSED
    ...
    ========= 1 passed in 204.35 seconds =========

Defining instances flavor and volumes
-------------------------------------

Each target (i.e. a virtual machine or instance in the OpenStack
parlance) created by the OpenStack backend are exactly the same. By
default they have at least 8GB RAM, 20GB disk, 1 cpus and no disk
attached. It is equivalent to having the following in the
`~/.teuthology.yaml <https://github.com/ceph/teuthology/blob/master/docs/siteconfig.rst>`_ file::

    openstack:
      ...
      machine:
        disk: 20 # GB
        ram: 8000 # MB
        cpus: 1
      volumes:
        count: 0
        size: 1 # GB

If a job needs more RAM or disk etc. the following can be included in
an existing facet (yaml file in the teuthology parlance)::

    openstack:
      - machine:
          disk: 100 # GB
        volumes:
          count: 4
          size: 10 # GB

Teuthology interprets this as the minimimum requirements, on top of
the defaults found in the ``~/.teuthology.yaml`` file and the job will
be given instances with at least 100GB root disk, 8GB RAM, 1 cpus and
four 10GB volumes attached. The highest value wins: if the job claims
to need 4GB RAM and the defaults are 8GB RAM, the targets will all
have 8GB RAM.

Note the dash before the ``machine`` key: the ``openstack`` element is
an array with one value. If the dash is missing, it is a dictionary instead.
It matters because there can be multiple entries per job such as::

    openstack:
      - machine:
          disk: 40 # GB
          ram: 8000 # MB

    openstack:
      - machine:
          ram: 32000 # MB

    openstack:
      - volumes: # attached to each instance
          count: 3
          size: 200 # GB

When a job is composed with these, theuthology aggregates them as::

    openstack:
      - machine:
          disk: 40 # GB
          ram: 8000 # MB
      - machine:
          ram: 32000 # MB
      - volumes: # attached to each instance
          count: 3
          size: 200 # GB

i.e. all entries are grouped in a list in the same fashion ``tasks`` are.
The resource requirement is the maximum of the resources found in each
element (including the default values). In the example above it is equivalent to::

    openstack:
      machine:
        disk: 40 # GB
        ram: 32000 # MB
      volumes: # attached to each instance
        count: 3
        size: 200 # GB
