======================================
Server Access Logging / Bucket Logging
======================================

.. versionadded:: Mimic

The Ceph Object Gateway supports AWS S3 feature ``Server Access
Logging``, or so called ``Bucket Logging``.


Configuration
=============

Enable Ops Log
--------------

First we need to enable Ceph Object Gateway to record ops log by
adding following config to ``ceph.conf`` ::

  [global]
          rgw enable ops log = true

Then we also need to specify port which Ceph Object Gateway would use
to upload bucket logging file. For example, if we setup a Ceph cluster
by using ``vstart.sh`` script, it would start Ceph Object Gateway on
8000 by default, so you need to add following config to ``ceph.conf`` ::

          rgw bl url = "http://localhost:8000"


Create Bucket Logging Delivery User
-----------------------------------

The thread of Ceph Object Gateway which uploading bucket logging files
use `bl_deliver` typed S3 user. `bl_deliver` typed user has limited
permission, which can only upload objects(can't create bucket).

`bl_deliver` typed user can be created by specifing `--bl_deliver`
flag. For example::

  $ radosgw-admin user create --uid=log-delivery-service --display-name="Bucket Logging Delivery" \
                              --bl_deliver
  {
  ...
      "user_id": "bl_deliver",
      "display_name": "Bucket Logging Delivery",
      "keys": [
          {
              "user": "log-delivery-service",
              "access_key": "TGBG51N4F7V6DAKO5Y5D",
              "secret_key": "9OYA8DW4CaxykOaWbUaEwIi8C5QpwPOU1pmCP6CV"
          }
      ],
      "bl_deliver": "true",
  ...
  }


Add Bucket Logging Delivery User To Zone Config
-----------------------------------------------

After `bl_deliver` user creation,we need to config `bl_deliver` user
to Zone config, so that Ceph Object Gateway in the same Zone can share
`bl_deliver` user info. This can be done by ``radosgw-admin zone
modify`` command.

For example::

  $ radosgw-admin zone modify --access-key=TGBG51N4F7V6DAKO5Y5D \
                              --secret=9OYA8DW4CaxykOaWbUaEwIi8C5QpwPOU1pmCP6CV \
                              --bl_deliver --rgw-zonegroup=default  --rgw-zone=default

  $ radosgw-admin zone get --rgw-zone=default
  {
      "id": "12d51989-9652-4921-8d6d-c4a4d356cc85",
      "name": "default",
     "bl_pool": "default.rgw.log:bl",
     ...
     "bl_deliver_key": {
         "access_key": "TGBG51N4F7V6DAKO5Y5D",
         "secret_key": "9OYA8DW4CaxykOaWbUaEwIi8C5QpwPOU1pmCP6CV"
     },
      ...
  }

.. note:: A ``default`` zone is created for you if you have not done any
   previous `Multisite Configuration`_.


Restart Ceph Object Gateway
---------------------------

After Zone config modification, we need to restart Ceph Object
Gateway. Then we can use following command to check whether Zone
modification is taken effect::

  $ radosgw-admin zone get --rgw-zone=default
  {
      "id": "12d51989-9652-4921-8d6d-c4a4d356cc85",
      "name": "default",
      "bl_pool": "default.rgw.log:bl",
      ...
      "bl_deliver_key": {
          "access_key": "TGBG51N4F7V6DAKO5Y5D",
          "secret_key": "9OYA8DW4CaxykOaWbUaEwIi8C5QpwPOU1pmCP6CV"
      },
      ...
  }


Usage
=====

All buckets that with bucket logging enabled can be found by
`radosgw-admin bl list` command, sample output are like following ::

  $ radosgw-admin bl list
  [
  { "bucket": ":111:7e7ad28f-b7db-44ad-81ac-96de39a13a12.4105.1", "status": "PROCESSING" }
  ,
  { "bucket": ":500:7e7ad28f-b7db-44ad-81ac-96de39a13a12.4121.2", "status": "UNINITIAL" }
  ,
  { "bucket": ":501:7e7ad28f-b7db-44ad-81ac-96de39a13a12.4121.3", "status": "UNINITIAL" }
  ,
  { "bucket": ":502:7e7ad28f-b7db-44ad-81ac-96de39a13a12.4121.4", "status": "UNINITIAL" }
  ,
  { "bucket": ":503:7e7ad28f-b7db-44ad-81ac-96de39a13a12.4121.5", "status": "UNINITIAL" }
  ,
  { "bucket": ":504:7e7ad28f-b7db-44ad-81ac-96de39a13a12.4121.6", "status": "UNINITIAL" }
  ,
  { "bucket": ":505:7e7ad28f-b7db-44ad-81ac-96de39a13a12.4121.7", "status": "UNINITIAL" }
  ]


We can also use following command to initiate a bucket logging delivery manually::

  $ radosgw-admin bl process


.. _`Multisite Configuration`: ../multisite
