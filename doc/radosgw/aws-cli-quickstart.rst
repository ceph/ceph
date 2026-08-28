==========================================================
 Quickstart: Using the AWS CLI with a cephadm-deployed RGW
==========================================================

.. note:: This guide assumes you have already deployed a Ceph cluster
   with cephadm and have at least one RGW (RADOS Gateway) daemon running.
   See :doc:`../cephadm/services/rgw` for deployment instructions.

This page walks through the smallest possible path from a freshly
deployed RGW daemon to successfully creating a bucket and
uploading/downloading an object using the standard AWS CLI. It is
intended as a companion to the RGW service deployment docs and the
:doc:`s3` protocol reference, neither of which currently connects
"RGW is deployed" to "here is a working S3 client command."

Prerequisites
=============

- A running RGW daemon (verify with ``ceph orch ps --daemon-type rgw``)
- The internal or external IP/hostname and port RGW is listening on
  (default port is 80 unless configured otherwise)

Create an RGW user
===================

RGW manages its own users, separately from any cloud provider's IAM.
Create one with ``radosgw-admin``, run inside a ``cephadm shell``:

.. prompt:: bash $

   sudo cephadm shell -- radosgw-admin user create \
       --uid=myuser \
       --display-name="My User" \
       --email=myuser@example.com

The output includes an ``access_key`` and ``secret_key`` under
``"keys"``. These are what you will use in place of AWS credentials.

.. warning:: Treat these keys like any other credential. Do not commit
   them to version control or include them in screenshots you intend
   to share publicly.

Install and configure the AWS CLI
==================================

The standard AWS CLI works against any S3-compatible endpoint,
including RGW, by pointing it at your RGW address instead of AWS's:

.. prompt:: bash $

   pip3 install awscli --break-system-packages
   aws configure --profile ceph

When prompted, enter the ``access_key`` and ``secret_key`` from the
previous step. Leave region and output format blank, RGW does not
require them.

.. note:: On some systems, ``pip``-installed binaries land in
   ``~/.local/bin``, which may not be on your ``PATH`` by default. If
   ``aws --version`` reports "command not found" after installing, add
   ``export PATH=$PATH:$HOME/.local/bin`` to your shell profile.

Verify it works
================

Every command below needs ``--endpoint-url`` pointing at your RGW
address, since the AWS CLI otherwise assumes the real AWS S3 endpoint:

.. prompt:: bash $

   aws --profile ceph --endpoint-url http://<RGW_HOST>:80 s3 mb s3://my-test-bucket
   echo "hello from RGW" > test.txt
   aws --profile ceph --endpoint-url http://<RGW_HOST>:80 s3 cp test.txt s3://my-test-bucket/
   aws --profile ceph --endpoint-url http://<RGW_HOST>:80 s3 ls s3://my-test-bucket/
   aws --profile ceph --endpoint-url http://<RGW_HOST>:80 s3 cp s3://my-test-bucket/test.txt downloaded.txt
   cat downloaded.txt

If the final ``cat`` prints ``hello from RGW``, the round trip worked
and your RGW deployment is serving S3-compatible traffic correctly.

Common issues
=============

**"Connection refused" or timeout**
   Confirm the RGW daemon is actually running and listening on the
   port you're targeting: ``ceph orch ps --daemon-type rgw``. If
   you're connecting from outside the cluster's network, confirm the
   port is reachable through any firewall or security group rules.

**"SignatureDoesNotMatch" or auth errors**
   Double-check the access/secret key were copied exactly, and that
   the ``--profile`` flag matches the profile name used in
   ``aws configure``.

**Using a self-signed certificate (https endpoint)**
   If your RGW is configured with ``protocol: https`` and a
   self-signed certificate, add ``--no-verify-ssl`` to AWS CLI
   commands, or use ``http://`` if TLS is not required for your
   environment.

