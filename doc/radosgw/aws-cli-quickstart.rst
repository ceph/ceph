==============================
 Quickstart: Using the AWS CLI
==============================

.. note:: This guide assumes you have already deployed a Ceph cluster
   and have at least one RGW (RADOS Gateway) daemon running.
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

Get RGW credentials
====================

This guide assumes you already have an RGW ``access_key`` and
``secret_key``. Most users configuring an S3 client will already have
been given these by whoever administers the cluster, this guide
focuses on using them, not creating them.

If you're a cluster administrator and need to create a new user, see
:ref:`radosgw-user-management` for the full ``radosgw-admin user
create`` workflow.

.. warning:: Treat your access and secret keys like any other
   credential. Do not commit them to version control or include them
   in screenshots you intend to share publicly.

Install and configure the AWS CLI
==================================

The standard AWS CLI works against any S3-compatible endpoint,
including RGW, by pointing it at your RGW address instead of AWS's.

.. prompt:: bash $

   curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
   unzip awscliv2.zip
   sudo ./aws/install

Set the endpoint for your profile once, so you don't need to repeat
it on every command:

.. prompt:: bash $

   aws --profile ceph configure set endpoint_url http://<RGW_HOST>:80
   aws --profile ceph configure

When prompted, enter the ``access_key`` and ``secret_key`` from the
previous step. Leave region and output format blank, RGW does not
require them.

This writes the endpoint into ``~/.aws/config`` under the
``[profile ceph]`` section, so commands using ``--profile ceph``
automatically use it, no need for ``--endpoint-url`` on every command.

.. note:: This uses AWS's official bundled installer for Linux, the
   recommended method per AWS's own documentation. It installs to
   ``/usr/local/aws-cli`` with a symlink at ``/usr/local/bin/aws``, and
   doesn't rely on ``pip`` or your system's Python packages at all.

Verify it works
================

Since the endpoint is already set on the ``ceph`` profile, these
commands work the same way you'd use them against real AWS S3:

.. prompt:: bash $

   aws --profile ceph s3 mb s3://my-test-bucket
   echo "hello from RGW" > test.txt
   aws --profile ceph s3 cp test.txt s3://my-test-bucket/
   aws --profile ceph s3 ls s3://my-test-bucket/
   aws --profile ceph s3 cp s3://my-test-bucket/test.txt downloaded.txt
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

**Using HTTPS instead of HTTP**
   Many production RGW deployments enable TLS. If your cluster uses
   HTTPS, replace ``http://`` with ``https://`` and port ``80`` with
   ``443`` (or whatever port your deployment uses) in the commands
   above. If the certificate is self-signed, you'll also need to add
   ``--no-verify-ssl`` to AWS CLI commands.

.. note:: RGW also supports several extensions to the standard S3 API
   (e.g. unordered bucket listings, bucket notification filtering).
   These aren't covered here to keep this guide beginner-focused, but
   if you want to explore them, see the `RGW boto3/AWS CLI examples
   <https://github.com/ceph/ceph/tree/main/examples/rgw/boto3>`_.
