.. _rgw-multitenancy:

=================
RGW Multi-tenancy
=================

.. versionadded:: Jewel

The multi-tenancy feature allows to use buckets and users of the same
name simultaneously by segregating them under so-called ``tenants``.
This may be useful, for instance, to permit users of Swift API to
create buckets with easily conflicting names such as "test" or "trove".

From the Jewel release onward, each user and bucket lies under a tenant.
For compatibility, a "legacy" tenant with an empty name is provided.
Whenever a bucket is referred without an explicit tenant, an implicit
tenant is used, taken from the user performing the operation. Since
the pre-existing users are under the legacy tenant, they continue
to create and access buckets as before. The layout of objects in RADOS
is extended in a compatible way, ensuring a smooth upgrade to Jewel.

Administering Users With Explicit Tenants
=========================================

Tenants as such do not have any operations on them. They appear and
disappear as needed, when users are administered. In order to create,
modify, and remove users with explicit tenants, either an additional
option --tenant is supplied, or a syntax "<tenant>$<user>" is used
in the parameters of the radosgw-admin command.

Examples
--------

Create a user testx$tester to be accessed with S3::

  # radosgw-admin --tenant testx --uid tester --display-name "Test User" --access_key TESTER --secret test123 user create

Create a user testx$tester to be accessed with Swift::

  # radosgw-admin --tenant testx --uid tester --display-name "Test User" --subuser tester:test --key-type swift --access full user create
  # radosgw-admin --subuser 'testx$tester:test' --key-type swift --secret test123

.. note:: The subuser with explicit tenant has to be quoted in the shell.

   Tenant names may contain only alphanumeric characters and underscores.

Accessing Buckets with Explicit Tenants
=======================================

When a client application accesses buckets, it always operates with
credentials of a particular user. As mentioned above, every user belongs
to a tenant. Therefore, every operation has an implicit tenant in its
context, to be used if no tenant is specified explicitly. Thus a complete
compatibility is maintained with previous releases, as long as the
referred buckets and referring user belong to the same tenant.
In other words, anything unusual occurs when accessing another tenant's
buckets *only*.

Extensions employed to specify an explicit tenant differ according
to the protocol and authentication system used.

S3
--

In case of S3, a colon character is used to separate tenant and bucket.
Thus a sample URL would be::

  https://ep.host.dom/tenant:bucket

Here's a simple Python sample:

.. code-block:: python
   :linenos:

	from boto.s3.connection import S3Connection, OrdinaryCallingFormat
	c = S3Connection(
		aws_access_key_id="TESTER",
		aws_secret_access_key="test123",
		host="ep.host.dom",
		calling_format = OrdinaryCallingFormat())
	bucket = c.get_bucket("test5b:testbucket")

Note that it's not possible to supply an explicit tenant using
a hostname. Hostnames cannot contain colons, or any other separators
that are not already valid in bucket names. Using a period creates an
ambiguous syntax. Therefore, the bucket-in-URL-path format has to be
used.

Due to the fact that the native S3 API does not deal with
multi-tenancy and radosgw's implementation does, things get a bit
involved when dealing with signed URLs and public read ACLs.

* A **signed URL** does contain the ``AWSAccessKeyId`` query
  parameters, from which radosgw is able to discern the correct user
  and tenant owning the bucket. In other words, an application
  generating signed URLs should be able to take just the un-prefixed
  bucket name, and produce a signed URL that itself contains the
  bucket name without the tenant prefix. However, it is *possible* to
  include the prefix if you so choose.

  Thus, accessing a signed URL of an object ``bar`` in a container
  ``foo`` belonging to the tenant ``7188e165c0ae4424ac68ae2e89a05c50``
  would be possible either via
  ``http://<host>:<port>/foo/bar?AWSAccessKeyId=b200fb6634c547199e436a0f93c0c46e&Expires=1542890806&Signature=eok6CYQC%2FDwmQQmqvY5jTg6ehXU%3D``,
  or via
  ``http://<host>:<port>/7188e165c0ae4424ac68ae2e89a05c50:foo/bar?AWSAccessKeyId=b200fb6634c547199e436a0f93c0c46e&Expires=1542890806&Signature=eok6CYQC%2FDwmQQmqvY5jTg6ehXU%3D``,
  depending on whether or not the tenant prefix was passed in on
  signature generation.

* A bucket with a **public read ACL** is meant to be read by an HTTP
  client *without* including any query parameters that would allow
  radosgw to discern tenants. Thus, publicly readable objects must
  always be accessed using the bucket name with the tenant prefix.

  Thus, if you set a public read ACL on an object ``bar`` in a
  container ``foo`` belonging to the tenant
  ``7188e165c0ae4424ac68ae2e89a05c50``, you would need to access that
  object via the public URL
  ``http://<host>:<port>/7188e165c0ae4424ac68ae2e89a05c50:foo/bar``.

Swift with built-in authenticator
---------------------------------

TBD -- not in test_multen.py yet

Swift with Keystone
-------------------

In the default configuration, although native Swift has inherent
multi-tenancy, radosgw does not enable multi-tenancy for the Swift
API. This is to ensure that a setup with legacy buckets --- that is,
buckets that were created before radosgw supported multitenancy ---,
those buckets retain their dual-API capability to be queried and
modified using either S3 or Swift.

If you want to enable multitenancy for Swift, particularly if your
users only ever authenticate against OpenStack Keystone, you should
enable Keystone-based multitenancy with the following ``ceph.conf``
configuration option::

  rgw keystone implicit tenants = true

Once you enable this option, any newly connecting user (whether they
are using the Swift API, or Keystone-authenticated S3) will prompt
radosgw to create a user named ``<tenant_id>$<tenant_id``, where
``<tenant_id>`` is a Keystone tenant (project) UUID --- for example,
``7188e165c0ae4424ac68ae2e89a05c50$7188e165c0ae4424ac68ae2e89a05c50``.

Whenever that user then creates an Swift container, radosgw internally
translates the given container name into
``<tenant_id>/<container_name>``, such as
``7188e165c0ae4424ac68ae2e89a05c50/foo``. This ensures that if there
are two or more different tenants all creating a container named
``foo``, radosgw is able to transparently discern them by their tenant
prefix.

It is also possible to limit the effects of implicit tenants
to only apply to swift or s3, by setting ``rgw keystone implicit tenants``
to either ``s3`` or ``swift``.  This will likely primarily
be of use to users who had previously used implicit tenants
with older versions of ceph, where implicit tenants
only applied to the swift protocol.

Notes and known issues
----------------------

Just to be clear, it is not possible to create buckets in other
tenants at present. The owner of newly created bucket is extracted
from authentication information.
