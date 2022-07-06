.. _mgr-rgw-module:

RGW Module
============
The rgw module helps with bootstrapping and configuring RGW realm
and the different related entities.

Enabling
--------

The *rgw* module is enabled with::

  ceph mgr module enable rgw


RGW Realm Operations
-----------------------

Bootstrapping RGW realm creates a new RGW realm entity, a new zonegroup,
and a new zone. It configures a new system user that can be used for
multisite sync operations, and returns a corresponding token. It sets
up new RGW instances via the orchestrator.

It is also possible to create a new zone that connects to the master
zone and synchronizes data to/from it.


Realm Credentials Token
-----------------------
A new token is created when bootstrapping a new realm, and also
when creating one explicitly.  The token encapsulates
the master zone endpoint, and a set of credentials that are associated
with a system user.
Removal of this token would remove the credentials, and if the corresponding
system user has no more access keys, it is removed.


Commands
--------
::

  ceph rgw realm bootstrap

Create a new realm + zonegroup + zone and deploy rgw daemons via the
orchestrator.  Command returns a realm token that allows new zones to easily
join this realm

::

  ceph rgw zone create

Create a new zone and join existing realm (using the realm token)

::

  ceph rgw zone-creds create

Create new credentials and return a token for new zone connection

::

  ceph rgw zone-creds remove
 
Remove credentials and/or user that are associated with the specified
token

::

  ceph rgw realm reconcile

Update the realm configuration to match the orchestrator deployment

::

  ceph rgw admin [*]

RGW admin command
