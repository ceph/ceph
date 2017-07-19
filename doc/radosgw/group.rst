===============
Groups
===============

Ceph Object Gateway provides support for Amazon Groups. A group is a collection
of Users. The Group ARN can be used as a Principal in a Bucket Policy, such that
the policy applies to all the users belonging to a particular group.


Group Management
====================

A group can be created/ deleted/ updated and users can be added to /removed
from a group via radosgw-admin commands.

Create a Group
--------------

To create a group, execute the following::

  radosgw-admin group create --groupname={groupname} [--tenant={tenant}] [--path={path}]

Get Group Info
--------------

To get details of a group, execute the following::

  radosgw-admin group get --groupname={groupname} [--tenant={tenant}]

List Groups
-----------

To list all groups with a specified path prefix, execute the following::

  radosgw-admin group list [--path-prefix={pathprefix}} [--tenant={tenant}]

Update a Group
--------------

To update the name/ path of a group, execute the following::

  radosgw-admin group update --groupname={groupname} [--new-group-name={newgroupname}} [--new-path={newpath}]

Delete a Group
--------------

To delete a group, execute the following::

  radosgw-admin group delete --groupname={groupname} [--tenant={tenant}]

.. note:: All Users from the Group should be removed before executing the group delete command.

Add a User to a Group
---------------------

To add a user to a group, execute the following::

  radosgw-admin group add --uid={uid} --groupname={groupname}

Remove a User from a Group
--------------------------

To remove a user from a group, execute the following::

  radosgw-admin group remove --uid={uid} --groupname={groupname}

List Groups for a User
----------------------

To list groups for a user, execute the following::

  radosgw-admin user list-groups --uid={uid} [--tenant={tenant}]

An example of a Bucket Policy to list a bucket and its contents with Principal
set to a Group ARN is below::

  {
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": ["arn:aws:iam::testx:group/testadmin"]},
        "Action": "s3:ListBucket",
        "Resource": [
          "arn:aws:s3:::happybucket"
          "arn:aws:s3:::happybucket/*"
        ]
      }]
  }

The above bucket policy will allow all users belonging to the group 'testadmin' under tenant 'testx' to access the bucket specified in the policy.

A group can contain multiple users and a user can belong to multiple groups.
A group identified by 'tenant1:groupname' can contain any user belonging to any
other tenant (user identified by 'tenant:username').

Limitations
===========

Currently, we do not support the REST APIs for Group operations.
Also setting permissions on a Group is not supported now.
