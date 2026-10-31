Ceph change_attr
================

The change_attr exists to satisfy the NFS "change" attribute, defined in
https://www.rfc-editor.org/info/rfc8881/#section-5.8.1.4 . It is exposed as
the "stx_version" member of a statx struct. NFS defines the change attr as::

  A value created by the server that the client can use to determine if file
  data, directory contents, or attributes of the object have been modified. The
  server may return the object's time_metadata attribute for this attribute's
  value, but only if the file system object cannot be updated more frequently
  than the resolution of time_metadata.

This is really unfortunate: the implication is that any time we report
change_attr out to a caller, we have to fully synchronize the change_attr across
all CephFS clients. It is a "REQUIRED" attribute

Current behavior
----------------
change_attr is passed between clients and the MDS using caps traffic. Any time
an entity receives a new change_attr, it checks and takes the larger value of
what it currently has, versus what it just received.
Clients and the MDS freely increment their local change_attr whenever they make
a change of any kind. It *does not* depend on a particular cap state: as long
as the Client can make a change to metadata locally, it calls it good and
increments change_attr. change_attr is incremented in Client::_do_setattr() if
the Client already had sufficient (ie, X) caps on the necessary attribute, but
also when completing a write (ie, Fw required, but not Fs nor Fx!)

change_attr is a member of frag_info_t and sr_t; of MClientCaps and
MClientReply; is embedded in the fscrypt block header; and of inode_t.

In Client::_do_setattr(), it does not explicitty send the MDS new change_attr
values. Is there something in the make_request() call chain that does that?
_do_setattr sets up work for Client::encode_cap_releases() with inode_drop, but
neither that nor Client::encode_inode_release() includes a change_attr value. So
I am not sure this is quite right, if the Client has exclusive caps: it can make
a lot of changes locally, but if it runs across one it can't do it has to make
an MDS request that might increment on a stale change_attr.
This is pretty hard to make happen, though: it would require the Client to have
something like pXxFrwxwb but not Ax, and then getting a change on the aut
values that doesn't trigger other changes. (This may not actually be possible,
so maybe things work out anyway? It's certainly quite a narrow inaccuracy.) We
likely need to update this so that the Client can include change_attr in an
MClientRequest.

The really interesting part is how change_attr is actually exposed. It is only
available via Client::fill_statx(), with the note::
  /* Change time and change_attr both require all shared caps to view */
  if ((mask & CEPH_STAT_CAP_INODE_ALL) == CEPH_STAT_CAP_INODE_ALL) {
    stx->stx_version = in->change_attr;
CEPH_STAT_CAP_INODE_ALL is a union of CEPH_CAP_{PIN|AUTH_SHARED|LINK_SHARED|
FILE_SHARED|XATTR_SHARED}, which...is actually not all caps.
BUT: every time the Client invokes fill_statx(), it is on a newly-created item,
after invoking path_walk(), or after invoking _getattr() with the relevant
mask/caps. So we are following the rules: the MDS does not provide SHARED caps
if there are multiple writers (we enter MIX modes on the filelock; see locks.c).
But if there are multiple writers, that means anything which gets the
change_attr requires an MDS request. That goes into
Server::handle_client_getattr(), which triggers a client gather stage
(round-trip to all clients, to get the latest state) by invoking
Locker::acquire_locks().

The terrible news here is, that means if Ganesha is actually invoking these
functions and getting a new change_attr, it is triggering that gather on every
operation.
