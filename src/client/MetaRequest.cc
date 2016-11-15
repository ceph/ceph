// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "client/MetaRequest.h"
#include "client/Dentry.h"
#include "client/Inode.h"
#include "messages/MClientReply.h"
#include "common/Formatter.h"

void MetaRequest::dump(Formatter *f) const
{
  f->dump_unsigned("tid", tid);
  f->dump_string("op", ceph_mds_op_name(head.op));
  f->dump_stream("path") << path;
  f->dump_stream("path2") << path2;
  if (_inode)
    f->dump_stream("ino") << _inode->ino;
  if (_old_inode)
    f->dump_stream("old_ino") << _old_inode->ino;
  if (_other_inode)
    f->dump_stream("other_ino") << _other_inode->ino;
  if (target)
    f->dump_stream("target_ino") << target->ino;
  if (_dentry)
    f->dump_string("dentry", _dentry->name);
  if (_old_dentry)
    f->dump_string("old_dentry", _old_dentry->name);
  f->dump_stream("hint_ino") << inodeno_t(head.ino);

  f->dump_stream("sent_stamp") << sent_stamp;
  f->dump_int("mds", mds);
  f->dump_int("resend_mds", resend_mds);
  f->dump_int("send_to_auth", send_to_auth);
  f->dump_unsigned("sent_on_mseq", sent_on_mseq);
  f->dump_int("retry_attempt", retry_attempt);

  f->dump_int("got_unsafe", got_unsafe);

  f->dump_unsigned("uid", head.caller_uid);
  f->dump_unsigned("gid", head.caller_gid);

  f->dump_unsigned("oldest_client_tid", head.oldest_client_tid);
  f->dump_unsigned("mdsmap_epoch", head.mdsmap_epoch);
  f->dump_unsigned("flags", head.flags);
  f->dump_unsigned("num_retry", head.num_retry);
  f->dump_unsigned("num_fwd", head.num_fwd);
  f->dump_unsigned("num_releases", head.num_releases);

  f->dump_int("abort_rc", abort_rc);
}

MetaRequest::~MetaRequest()
{
  if (_dentry)
    _dentry->put();
  if (_old_dentry)
    _old_dentry->put();
  if (reply)
    reply->put();
}

void MetaRequest::set_dentry(Dentry *d) {
  assert(_dentry == NULL);
  _dentry = d;
  _dentry->get();
}
Dentry *MetaRequest::dentry() {
  return _dentry;
}

void MetaRequest::set_old_dentry(Dentry *d) {
  assert(_old_dentry == NULL);
  _old_dentry = d;
  _old_dentry->get();
}
Dentry *MetaRequest::old_dentry() {
  return _old_dentry;
}
