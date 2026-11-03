// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/types.h"
#include "client/MetaRequest.h"
#include "client/Dentry.h"
#include "client/Inode.h"
#include "messages/MClientReply.h"
#include "common/Formatter.h"

void MetaRequest::dump(Formatter *f) const
{
  auto age = std::chrono::duration<double>(ceph_clock_now() - op_stamp);

  f->dump_unsigned("tid", tid);
  f->dump_string("op", ceph_mds_op_name(head.op));
  f->dump_stream("path") << path;
  f->dump_object("path_obj", path);
  if (std::holds_alternative<filepath>(arg2)) {
    auto&& fp = get_filepath2();
    f->dump_string("path2", fp.get_path());
    f->dump_object("path2_obj", fp);
  } else if (std::holds_alternative<std::string>(arg2)) {
    auto&& s = get_string2();
    f->dump_string("path2", s);
    f->dump_null("path2_obj");
  } else {
    f->dump_string("path2", "");
    f->dump_null("path2_obj");
  }
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
  f->dump_float("age", age.count());
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
  f->dump_unsigned("num_retry", head.ext_num_retry);
  f->dump_unsigned("num_fwd", head.ext_num_fwd);
  f->dump_unsigned("num_releases", head.num_releases);

  f->dump_int("abort_rc", abort_rc);

  f->dump_unsigned("owner_uid", head.owner_uid);
  f->dump_unsigned("owner_gid", head.owner_gid);
}

void MetaRequest::set_dentry(DentryRef dn) {
  ceph_assert(_dentry.get() == NULL);
  _dentry = std::move(dn);
}
Dentry *MetaRequest::dentry() {
  return _dentry.get();
}

void MetaRequest::set_old_dentry(DentryRef dn) {
  ceph_assert(_old_dentry.get() == NULL);
  _old_dentry = std::move(dn);
}
Dentry *MetaRequest::old_dentry() {
  return _old_dentry.get();
}
