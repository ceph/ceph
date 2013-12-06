// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "include/types.h"
#include "cls/user/cls_user_ops.h"
#include "include/rados/librados.hpp"


using namespace librados;


void cls_user_set_buckets(librados::ObjectWriteOperation& op, list<cls_user_bucket_entry>& entries)
{
  bufferlist in;
  cls_user_set_buckets_op call;
  call.entries = entries;
  ::encode(call, in);
  op.exec("user", "set_buckets_info", in);
}

void cls_user_remove_bucket(librados::ObjectWriteOperation& op, const cls_user_bucket& bucket)
{
  bufferlist in;
  cls_user_remove_bucket_op call;
  call.bucket = bucket;
  ::encode(call, in);
  op.exec("user", "remove_bucket", in);
}

class ClsUserListCtx : public ObjectOperationCompletion {
  list<cls_user_bucket_entry> *entries;
  string *marker;
  bool *truncated;
public:
  ClsUserListCtx(list<cls_user_bucket_entry> *_entries, string *_marker, bool *_truncated) :
                                      entries(_entries), marker(_marker), truncated(_truncated) {}
  void handle_completion(int r, bufferlist& outbl) {
    if (r >= 0) {
      cls_user_list_buckets_ret ret;
      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode(ret, iter);
        if (entries)
	  *entries = ret.entries;
        if (truncated)
          *truncated = ret.truncated;
        if (marker)
          *marker = ret.marker;
      } catch (buffer::error& err) {
        // nothing we can do about it atm
      }
    }
  }
};

void cls_user_bucket_list(librados::ObjectReadOperation& op,
                       const string& in_marker, int max_entries, list<cls_user_bucket_entry>& entries,
                       string *out_marker, bool *truncated)
{
  bufferlist inbl;
  cls_user_list_buckets_op call;
  call.marker = in_marker;
  call.max_entries = max_entries;

  ::encode(call, inbl);

  op.exec("user", "list_buckets", inbl, new ClsUserListCtx(&entries, out_marker, truncated));
}


