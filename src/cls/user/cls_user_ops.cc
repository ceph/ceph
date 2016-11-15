// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/user/cls_user_types.h"
#include "cls/user/cls_user_ops.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

void cls_user_set_buckets_op::dump(Formatter *f) const
{
  encode_json("entries", entries, f);
  encode_json("add", add, f);
  encode_json("time", utime_t(time), f);
}

void cls_user_set_buckets_op::generate_test_instances(list<cls_user_set_buckets_op*>& ls)
{
  ls.push_back(new cls_user_set_buckets_op);
  cls_user_set_buckets_op *op = new cls_user_set_buckets_op;
  for (int i = 0; i < 3; i++) {
    cls_user_bucket_entry e;
    cls_user_gen_test_bucket_entry(&e, i);
    op->entries.push_back(e);
  }
  op->add = true;
  op->time = utime_t(1, 0).to_real_time();
  ls.push_back(op);
}

void cls_user_remove_bucket_op::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
}

void cls_user_remove_bucket_op::generate_test_instances(list<cls_user_remove_bucket_op*>& ls)
{
  ls.push_back(new cls_user_remove_bucket_op);
  cls_user_remove_bucket_op *op = new cls_user_remove_bucket_op;
  cls_user_gen_test_bucket(&op->bucket, 0);
  ls.push_back(op);
}

void cls_user_list_buckets_op::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  encode_json("max_entries", max_entries, f);
}

void cls_user_list_buckets_op::generate_test_instances(list<cls_user_list_buckets_op*>& ls)
{
  ls.push_back(new cls_user_list_buckets_op);
  cls_user_list_buckets_op *op = new cls_user_list_buckets_op;
  op->marker = "marker";
  op->max_entries = 1000;
  ls.push_back(op);
}

void cls_user_list_buckets_ret::dump(Formatter *f) const
{
  encode_json("entries", entries, f);
  encode_json("marker", marker, f);
  encode_json("truncated", truncated, f);
}

void cls_user_list_buckets_ret::generate_test_instances(list<cls_user_list_buckets_ret*>& ls)
{
  ls.push_back(new cls_user_list_buckets_ret);
  cls_user_list_buckets_ret *ret = new cls_user_list_buckets_ret;
  for (int i = 0; i < 3; i++) {
    cls_user_bucket_entry e;
    cls_user_gen_test_bucket_entry(&e, i);
    ret->entries.push_back(e);
  }
  ret->marker = "123";
  ret->truncated = true;
  ls.push_back(ret);
}

void cls_user_get_header_op::dump(Formatter *f) const
{
  // empty!
}

void cls_user_get_header_op::generate_test_instances(list<cls_user_get_header_op*>& ls)
{
  ls.push_back(new cls_user_get_header_op);
}

void cls_user_get_header_ret::dump(Formatter *f) const
{
  encode_json("header", header, f);
}

void cls_user_get_header_ret::generate_test_instances(list<cls_user_get_header_ret*>& ls)
{
  ls.push_back(new cls_user_get_header_ret);
  cls_user_get_header_ret *ret = new cls_user_get_header_ret;
  cls_user_gen_test_header(&ret->header);
  ls.push_back(ret);
}

void cls_user_complete_stats_sync_op::dump(Formatter *f) const
{
  encode_json("time", utime_t(time), f);
}

void cls_user_complete_stats_sync_op::generate_test_instances(list<cls_user_complete_stats_sync_op*>& ls)
{
  ls.push_back(new cls_user_complete_stats_sync_op);
  cls_user_complete_stats_sync_op *op = new cls_user_complete_stats_sync_op;
  op->time = utime_t(12345, 0).to_real_time();
  ls.push_back(op);
}


