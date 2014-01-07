// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_USER_OPS_H
#define CEPH_CLS_USER_OPS_H

#include "include/types.h"
#include "cls_user_types.h"

struct cls_user_set_buckets_op {
  list<cls_user_bucket_entry> entries;

  cls_user_set_buckets_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_user_set_buckets_op)

struct cls_user_remove_bucket_op {
  cls_user_bucket bucket;

  cls_user_remove_bucket_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(bucket, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_user_remove_bucket_op)

struct cls_user_list_buckets_op {
  string marker;
  int max_entries; /* upperbound to returned num of entries
                      might return less than that and still be truncated */

  cls_user_list_buckets_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(marker, bl);
    ::encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(marker, bl);
    ::decode(max_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_user_list_buckets_op)

struct cls_user_list_buckets_ret {
  list<cls_user_bucket_entry> entries;
  string marker;
  bool truncated;

  cls_user_list_buckets_ret() : truncated(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ::encode(marker, bl);
    ::encode(truncated, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    ::decode(marker, bl);
    ::decode(truncated, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_user_list_buckets_ret)


struct cls_user_get_header_op {
  cls_user_get_header_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_user_get_header_op)

struct cls_user_get_header_ret {
  cls_user_header header;

  cls_user_get_header_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(header, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(header, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_user_get_header_ret)


#endif
