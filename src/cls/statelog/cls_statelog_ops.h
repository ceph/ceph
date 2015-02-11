// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_STATELOG_OPS_H
#define CEPH_CLS_STATELOG_OPS_H

#include "include/types.h"
#include "cls_statelog_types.h"

struct cls_statelog_add_op {
  list<cls_statelog_entry> entries;

  cls_statelog_add_op() {}

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
WRITE_CLASS_ENCODER(cls_statelog_add_op)

struct cls_statelog_list_op {
  string object;
  string client_id;
  string op_id;
  string marker; /* if not empty, overrides from_time */
  int max_entries; /* upperbound to returned num of entries
                      might return less than that and still be truncated */

  cls_statelog_list_op() : max_entries(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(object, bl);
    ::encode(client_id, bl);
    ::encode(op_id, bl);
    ::encode(marker, bl);
    ::encode(max_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(object, bl);
    ::decode(client_id, bl);
    ::decode(op_id, bl);
    ::decode(marker, bl);
    ::decode(max_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_statelog_list_op)

struct cls_statelog_list_ret {
  list<cls_statelog_entry> entries;
  string marker;
  bool truncated;

  cls_statelog_list_ret() : truncated(false) {}

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
WRITE_CLASS_ENCODER(cls_statelog_list_ret)


/*
 * operation will return 0 when successfully removed but not done. Will return
 * -ENODATA when done, so caller needs to repeat sending request until that.
 */
struct cls_statelog_remove_op {
  string client_id;
  string op_id;
  string object;

  cls_statelog_remove_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(client_id, bl);
    ::encode(op_id, bl);
    ::encode(object, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(client_id, bl);
    ::decode(op_id, bl);
    ::decode(object, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_statelog_remove_op)

struct cls_statelog_check_state_op {
  string client_id;
  string op_id;
  string object;
  uint32_t state;

  cls_statelog_check_state_op() : state(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(client_id, bl);
    ::encode(op_id, bl);
    ::encode(object, bl);
    ::encode(state, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(client_id, bl);
    ::decode(op_id, bl);
    ::decode(object, bl);
    ::decode(state, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_statelog_check_state_op)

#endif
