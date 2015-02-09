#ifndef CEPH_CLS_STATELOG_TYPES_H
#define CEPH_CLS_STATELOG_TYPES_H

#include "include/encoding.h"
#include "include/types.h"

#include "include/utime.h"

class JSONObj;

struct cls_statelog_entry {
  string client_id;
  string op_id;
  string object;
  utime_t timestamp;
  bufferlist data;
  uint32_t state; /* user defined state */

  cls_statelog_entry() : state(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(client_id, bl);
    ::encode(op_id, bl);
    ::encode(object, bl);
    ::encode(timestamp, bl);
    ::encode(data, bl);
    ::encode(state, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(client_id, bl);
    ::decode(op_id, bl);
    ::decode(object, bl);
    ::decode(timestamp, bl);
    ::decode(data, bl);
    ::decode(state, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(cls_statelog_entry)


#endif


