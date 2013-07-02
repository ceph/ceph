#ifndef CEPH_CLS_LOG_TYPES_H
#define CEPH_CLS_LOG_TYPES_H

#include "include/encoding.h"
#include "include/types.h"

#include "include/utime.h"

class JSONObj;


struct cls_log_entry {
  string id;
  string section;
  string name;
  utime_t timestamp;
  bufferlist data;

  cls_log_entry() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(section, bl);
    ::encode(name, bl);
    ::encode(timestamp, bl);
    ::encode(data, bl);
    ::encode(id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(section, bl);
    ::decode(name, bl);
    ::decode(timestamp, bl);
    ::decode(data, bl);
    if (struct_v >= 2)
      ::decode(id, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_log_entry)

struct cls_log_header {
  string max_marker;
  utime_t max_time;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(max_marker, bl);
    ::encode(max_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(max_marker, bl);
    ::decode(max_time, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_log_header)


#endif


