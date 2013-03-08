#ifndef CEPH_CLS_LOG_TYPES_H
#define CEPH_CLS_LOG_TYPES_H

#include "include/encoding.h"
#include "include/types.h"

#include "include/utime.h"

class JSONObj;


struct cls_log_entry {
  string section;
  string name;
  utime_t timestamp;
  bufferlist data;

  cls_log_entry() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(section, bl);
    ::encode(name, bl);
    ::encode(timestamp, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(section, bl);
    ::decode(name, bl);
    ::decode(timestamp, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(cls_log_entry)


#endif


