#ifndef CEPH_RGW_QUOTA_H
#define CEPH_RGW_QUOTA_H


#include "include/utime.h"
#include "common/lru_map.h"

class RGWRados;
class JSONObj;

struct RGWQuotaInfo {
  uint64_t max_size_kb;
  uint64_t max_objects;
  bool enabled;

  RGWQuotaInfo() : max_size_kb(0), max_objects(0), enabled(false) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(max_size_kb, bl);
    ::encode(max_objects, bl);
    ::encode(enabled, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(max_size_kb, bl);
    ::decode(max_objects, bl);
    ::decode(enabled, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;

  void decode_json(JSONObj *obj);

};
WRITE_CLASS_ENCODER(RGWQuotaInfo)


#endif
