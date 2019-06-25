#ifndef CEPH_CLS_RGW_QUEUE_OPS_H
#define CEPH_CLS_RGW_QUEUE_OPS_H

#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_ops.h"

struct cls_gc_create_queue_op {
  uint64_t size;
  uint64_t num_urgent_data_entries{0};
  string name; //for debugging, to be removed later

  cls_gc_create_queue_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(size, bl);
    encode(num_urgent_data_entries, bl);
    encode(name, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(size, bl);
    decode(num_urgent_data_entries, bl);
    decode(name, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_gc_create_queue_op)

struct cls_gc_init_queue_op : cls_gc_create_queue_op {

};
WRITE_CLASS_ENCODER(cls_gc_init_queue_op)

struct cls_rgw_gc_queue_remove_op {
  uint64_t num_entries;
  string marker;

  cls_rgw_gc_queue_remove_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(num_entries, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(num_entries, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_queue_remove_op)

struct cls_gc_defer_entry_op {
  uint32_t expiration_secs;
  cls_rgw_gc_obj_info info;
  cls_gc_defer_entry_op() : expiration_secs(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(expiration_secs, bl);
    encode(info, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(expiration_secs, bl);
    decode(info, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_gc_defer_entry_op)
#endif /* CEPH_CLS_RGW_QUEUE_OPS_H */