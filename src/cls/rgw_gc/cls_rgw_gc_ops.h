#ifndef CEPH_CLS_RGW_GC_OPS_H
#define CEPH_CLS_RGW_GC_OPS_H

#include "cls/rgw/cls_rgw_types.h"

struct cls_rgw_gc_queue_init_op {
  uint64_t size;
  uint64_t num_deferred_entries{0};

  cls_rgw_gc_queue_init_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(size, bl);
    encode(num_deferred_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(size, bl);
    decode(num_deferred_entries, bl);
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_rgw_gc_queue_init_op)

struct cls_rgw_gc_queue_remove_entries_op {
  uint64_t num_entries;

  cls_rgw_gc_queue_remove_entries_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(num_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(num_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_queue_remove_entries_op)

struct cls_rgw_gc_queue_defer_entry_op {
  uint32_t expiration_secs;
  cls_rgw_gc_obj_info info;
  cls_rgw_gc_queue_defer_entry_op() : expiration_secs(0) {}

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
WRITE_CLASS_ENCODER(cls_rgw_gc_queue_defer_entry_op)
#endif /* CEPH_CLS_RGW_GC_OPS_H */