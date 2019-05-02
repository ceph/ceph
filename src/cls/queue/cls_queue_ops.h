#ifndef CEPH_CLS_QUEUE_OPS_H
#define CEPH_CLS_QUEUE_OPS_H

#include "cls/rgw/cls_rgw_types.h"
#include "cls/queue/cls_queue_types.h"

struct cls_gc_create_queue_op {
  uint64_t size;
  uint64_t num_urgent_data_entries;

  cls_gc_create_queue_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(size, bl);
    encode(num_urgent_data_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(size, bl);
    decode(num_urgent_data_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_gc_create_queue_op)

struct cls_create_queue_op {
  cls_queue_head head;
  uint64_t head_size{0};

  cls_create_queue_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(head, bl);
    encode(head_size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(head, bl);
    decode(head_size, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_create_queue_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_create_queue_op)

struct cls_enqueue_op {
  vector<bufferlist> bl_data_vec;
  bool has_urgent_data{false};
  bufferlist bl_urgent_data;

  cls_enqueue_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bl_data_vec, bl);
    encode(has_urgent_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bl_data_vec, bl);
    decode(has_urgent_data, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  } 
};
WRITE_CLASS_ENCODER(cls_enqueue_op)

struct cls_dequeue_op {
  bufferlist bl;

  cls_dequeue_op() {}

  static void generate_test_instances(list<cls_dequeue_op*>& ls);
};

struct cls_queue_list_op {
  uint64_t max;
  uint64_t start_offset;

  cls_queue_list_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max, bl);
    encode(start_offset, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max, bl);
    decode(start_offset, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_list_op)

struct cls_queue_list_ret {
  bool is_truncated;
  uint64_t next_offset;
  vector<bufferlist> data;
  vector<uint64_t> offsets;
  bool has_urgent_data;
  bufferlist bl_urgent_data;

  cls_queue_list_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(is_truncated, bl);
    encode(next_offset, bl);
    encode(data, bl);
    encode(offsets, bl);
    encode(has_urgent_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(is_truncated, bl);
    decode(next_offset, bl);
    decode(data, bl);
    decode(offsets, bl);
    decode(has_urgent_data, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_list_ret)

struct cls_queue_remove_op {
  uint64_t start_offset;
  uint64_t end_offset;
  bool has_urgent_data;
  bufferlist bl_urgent_data;

  cls_queue_remove_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(start_offset, bl);
    encode(end_offset, bl);
    encode(has_urgent_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(start_offset, bl);
    decode(end_offset, bl);
    decode(has_urgent_data, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_remove_op)

struct cls_queue_urgent_data_ret {
  bool has_urgent_data{false};
  bufferlist bl_urgent_data;

  cls_queue_urgent_data_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(has_urgent_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(has_urgent_data, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_urgent_data_ret)

struct cls_queue_write_urgent_data_op {
  bool has_urgent_data{false};
  bufferlist bl_urgent_data;

  cls_queue_write_urgent_data_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(has_urgent_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(has_urgent_data, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_write_urgent_data_op)

struct cls_queue_update_last_entry_op {
  bufferlist bl_data;
  bool has_urgent_data{false};
  bufferlist bl_urgent_data;

  cls_queue_update_last_entry_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bl_data, bl);
    encode(has_urgent_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bl_data, bl);
    decode(has_urgent_data, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_update_last_entry_op)

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

#endif /* CEPH_CLS_QUEUE_OPS_H */