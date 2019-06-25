#ifndef CEPH_CLS_QUEUE_OPS_H
#define CEPH_CLS_QUEUE_OPS_H

#include "cls/queue/cls_queue_types.h"

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
  bufferlist bl_urgent_data;

  cls_enqueue_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bl_data_vec, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bl_data_vec, bl);
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
  bufferlist bl_urgent_data;

  cls_queue_list_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(is_truncated, bl);
    encode(next_offset, bl);
    encode(data, bl);
    encode(offsets, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(is_truncated, bl);
    decode(next_offset, bl);
    decode(data, bl);
    decode(offsets, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_list_ret)

struct cls_queue_remove_op {
  uint64_t start_offset;
  uint64_t end_offset;
  bufferlist bl_urgent_data;

  cls_queue_remove_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(start_offset, bl);
    encode(end_offset, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(start_offset, bl);
    decode(end_offset, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_remove_op)

struct cls_queue_urgent_data_ret {
  bufferlist bl_urgent_data;

  cls_queue_urgent_data_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_urgent_data_ret)

struct cls_queue_write_urgent_data_op {
  bufferlist bl_urgent_data;

  cls_queue_write_urgent_data_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_write_urgent_data_op)

struct cls_queue_update_last_entry_op {
  bufferlist bl_data;
  bufferlist bl_urgent_data;

  cls_queue_update_last_entry_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bl_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bl_data, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_update_last_entry_op)

struct cls_init_queue_op : cls_create_queue_op{

};
WRITE_CLASS_ENCODER(cls_init_queue_op)

#endif /* CEPH_CLS_QUEUE_OPS_H */