#ifndef CEPH_CLS_QUEUE_OPS_H
#define CEPH_CLS_QUEUE_OPS_H

#include "cls/queue/cls_queue_types.h"

struct cls_queue_init_op {
  cls_queue_head head;
  uint64_t head_size{0};
  bool has_urgent_data{false};

  cls_queue_init_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(head, bl);
    encode(head_size, bl);
    encode(has_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(head, bl);
    decode(head_size, bl);
    decode(has_urgent_data, bl);
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(cls_queue_init_op)

struct cls_queue_enqueue_op {
  vector<bufferlist> bl_data_vec;

  cls_queue_enqueue_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bl_data_vec, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bl_data_vec, bl);
    DECODE_FINISH(bl);
  } 
};
WRITE_CLASS_ENCODER(cls_queue_enqueue_op)

struct cls_queue_list_op {
  uint64_t max;
  string start_marker;

  cls_queue_list_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max, bl);
    encode(start_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max, bl);
    decode(start_marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_list_op)

struct cls_queue_list_ret {
  bool is_truncated;
  string next_marker;
  vector<bufferlist> data;
  vector<string> markers;

  cls_queue_list_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(is_truncated, bl);
    encode(next_marker, bl);
    encode(data, bl);
    encode(markers, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(is_truncated, bl);
    decode(next_marker, bl);
    decode(data, bl);
    decode(markers, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_list_ret)

struct cls_queue_remove_op {
  string end_marker;

  cls_queue_remove_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(end_marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(end_marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_remove_op)

struct cls_queue_get_size_ret {
  uint64_t queue_size;

  cls_queue_get_size_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(queue_size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(queue_size, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_get_size_ret)

#endif /* CEPH_CLS_QUEUE_OPS_H */