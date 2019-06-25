#ifndef CEPH_CLS_QUEUE_TYPES_H
#define CEPH_CLS_QUEUE_TYPES_H

#include "include/types.h"
#include "common/ceph_time.h"
#include "common/Formatter.h"

#define QUEUE_HEAD_SIZE_1K 1024
//Actual start offset of queue data
#define QUEUE_START_OFFSET_1K QUEUE_HEAD_SIZE_1K

#define QUEUE_HEAD_SIZE_4K (4 * 1024)
//Actual start offset of queue data
#define QUEUE_START_OFFSET_4K QUEUE_HEAD_SIZE_4K

struct cls_queue_head
{
  uint64_t front = QUEUE_START_OFFSET_1K;
  uint64_t tail = QUEUE_START_OFFSET_1K;
  uint64_t queue_size{0}; // size of queue requested by user, with head size added to it
  uint64_t last_entry_offset = QUEUE_START_OFFSET_1K;
  bool is_empty{true};
  bool has_urgent_data{false};
  bufferlist bl_urgent_data;  // special data known to application using queue

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(front, bl);
    encode(tail, bl);
    encode(queue_size, bl);
    encode(last_entry_offset, bl);
    encode(is_empty, bl);
    encode(has_urgent_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(front, bl);
    decode(tail, bl);
    decode(queue_size, bl);
    decode(last_entry_offset, bl);
    decode(is_empty, bl);
    decode(has_urgent_data, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_queue_head*>& o);
};
WRITE_CLASS_ENCODER(cls_queue_head)

#endif