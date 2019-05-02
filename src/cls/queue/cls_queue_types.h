#ifndef CEPH_CLS_QUEUE_TYPES_H
#define CEPH_CLS_QUEUE_TYPES_H

#include "common/ceph_time.h"
#include "common/Formatter.h"

#include "rgw/rgw_basic_types.h"

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
  uint64_t size{0}; // size of queue requested by user, with head size added to it
  uint64_t last_entry_offset = QUEUE_START_OFFSET_1K;
  uint32_t num_urgent_data_entries{0}; // requested by user
  uint32_t num_head_urgent_entries{0}; // actual number of entries in head
  uint32_t num_xattr_urgent_entries{0}; // actual number of entries in xattr in case of spill over
  bool is_empty{true};
  bool has_urgent_data{false};
  bufferlist bl_urgent_data;  // special data known to application using queue

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(front, bl);
    encode(tail, bl);
    encode(size, bl);
    encode(last_entry_offset, bl);
    encode(num_urgent_data_entries, bl);
    encode(num_head_urgent_entries, bl);
    encode(num_xattr_urgent_entries, bl);
    encode(is_empty, bl);
    encode(has_urgent_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(front, bl);
    decode(tail, bl);
    decode(size, bl);
    decode(last_entry_offset, bl);
    decode(num_urgent_data_entries, bl);
    decode(num_head_urgent_entries, bl);
    decode(num_xattr_urgent_entries, bl);
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