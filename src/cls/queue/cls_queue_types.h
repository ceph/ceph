#ifndef CEPH_CLS_QUEUE_TYPES_H
#define CEPH_CLS_QUEUE_TYPES_H

#include <errno.h>
#include "include/types.h"

//Size of head leaving out urgent data
#define QUEUE_HEAD_SIZE_1K 1024

#define QUEUE_START_OFFSET_1K QUEUE_HEAD_SIZE_1K

constexpr unsigned int QUEUE_HEAD_START = 0xDEAD;
constexpr unsigned int QUEUE_ENTRY_START = 0xBEEF;
constexpr unsigned int QUEUE_ENTRY_OVERHEAD = sizeof(uint16_t) + sizeof(uint64_t);

struct cls_queue_entry
{
  bufferlist data;
  std::string marker;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(data, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(data, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_entry)

struct cls_queue_marker
{
  uint64_t offset{0};
  uint64_t gen{0};

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(gen, bl);
    encode(offset, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(gen, bl);
    decode(offset, bl);
    DECODE_FINISH(bl);
  }

  string to_str() {
    string marker = std::to_string(gen) + '/' + std::to_string(offset);
    return marker;
  }

  int from_str(const char* str) {
    errno = 0;
    char* end = nullptr;
    gen = ::strtoull(str, &end, 10);
    if (errno) {
      return errno;
    }
    if (str == end || *end != '/') { // expects delimiter
      return -EINVAL;
    }
    str = end + 1;
    offset = ::strtoull(str, &end, 10);
    if (errno) {
      return errno;
    }
    if (str == end || *end != 0) { // expects null terminator
      return -EINVAL;
    }
    return 0;
  }

};
WRITE_CLASS_ENCODER(cls_queue_marker)

struct cls_queue_head
{
  uint64_t max_head_size = QUEUE_HEAD_SIZE_1K;
  cls_queue_marker front{QUEUE_START_OFFSET_1K, 0};
  cls_queue_marker tail{QUEUE_START_OFFSET_1K, 0};
  uint64_t queue_size{0}; // size of queue requested by user, with head size added to it
  uint64_t max_urgent_data_size{0};
  bufferlist bl_urgent_data;  // special data known to application using queue

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_head_size, bl);
    encode(front, bl);
    encode(tail, bl);
    encode(queue_size, bl);
    encode(max_urgent_data_size, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max_head_size, bl);
    decode(front, bl);
    decode(tail, bl);
    decode(queue_size, bl);
    decode(max_urgent_data_size, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_head)

#endif
