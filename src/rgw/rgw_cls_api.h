#ifndef CEPH_RGW_CLS_API_H
#define CEPH_RGW_CLS_API_H

#include <map>

#include "include/types.h"
#include "include/utime.h"

struct rgw_bucket_dir_entry {
  std::string name;
  uint64_t size;
  utime_t mtime;
  uint64_t epoch;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(name, bl);
    ::encode(mtime, bl);
    ::encode(epoch, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(name, bl);
    ::decode(mtime, bl);
    ::decode(epoch, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_entry)

struct rgw_bucket_dir_header {
  uint64_t total_size;
  uint64_t num_entries;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(total_size, bl);
    ::encode(num_entries, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(total_size, bl);
    ::decode(num_entries, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_dir_header)

struct rgw_bucket_dir {
  struct rgw_bucket_dir_header header;
  std::map<string, struct rgw_bucket_dir_entry> m;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(header, bl);
    ::encode(m, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(header, bl);
    ::decode(m, bl);
  }
};
WRITE_CLASS_ENCODER(rgw_bucket_dir)

enum modify_op {
  CLS_RGW_OP_ADD = 0,
  CLS_RGW_OP_DEL = 1,
};

#endif
