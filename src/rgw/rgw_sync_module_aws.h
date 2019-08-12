// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_SYNC_MODULE_AWS_H
#define RGW_SYNC_MODULE_AWS_H

#include "rgw_sync_module.h"

struct rgw_sync_aws_multipart_part_info {
  int part_num{0};
  uint64_t ofs{0};
  uint64_t size{0};
  string etag;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(part_num, bl);
    encode(ofs, bl);
    encode(size, bl);
    encode(etag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(part_num, bl);
    decode(ofs, bl);
    decode(size, bl);
    decode(etag, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_sync_aws_multipart_part_info)

struct rgw_sync_aws_src_obj_properties {
  ceph::real_time mtime;
  string etag;
  uint32_t zone_short_id{0};
  uint64_t pg_ver{0};
  uint64_t versioned_epoch{0};

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(mtime, bl);
    encode(etag, bl);
    encode(zone_short_id, bl);
    encode(pg_ver, bl);
    encode(versioned_epoch, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(mtime, bl);
    decode(etag, bl);
    decode(zone_short_id, bl);
    decode(pg_ver, bl);
    decode(versioned_epoch, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_sync_aws_src_obj_properties)

struct rgw_sync_aws_multipart_upload_info {
  string upload_id;
  uint64_t obj_size;
  rgw_sync_aws_src_obj_properties src_properties;
  uint32_t part_size{0};
  uint32_t num_parts{0};

  int cur_part{0};
  uint64_t cur_ofs{0};

  std::map<int, rgw_sync_aws_multipart_part_info> parts;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(upload_id, bl);
    encode(obj_size, bl);
    encode(src_properties, bl);
    encode(part_size, bl);
    encode(num_parts, bl);
    encode(cur_part, bl);
    encode(cur_ofs, bl);
    encode(parts, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(upload_id, bl);
    decode(obj_size, bl);
    decode(src_properties, bl);
    decode(part_size, bl);
    decode(num_parts, bl);
    decode(cur_part, bl);
    decode(cur_ofs, bl);
    decode(parts, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_sync_aws_multipart_upload_info)

class RGWAWSSyncModule : public RGWSyncModule {
 public:
  RGWAWSSyncModule() {}
  bool supports_data_export() override { return false;}
  int create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) override;
};

#endif /* RGW_SYNC_MODULE_AWS_H */
