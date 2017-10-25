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

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(part_num, bl);
    decode(ofs, bl);
    decode(size, bl);
    decode(etag, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_sync_aws_multipart_part_info)

struct rgw_sync_aws_multipart_upload_info {
  string upload_id;
  uint64_t obj_size;
  ceph::real_time mtime;
  uint32_t part_size{0};
  uint32_t num_parts{0};

  int cur_part{0};
  uint64_t cur_ofs{0};

  std::map<int, rgw_sync_aws_multipart_part_info> parts;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(upload_id, bl);
    encode(obj_size, bl);
    encode(mtime, bl);
    encode(part_size, bl);
    encode(num_parts, bl);
    encode(cur_part, bl);
    encode(cur_ofs, bl);
    encode(parts, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(upload_id, bl);
    decode(obj_size, bl);
    decode(mtime, bl);
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
  int create_instance(CephContext *cct, map<string, string, ltstr_nocase>& config, RGWSyncModuleInstanceRef *instance) override;
};

#endif /* RGW_SYNC_MODULE_AWS_H */
