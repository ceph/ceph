#pragma once
#include "common/dout.h"
#include "rgw_dedup_utils.h"

#include <string>

namespace rgw::dedup {
  constexpr const char* RGW_DEDUP_ATTR_EPOCH = "rgw.dedup.attr.epoch";
  //===========================================================================

  struct dedup_epoch_t {
    uint32_t serial;
    //dedup_req_type_t dedup_type;
    int dedup_type;
    utime_t time;
    uint32_t num_work_shards = 0;
    uint32_t num_md5_shards = 0;
  };

  //---------------------------------------------------------------------------
  inline void encode(const dedup_epoch_t& o, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(o.serial, bl);
    encode(o.dedup_type, bl);
    encode(o.time, bl);
    encode(o.num_work_shards, bl);
    encode(o.num_md5_shards, bl);
    ENCODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  inline void decode(dedup_epoch_t& o, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(o.serial, bl);
    decode(o.dedup_type, bl);
    decode(o.time, bl);
    decode(o.num_work_shards, bl);
    decode(o.num_md5_shards, bl);
    DECODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  inline std::ostream& operator<<(std::ostream &out, const dedup_epoch_t &ep)
  {
    utime_t elapsed = ceph_clock_now() - ep.time;
    out << "EPOCH::Time={" << ep.time.tv.tv_sec <<":"<< ep.time.tv.tv_nsec << "}::";
    out << "Elapsed={" << elapsed.tv.tv_sec <<":"<< elapsed.tv.tv_nsec << "}::";
    out << ep.dedup_type << "::serial=" << ep.serial;
    out << "::num_work_shards=" << ep.num_work_shards;
    out << "::num_md5_shards=" << ep.num_md5_shards;
    return out;
  }

} //namespace rgw::dedup
