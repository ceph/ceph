// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * RGW Etag Verifier is an RGW filter which enables the objects copied using
 * multisite sync to be verified using their ETag from source i.e. the MD5
 * checksum of the object is computed at the destination and is verified to be
 * identical to the ETag stored in the object HEAD at source cluster.
 * 
 * For MPU objects, a different filter named RGWMultipartEtagFilter is applied
 * which re-computes ETag using RGWObjManifest. This computes the ETag using the
 * same algorithm used at the source cluster i.e. MD5 sum of the individual ETag
 * on the MPU parts.
 */
#ifndef CEPH_RGW_ETAG_VERIFIER_H
#define CEPH_RGW_ETAG_VERIFIER_H

#include "rgw_putobj.h"
#include "rgw_op.h"

namespace rgw::putobj {

class ETagVerifier : public rgw::putobj::Pipe
{
protected:
  CephContext* cct;
  MD5 hash;
  string calculated_etag;

public:
  ETagVerifier(CephContext* cct_, rgw::putobj::DataProcessor *next)
    : Pipe(next), cct(cct_) {}

  virtual void calculate_etag() = 0;
  string get_calculated_etag() { return calculated_etag;}

}; /* ETagVerifier */

class ETagVerifier_Atomic : public ETagVerifier
{
public:
  ETagVerifier_Atomic(CephContext* cct_, rgw::putobj::DataProcessor *next)
    : ETagVerifier(cct_, next) {}

  int process(bufferlist&& data, uint64_t logical_offset) override;
  void calculate_etag() override;

}; /* ETagVerifier_Atomic */

class ETagVerifier_MPU : public ETagVerifier
{
  std::vector<uint64_t> part_ofs;
  int cur_part_index{0}, next_part_index{1};
  MD5 mpu_etag_hash;
 
  void process_end_of_MPU_part();

public:
  ETagVerifier_MPU(CephContext* cct,
                             std::vector<uint64_t> part_ofs,
                             rgw::putobj::DataProcessor *next)
    : ETagVerifier(cct, next),
      part_ofs(std::move(part_ofs))
  {}

  int process(bufferlist&& data, uint64_t logical_offset) override;
  void calculate_etag() override;

}; /* ETagVerifier_MPU */

} // namespace rgw::putobj

#endif /* CEPH_RGW_ETAG_VERIFIER_H */
