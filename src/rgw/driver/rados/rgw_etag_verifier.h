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

#pragma once

#include "rgw_putobj.h"
#include "rgw_op.h"
#include "common/static_ptr.h"

namespace rgw::putobj {

class ETagVerifier : public rgw::putobj::Pipe
{
protected:
  CephContext* cct;
  MD5 hash;
  std::string calculated_etag;

public:
  ETagVerifier(CephContext* cct_, rgw::sal::DataProcessor *next)
    : Pipe(next), cct(cct_) {
      // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
      hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
    }

  virtual void calculate_etag() = 0;
  std::string get_calculated_etag() { return calculated_etag;}

}; /* ETagVerifier */

class ETagVerifier_Atomic : public ETagVerifier
{
public:
  ETagVerifier_Atomic(CephContext* cct_, rgw::sal::DataProcessor *next)
    : ETagVerifier(cct_, next) {}

  int process(bufferlist&& data, uint64_t logical_offset) override;
  void calculate_etag() override;

}; /* ETagVerifier_Atomic */

class ETagVerifier_MPU : public ETagVerifier
{
  std::vector<uint64_t> part_ofs;
  uint64_t cur_part_index{0}, next_part_index{1};
  MD5 mpu_etag_hash;
 
  void process_end_of_MPU_part();

public:
  ETagVerifier_MPU(CephContext* cct,
                             std::vector<uint64_t> part_ofs,
                             rgw::sal::DataProcessor *next)
    : ETagVerifier(cct, next),
      part_ofs(std::move(part_ofs))
  {
    // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
    hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  }

  int process(bufferlist&& data, uint64_t logical_offset) override;
  void calculate_etag() override;

}; /* ETagVerifier_MPU */

constexpr auto max_etag_verifier_size = std::max(
    sizeof(ETagVerifier_Atomic),
    sizeof(ETagVerifier_MPU)
  );
using etag_verifier_ptr = ceph::static_ptr<ETagVerifier, max_etag_verifier_size>;

int create_etag_verifier(const DoutPrefixProvider *dpp, 
                         CephContext* cct, rgw::sal::DataProcessor* next,
                         const bufferlist& manifest_bl,
                         const std::optional<RGWCompressionInfo>& compression,
                         etag_verifier_ptr& verifier);

} // namespace rgw::putobj
