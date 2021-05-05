// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_etag_verifier.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::putobj {

int create_etag_verifier(const DoutPrefixProvider *dpp, 
                         CephContext* cct, DataProcessor* filter,
                         const bufferlist& manifest_bl,
                         const std::optional<RGWCompressionInfo>& compression,
                         etag_verifier_ptr& verifier)
{
  RGWObjManifest manifest;

  try {
    auto miter = manifest_bl.cbegin();
    decode(manifest, miter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: couldn't decode manifest" << dendl;
    return -EIO;
  }

  RGWObjManifestRule rule;
  bool found = manifest.get_rule(0, &rule);
  if (!found) {
    ldpp_dout(dpp, -1) << "ERROR: manifest->get_rule() could not find rule" << dendl;
    return -EIO;
  }

  if (rule.start_part_num == 0) {
    /* Atomic object */
    verifier.emplace<ETagVerifier_Atomic>(cct, filter);
    return 0;
  }

  uint64_t cur_part_ofs = UINT64_MAX;
  std::vector<uint64_t> part_ofs;

  /*
   * We must store the offset of each part to calculate the ETAGs for each
   * MPU part. These part ETags then become the input for the MPU object
   * Etag.
   */
  for (auto mi = manifest.obj_begin(dpp); mi != manifest.obj_end(dpp); ++mi) {
    if (cur_part_ofs == mi.get_part_ofs())
      continue;
    cur_part_ofs = mi.get_part_ofs();
    ldpp_dout(dpp, 20) << "MPU Part offset:" << cur_part_ofs << dendl;
    part_ofs.push_back(cur_part_ofs);
  }

  if (compression) {
    // if the source object was compressed, the manifest is storing
    // compressed part offsets. transform the compressed offsets back to
    // their original offsets by finding the first block of each part
    const auto& blocks = compression->blocks;
    auto block = blocks.begin();
    for (auto& ofs : part_ofs) {
      // find the compression_block with new_ofs == ofs
      constexpr auto less = [] (const compression_block& block, uint64_t ofs) {
        return block.new_ofs < ofs;
      };
      block = std::lower_bound(block, blocks.end(), ofs, less);
      if (block == blocks.end() || block->new_ofs != ofs) {
        ldpp_dout(dpp, 4) << "no match for compressed offset " << ofs
            << ", disabling etag verification" << dendl;
        return -EIO;
      }
      ofs = block->old_ofs;
      ldpp_dout(dpp, 20) << "MPU Part uncompressed offset:" << ofs << dendl;
    }
  }

  verifier.emplace<ETagVerifier_MPU>(cct, std::move(part_ofs), filter);
  return 0;
}

int ETagVerifier_Atomic::process(bufferlist&& in, uint64_t logical_offset)
{
  bufferlist out;
  if (in.length() > 0)
    hash.Update((const unsigned char *)in.c_str(), in.length());

  return Pipe::process(std::move(in), logical_offset);
}

void ETagVerifier_Atomic::calculate_etag()
{
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char calc_md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];

  /* Return early if ETag has already been calculated */
  if (!calculated_etag.empty())
    return;

  hash.Final(m);
  buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5);
  calculated_etag = calc_md5;
  ldout(cct, 20) << "Single part object: " << " etag:" << calculated_etag
          << dendl;
}

void ETagVerifier_MPU::process_end_of_MPU_part()
{
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char calc_md5_part[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  std::string calculated_etag_part;

  hash.Final(m);
  mpu_etag_hash.Update((const unsigned char *)m, sizeof(m));
  hash.Restart();

  if (cct->_conf->subsys.should_gather(dout_subsys, 20)) {
    buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, calc_md5_part);
    calculated_etag_part = calc_md5_part;
    ldout(cct, 20) << "Part etag: " << calculated_etag_part << dendl;
  }

  cur_part_index++;
  next_part_index++;
}

int ETagVerifier_MPU::process(bufferlist&& in, uint64_t logical_offset)
{
  uint64_t bl_end = in.length() + logical_offset;

  /* Handle the last MPU part */
  if (size_t(next_part_index) == part_ofs.size()) {
    hash.Update((const unsigned char *)in.c_str(), in.length());
    goto done;
  }

  /* Incoming bufferlist spans two MPU parts. Calculate separate ETags */
  if (bl_end > part_ofs[next_part_index]) {

    uint64_t part_one_len = part_ofs[next_part_index] - logical_offset;
    hash.Update((const unsigned char *)in.c_str(), part_one_len);
    process_end_of_MPU_part();

    hash.Update((const unsigned char *)in.c_str() + part_one_len,
      bl_end - part_ofs[cur_part_index]);
    /*
     * If we've moved to the last part of the MPU, avoid usage of
     * parts_ofs[next_part_index] as it will lead to our-of-range access.
     */
    if (size_t(next_part_index) == part_ofs.size())
      goto done;
  } else {
    hash.Update((const unsigned char *)in.c_str(), in.length());
  }

  /* Update the MPU Etag if the current part has ended */
  if (logical_offset + in.length() + 1 == part_ofs[next_part_index])
    process_end_of_MPU_part();

done:
  return Pipe::process(std::move(in), logical_offset);
}

void ETagVerifier_MPU::calculate_etag()
{
  const uint32_t parts = part_ofs.size();
  constexpr auto digits10 = std::numeric_limits<uint32_t>::digits10;
  constexpr auto extra = 2 + digits10; // add "-%u\0" at the end

  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE], mpu_m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + extra];

  /* Return early if ETag has already been calculated */
  if (!calculated_etag.empty())
    return;

  hash.Final(m);
  mpu_etag_hash.Update((const unsigned char *)m, sizeof(m));

  /* Refer RGWCompleteMultipart::execute() for ETag calculation for MPU object */
  mpu_etag_hash.Final(mpu_m);
  buf_to_hex(mpu_m, CEPH_CRYPTO_MD5_DIGESTSIZE, final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],
           sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%u", parts);

  calculated_etag = final_etag_str;
  ldout(cct, 20) << "MPU calculated ETag:" << calculated_etag << dendl;
}

} // namespace rgw::putobj
