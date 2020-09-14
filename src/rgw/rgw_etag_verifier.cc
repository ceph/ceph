// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_etag_verifier.h"

#define dout_subsys ceph_subsys_rgw

int RGWPutObj_ETagVerifier_Atomic::process(bufferlist&& in, uint64_t logical_offset)
{
  bufferlist out;
  if (in.length() > 0)
    hash.Update((const unsigned char *)in.c_str(), in.length());

  return Pipe::process(std::move(in), logical_offset);
}

void RGWPutObj_ETagVerifier_Atomic::calculate_etag()
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

void RGWPutObj_ETagVerifier_MPU::process_end_of_MPU_part()
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

int RGWPutObj_ETagVerifier_MPU::process(bufferlist&& in, uint64_t logical_offset)
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

void RGWPutObj_ETagVerifier_MPU::calculate_etag()
{
  unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE], mpu_m[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];

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
           "-%lld", (long long)(part_ofs.size()));

  calculated_etag = final_etag_str;
  ldout(cct, 20) << "MPU calculated ETag:" << calculated_etag << dendl;
}
