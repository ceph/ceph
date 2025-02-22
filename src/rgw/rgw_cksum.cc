// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_cksum.h"
#include <cstdint>

extern "C" {
#include "madler/crc64nvme.h"
#include "spdk/crc64.h"
} // extern "C"

namespace rgw::cksum {

  uint64_t diag_crc64_nvme_madler(uint64_t crc, const char* data, size_t len)
  {
    crc = crc64nvme_bit(0, NULL, 0);
    return crc64nvme_bit(crc, data, len);
  }

  std::optional<uint64_t> diag_get_crc(const Cksum ck1)
  {
    std::optional<uint64_t> res;
    if (!ck1.combinable()) {
      goto out;
    }
    uint64_t crc;
    memcpy(&crc, (char*) ck1.digest.data(), sizeof(crc));
    res = crc;
  out:
    return res;
  }

  std::optional<rgw::cksum::Cksum>
  combine_crc_cksum(const Cksum ck1, const Cksum ck2, uintmax_t len1)
  {
    std::optional<rgw::cksum::Cksum> ck3;
    if ((ck1.type != ck2.type) ||
	!ck1.combinable()) {
      goto out;
    }

    switch(ck1.type) {
    case rgw::cksum::Type::crc64nvme:
      uint64_t cck1, cck2, cck3;
      memcpy(&cck1, (char*) ck1.digest.data(), sizeof(cck1));
      memcpy(&cck2, (char*) ck2.digest.data(), sizeof(cck2));
#if 0
      cck3 = crc64nvme_comb(cck1, cck2, len1);
#else
      cck3 = crc64_nvme_combine(cck1, cck2, len1);
#endif
      ck3 = cksum::Cksum(ck1.type, (char*) &cck3,
			 cksum::Cksum::CtorStyle::raw);
      break;
    default:
      break;
    };

  out:
    return ck3;
  }

} // namespace rgw::cksum
