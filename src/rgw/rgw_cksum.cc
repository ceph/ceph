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
#include "rgw_crc_digest.h"

extern "C" {
#include "madler/crc64nvme.h"
#include "madler/crc32iso_hdlc.h"
#include "madler/crc32iscsi.h"
#include "spdk/crc64.h"
} // extern "C"

namespace rgw::cksum {

  uint64_t diag_crc64nvme_madler(uint64_t crc, const char* data, size_t len)
  {
    crc = crc64nvme_bit(0, NULL, 0);
    return crc64nvme_bit(crc, data, len);
  }

  uint64_t diag_crc64nvme_combine_madler(uint64_t crc1, uint64_t crc2,
					 uint64_t len)
  {
    return crc64nvme_comb(crc1, crc2, len);
  }

  std::optional<rgw::cksum::Cksum>
  combine_crc_cksum(const Cksum ck1, const Cksum ck2, uintmax_t len1)
  {
    std::optional<rgw::cksum::Cksum> ck3;
    if ((ck1.type != ck2.type) ||
	!ck1.crc()) {
      goto out;
    }

    switch(ck1.type) {
    case cksum::Type::crc64nvme:
      {
	/* due to AWS (and other) convention, the at-rest
	 * digest is byteswapped (on LE?);  restore the
	 * defined byte order before combining */
	auto cck1 =
	  rgw::digest::byteswap(std::get<uint64_t>(*ck1.get_crc()));
	auto cck2 =
	  rgw::digest::byteswap(std::get<uint64_t>(*ck2.get_crc()));
	/* madler crcany */
	auto cck3 = crc64nvme_comb(cck1, cck2, len1);
	/* and byteswap */
	cck3 = rgw::digest::byteswap(cck3);
	/* convert to a Cksum, no ascii armor */
	ck3 = Cksum(ck1.type, (char*) &cck3, Cksum::CtorStyle::raw);
      }
      break;
    case cksum::Type::crc32:
    case cksum::Type::crc32c:
      {
	uint32_t cck3;
	auto cck1 =
	  rgw::digest::byteswap(std::get<uint32_t>(*ck1.get_crc()));
	auto cck2 =
	  rgw::digest::byteswap(std::get<uint32_t>(*ck2.get_crc()));
	/* madler crcany */
	switch (ck1.type) {
	case cksum::Type::crc32:
	  cck3 =  crc32iso_hdlc_comb(cck1, cck2, len1);
	  break;
	case cksum::Type::crc32c:
	  cck3 =  crc32iscsi_comb(cck1, cck2, len1);
	  break;
	default:
	  break;
	}
        /* and byteswap */
	cck3 = rgw::digest::byteswap(cck3);
	/* convert to a Cksum, no ascii armor */
	ck3 = Cksum(ck1.type, (char*) &cck3, Cksum::CtorStyle::raw);
      }
      break;
    default:
      break;
    };

  out:
    return ck3;
  }

} // namespace rgw::cksum
