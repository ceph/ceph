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
#include "rgw_cksum_digest.h"
#include <cstdint>
#include <memory>
#include "rgw_crc_digest.h"

extern "C" {
#include "madler/crc64nvme.h"
#include "madler/crc32iso_hdlc.h"
#include "madler/crc32iscsi.h"
#include "spdk/crc64.h"
} // extern "C"

namespace rgw::cksum {

  std::optional<Cksum>
  combine_crc_cksum(const Cksum& ck1, const Cksum& ck2, uintmax_t len1)
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

  /* the checksum of the final object is a checksum (of the same type,
   * presumably) of the concatenated checksum bytes of the parts, plus
   * "-<num-parts>.  See
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums
   */
  class DigestCombiner : public Combiner
  {
    DigestVariant dv;
    Digest* digest{nullptr};

  public:
    DigestCombiner(cksum::Type t)
      : Combiner(t),
	dv(digest_factory(t)),
	digest(get_digest(dv))
    {}
    virtual void append(const Cksum& cksum, uint64_t ignored) {
      auto ckr = cksum.raw();
      digest->Update((unsigned char *)ckr.data(), ckr.length());
    }
    virtual Cksum final() {
      auto cksum = finalize_digest(digest, get_type());
      cksum.flags |= Cksum::COMPOSITE_MASK;
      return cksum;
    }
  }; /* Digest */

  /* the checksum of the final object is an "ordinary" (full object) checksum,
   * synthesized from the CRCs of the component checksums
   */
  class CRCCombiner : public Combiner
  {
    Cksum cksum;
    bool once{false};
  public:
    CRCCombiner(cksum::Type t)
      : Combiner(t)
    {}
    virtual void append(const Cksum& rhs, uint64_t part_size) {
      if (! once) {
	// save the first part checksum
	cksum = rhs;
	once = true;
      } else {
	// combine the next partial checksum into cksum
	cksum = *combine_crc_cksum(cksum, rhs, part_size);
      }
    }
    virtual Cksum final() {
      cksum.flags |= Cksum::FULL_OBJECT_MASK;
      return cksum;
    }
  }; /* CRCCombine */

  std::unique_ptr<Combiner> CombinerFactory(cksum::Type t, uint16_t flags)
  {
    switch(t) {
    case cksum::Type::crc32:
    case cksum::Type::crc32c:
    case cksum::Type::crc64nvme:
      return std::unique_ptr<Combiner>(new CRCCombiner(t));
    default:
      return std::unique_ptr<Combiner>(new DigestCombiner(t));
    };
  }

} // namespace rgw::cksum
