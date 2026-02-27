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

#include "rgw_cksum_pipe.h"
#include <cstdint>
#include <memory>
#include <string>
#include <fmt/format.h>
#include <boost/algorithm/string.hpp>
#include "rgw_cksum.h"
#include "rgw_common.h"
#include "common/dout.h"
#include "rgw_client_io.h"

namespace rgw::putobj {

  RGWPutObj_Cksum::RGWPutObj_Cksum(rgw::sal::DataProcessor* next,
				   rgw::cksum::Type _typ,
				   uint16_t _flags,
				   cksum_hdr_t&& _hdr)
    : Pipe(next),
      _type(_typ),
      flags(_flags),
      dv(rgw::cksum::digest_factory(_type)),
      _digest(cksum::get_digest(dv)), cksum_hdr(_hdr)
  {}

  std::unique_ptr<RGWPutObj_Cksum> RGWPutObj_Cksum::Factory(
    rgw::sal::DataProcessor* next, const RGWEnv& env,
    rgw::cksum::Type override_type,
    uint16_t cksum_flags)
  {
    /* look for matching headers */
    auto algo_header = cksum_algorithm_hdr(env);
    if (algo_header.first) {
      if (algo_header.second) {
	auto cksum_type = cksum::parse_cksum_type(algo_header.second);
       /* unknown checksum type in header */
       if (cksum_type != rgw::cksum::Type::none) {
         return
           std::make_unique<RGWPutObj_Cksum>(
		  next, cksum_type, cksum_flags, std::move(algo_header));
       } else {
	 /* unknown checksum type requested */
	 throw rgw::io::Exception(EINVAL, std::system_category());
       }
      }
      /* malformed checksum algorithm header(s) */
      throw rgw::io::Exception(EINVAL, std::system_category());
    }
    /* no checksum header */
    if (override_type != rgw::cksum::Type::none) {
      /* XXXX safe? do we need to fixup env as well? */
      auto algo_header = cksum_algorithm_hdr(override_type);
      return
	std::make_unique<RGWPutObj_Cksum>(
	        next, override_type, cksum_flags, std::move(algo_header));
    }
    /* no checksum requested */
    return std::unique_ptr<RGWPutObj_Cksum>();
  }

  int RGWPutObj_Cksum::process(ceph::buffer::list &&data, uint64_t logical_offset)
  {
    for (const auto& ptr : data.buffers()) {
      _digest->Update(reinterpret_cast<const unsigned char*>(ptr.c_str()),
                      ptr.length());
    }
    return Pipe::process(std::move(data), logical_offset);
  }

} // namespace rgw::putobj
