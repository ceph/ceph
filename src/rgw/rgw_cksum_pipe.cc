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
#include <memory>
#include <string>
#include <fmt/format.h>
#include <boost/algorithm/string.hpp>
#include "rgw_common.h"
#include "common/dout.h"
#include "rgw_client_io.h"

namespace rgw::putobj {

  RGWPutObj_Cksum::RGWPutObj_Cksum(rgw::sal::DataProcessor* next,
				   rgw::cksum::Type _typ,
				   cksum_hdr_t&& _hdr)
    : Pipe(next),
      _type(_typ),
      dv(rgw::cksum::digest_factory(_type)),
      _digest(cksum::get_digest(dv)), cksum_hdr(_hdr),
      _state(State::DIGEST)
  {}

  std::unique_ptr<RGWPutObj_Cksum> RGWPutObj_Cksum::Factory(
    rgw::sal::DataProcessor* next, const RGWEnv& env)
  {
    /* look for matching headers */
    auto algo_header = cksum_algorithm_hdr(env);
    if (algo_header.first) {
      if (algo_header.second) {
	auto cksum_type = cksum::parse_cksum_type(algo_header.second);
	return
	  std::make_unique<RGWPutObj_Cksum>(
				    next, cksum_type, std::move(algo_header));
      }
      /* malformed checksum algorithm header(s) */
      throw rgw::io::Exception(EINVAL, std::system_category());
    }
    /* no checksum header */
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
