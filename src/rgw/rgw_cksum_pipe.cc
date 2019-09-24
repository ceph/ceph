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
  {
    cksum::Digest* digest = cksum::get_digest(dv);
    /* XXXX remove this */
    std::cout << "ctor had digest " << _digest
	      << " and got digest: " << digest
	      << std::endl;
  }

  std::unique_ptr<RGWPutObj_Cksum> RGWPutObj_Cksum::Factory(
    rgw::sal::DataProcessor* next, const RGWEnv& env)
  {
    /* look for matching headers */
    auto match = [&env] () -> const cksum_hdr_t {
      /* If the individual checksum value you provide through
      x-amz-checksum-algorithm doesn't match the checksum algorithm
      you set through x-amz-sdk-checksum-algorithm, Amazon S3 ignores
      any provided ChecksumAlgorithm parameter and uses the checksum
      algorithm that matches the provided value in
      x-amz-checksum-algorithm.
      https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
      */
      for (const auto hk : {"HTTP_X_AMZ_CHECKSUM_ALGORITHM",
			    "HTTP_X_AMZ_SDK_CHECKSUM_ALGORITHM"}) {
	auto hv = env.get(hk);
	if (hv) {
	  return cksum_hdr_t(hk, hv);
	}
      }
      return cksum_hdr_t(nullptr, nullptr);
    };

    auto algo_header = match();
    if (algo_header.first) {
      if (algo_header.second) {
	auto cksum_type = cksum::parse_cksum_type(algo_header.second);
	return  std::make_unique<RGWPutObj_Cksum>(
						  next,
						  cksum_type,
						  std::move(algo_header));
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
    return 0;
  }

} // namespace rgw::putobj
