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

#pragma once

#include <cstdint>
#include <utility>
#include <tuple>
#include <cstring>
#include <boost/algorithm/string/case_conv.hpp>
#include "rgw_cksum_digest.h"
#include "rgw_common.h"
#include "rgw_putobj.h"

namespace rgw::putobj {

  namespace cksum = rgw::cksum;
  using cksum_hdr_t = std::pair<const char*, const char*>;

  static inline const cksum_hdr_t cksum_algorithm_hdr(const RGWEnv& env) {
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
  } /* cksum_algorithm_hdr */

  using GetHeaderCksumResult = std::pair<cksum::Cksum, std::string_view>;

  static inline GetHeaderCksumResult get_hdr_cksum(const RGWEnv& env) {
    cksum::Type cksum_type;
    auto algo_hdr = cksum_algorithm_hdr(env);
    if (algo_hdr.first) {
      if (algo_hdr.second) {
	cksum_type = cksum::parse_cksum_type(algo_hdr.second);
	auto hk = fmt::format("HTTP_X_AMZ_CHECKSUM_{}", algo_hdr.second);
	auto hv = env.get(hk.c_str());
	if (hv) {
	  return
	    GetHeaderCksumResult(cksum::Cksum(cksum_type, hv),
				 std::string_view(hv, std::strlen(hv)));
	}
      }
    }
    return GetHeaderCksumResult(cksum::Cksum(cksum_type), "");
  } /* get_hdr_cksum */

  /* CompleteMultipartUpload can have a checksum value but unlike
   * PutObject, it won't have a checksum algorithm header, so we
   * need to search for one */
  static inline GetHeaderCksumResult find_hdr_cksum(const RGWEnv& env) {
    cksum::Type cksum_type;
    for (int16_t ix = int16_t(cksum::Type::crc32);
	 ix <= uint16_t(cksum::Type::blake3); ++ix) {
      cksum_type = cksum::Type(ix);
      auto hk = fmt::format("HTTP_X_AMZ_CHECKSUM_{}",
			    boost::to_upper_copy(to_string(cksum_type)));
      auto hv = env.get(hk.c_str());
      if (hv) {
	return
	  GetHeaderCksumResult(cksum::Cksum(cksum_type, hv),
			       std::string_view(hv, std::strlen(hv)));
      }
    }
    return GetHeaderCksumResult(cksum::Cksum(cksum_type), "");
  } /* find_hdr_cksum */

  // PutObj filter for streaming checksums
  class RGWPutObj_Cksum : public rgw::putobj::Pipe {

    cksum::Type _type;
    cksum::DigestVariant dv;
    cksum::Digest* _digest;
    cksum::Cksum _cksum;
    cksum_hdr_t cksum_hdr;

  public:

    using VerifyResult = std::tuple<bool, const cksum::Cksum&>;

    static std::unique_ptr<RGWPutObj_Cksum> Factory(
      rgw::sal::DataProcessor* next, const RGWEnv&);

    RGWPutObj_Cksum(rgw::sal::DataProcessor* next, rgw::cksum::Type _type,
		    cksum_hdr_t&& _hdr);
    RGWPutObj_Cksum(RGWPutObj_Cksum& rhs) = delete;
    ~RGWPutObj_Cksum() {}

    cksum::Type type() { return _type; }
    cksum::Digest* digest() const { return _digest; }
    const cksum::Cksum& cksum() { return _cksum; };

    const cksum_hdr_t& header() const {
      return cksum_hdr;
    }

    const cksum::Cksum& finalize() {
      _cksum = finalize_digest(_digest, _type);
      return _cksum;
    }

    const char* expected(const RGWEnv& env) {
      auto hk = fmt::format("HTTP_X_AMZ_CHECKSUM_{}", cksum_hdr.second);
      auto hv = env.get(hk.c_str());
      return hv;
    }

    VerifyResult verify(const RGWEnv& env) {
      if (_cksum.type == cksum::Type::none) [[likely]] {
	(void) finalize();
      }
      auto hv = expected(env);
      auto cv = _cksum.to_armor();
      return VerifyResult(cksum_hdr.first &&
			  hv && !std::strcmp(hv, cv.c_str()),
			  _cksum);
    }

    int process(bufferlist &&data, uint64_t logical_offset) override;

  }; /* RGWPutObj_Cksum */

} // namespace rgw::putobj
