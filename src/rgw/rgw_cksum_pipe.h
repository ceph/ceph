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
#include "rgw_cksum.h"
#include "rgw_cksum_digest.h"
#include "rgw_common.h"
#include "rgw_putobj.h"

namespace rgw::putobj {

  namespace cksum = rgw::cksum;
  using cksum_hdr_t = std::pair<const char*, const char*>;

  static inline const cksum_hdr_t cksum_algorithm_hdr(rgw::cksum::Type t) {
    static constexpr std::string_view hdr =
      "HTTP_X_AMZ_SDK_CHECKSUM_ALGORITHM";
    using rgw::cksum::Type;
    switch (t) {
    case Type::sha256:
      return cksum_hdr_t(hdr.data(), "SHA256");
      break;
    case Type::crc32:
      return cksum_hdr_t(hdr.data(), "CRC32");
      break;
    case Type::crc32c:
      return cksum_hdr_t(hdr.data(), "CRC32C");
      break;
    case Type::xxh3:
      return cksum_hdr_t(hdr.data(), "XX3");
      break;
    case Type::sha1:
      return cksum_hdr_t(hdr.data(), "SHA1");
      break;
    case Type::sha512:
      return cksum_hdr_t(hdr.data(), "SHA512");
      break;
    case Type::blake3:
      return cksum_hdr_t(hdr.data(), "BLAKE3");
      break;
    default:
      break;
    };
    return cksum_hdr_t(nullptr, nullptr);;
  }

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

  static inline cksum::Type
  multipart_cksum_algo(const RGWEnv& env) {
    /* AWS has shifted to "strong" integrity checking by default,
     * we may define policy in future */
    cksum::Type cksum_algo{cksum::Type::none};
    auto algo_hdr = rgw::putobj::cksum_algorithm_hdr(env);
    if (algo_hdr.second) {
      cksum_algo = rgw::cksum::parse_cksum_type(algo_hdr.second);
    }
    return cksum_algo;
  }

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
	    GetHeaderCksumResult(cksum::Cksum(cksum_type, hv,
				   cksum::Cksum::CtorStyle::from_armored),
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
	  GetHeaderCksumResult(cksum::Cksum(cksum_type, hv,
				 cksum::Cksum::CtorStyle::from_armored),
			       std::string_view(hv, std::strlen(hv)));
      }
    }
    return GetHeaderCksumResult(cksum::Cksum(cksum_type), "");
  } /* find_hdr_cksum */

  static inline uint16_t
  parse_cksum_flags(cksum::Type t, boost::optional<const std::string &> type_hdr)  {
    uint16_t cksum_flags{0};
    if (type_hdr) {
      if (boost::algorithm::iequals(*type_hdr, "full_object")) {
	cksum_flags |= cksum::Cksum::FLAG_FULL_OBJECT;
      }
      if (boost::algorithm::iequals(*type_hdr, "composite")) {
	cksum_flags |= cksum::Cksum::FLAG_COMPOSITE;
      }
    } else {
      /* if the client sent a checksum algorithm header but not a "type" header,
       * we can select the matching type */
      cksum_flags |= cksum_flags_of(t);
    }
    return cksum_flags;
  } /* parse_cksum_flags */

  // PutObj filter for streaming checksums
  class RGWPutObj_Cksum : public rgw::putobj::Pipe {

    cksum::Type _type;
    uint16_t flags;
    cksum::DigestVariant dv;
    cksum::Digest* _digest;
    cksum::Cksum _cksum;
    cksum_hdr_t cksum_hdr;

  public:

    using VerifyResult = std::tuple<bool, const cksum::Cksum&>;

    static std::unique_ptr<RGWPutObj_Cksum> Factory(
      rgw::sal::DataProcessor* next, const RGWEnv&,
      rgw::cksum::Type override_type, uint16_t cksum_flags);

    RGWPutObj_Cksum(rgw::sal::DataProcessor* next, rgw::cksum::Type _type,
		    uint16_t _flags, cksum_hdr_t&& _hdr);
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
      _cksum.flags = flags; // n.b., this may clear the COMPOSITE flag
      return _cksum;
    }

    const char* expected(const RGWEnv& env) {
      if (cksum_hdr.second) {
	auto hk = fmt::format("HTTP_X_AMZ_CHECKSUM_{}", cksum_hdr.second);
	auto hv = env.get(hk.c_str());
	return hv;
      }
      return nullptr;
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
