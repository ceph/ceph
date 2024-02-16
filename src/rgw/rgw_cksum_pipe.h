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

#include <utility>
#include <tuple>
#include <cstring>
#include "rgw_cksum.h"
#include "rgw_putobj.h"

namespace rgw::putobj {

  namespace cksum = rgw::cksum;
  using cksum_hdr_t = std::pair<const char*, const char*>;
  
  // PutObj filter for streaming checksums
  class RGWPutObj_Cksum : public rgw::putobj::Pipe {

    enum class State : uint16_t {
      START,
      DIGEST,
      FINAL
    };
    
    cksum::Type _type;
    cksum::DigestVariant dv;
    cksum::Digest* _digest;
    cksum::Cksum _cksum;
    cksum_hdr_t cksum_hdr;
    State _state;

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
    State state() const { return _state; }

    const cksum_hdr_t& header() const {
      return cksum_hdr;
    }

    const cksum::Cksum& finalize() {
      _cksum = finalize_digest(_digest, _type);
      _state = State::FINAL;
      return _cksum;
    }

    const char* expected(const RGWEnv& env) {
      auto hk = fmt::format("HTTP_X_AMZ_CHECKSUM_{}", cksum_hdr.second);
      auto hv = env.get(hk.c_str());
      return hv;
    }

    VerifyResult verify(const RGWEnv& env) {
      if (_state == State::DIGEST) [[likely]] {
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
