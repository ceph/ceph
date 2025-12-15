// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_SHA_DIGEST_H
#define CEPH_SHA_DIGEST_H

#include <array>
#include <cstdint>
#include <cstring> // for memcmp(), memcpy()
#include <list>
#include <string>

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "common/Formatter.h"
#include "include/encoding.h"
#include "include/encoding_array.h"

template <uint8_t S>
struct sha_digest_t {
  constexpr static uint32_t SIZE = S;
  // TODO: we might consider std::array in the future. Avoiding it for now
  // as sha_digest_t is a part of our public API.
  unsigned char v[S] = {0};

  std::string to_str() const {
    char str[S * 2 + 1] = {0};
    str[0] = '\0';
    for (size_t i = 0; i < S; i++) {
      ::sprintf(&str[i * 2], "%02x", static_cast<int>(v[i]));
    }
    return std::string(str);
  }
  sha_digest_t(const unsigned char *_v) { memcpy(v, _v, SIZE); };
  sha_digest_t() {}

  bool operator==(const sha_digest_t& r) const {
    return ::memcmp(v, r.v, SIZE) == 0;
  }
  bool operator!=(const sha_digest_t& r) const {
    return ::memcmp(v, r.v, SIZE) != 0;
  }

  void encode(ceph::buffer::list &bl) const {
    // copy to avoid reinterpret_cast, is_pod and other nasty things
    using ceph::encode;
    std::array<unsigned char, SIZE> tmparr;
    memcpy(tmparr.data(), v, SIZE);
    encode(tmparr, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    std::array<unsigned char, SIZE> tmparr;
    decode(tmparr, bl);
    memcpy(v, tmparr.data(), SIZE);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_string("sha1", to_str());
  }
  static std::list<sha_digest_t> generate_test_instances() {
    std::list<sha_digest_t> ls;
    ls.emplace_back();
    ls.emplace_back();
    ls.back().v[0] = 1;
    ls.emplace_back();
    ls.back().v[0] = 2;
    return ls;
  }
};

template<uint8_t S>
inline std::ostream &operator<<(std::ostream &out, const sha_digest_t<S> &b) {
  std::string str = b.to_str();
  return out << str;
}

#if FMT_VERSION >= 90000
template <uint8_t S> struct fmt::formatter<sha_digest_t<S>> : fmt::ostream_formatter {};
#endif

using sha1_digest_t = sha_digest_t<20>;
WRITE_CLASS_ENCODER(sha1_digest_t)

using sha256_digest_t = sha_digest_t<32>;
WRITE_CLASS_ENCODER(sha256_digest_t)

using sha512_digest_t = sha_digest_t<64>;

using md5_digest_t = sha_digest_t<16>;
WRITE_CLASS_ENCODER(md5_digest_t)

#endif
