// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdint.h>
#include <string>
#include <string_view>
#include <array>
#include <iterator>
#include <boost/algorithm/string.hpp>
#include "fmt/format.h"
#include "common/armor.h"
#include <boost/algorithm/hex.hpp>
#include "rgw_hex.h"
#include "rgw_b64.h"

#include "include/buffer.h"
#include "include/encoding.h"

namespace rgw { namespace cksum {

  enum class Type : uint16_t
  {
      none = 0,
      crc32,  /* !cryptographic, but AWS supports */
      crc32c, /* !cryptographic, but AWS supports */
      xxh3,   /* !cryptographic, but strong and very fast */
      sha1,   /* unsafe, but AWS supports */
      sha256,
      sha512,
      blake3,
  };

  static constexpr uint16_t FLAG_NONE =      0x0000;
  static constexpr uint16_t FLAG_AWS_CKSUM = 0x0001;

  class Desc
  {
  public:
    const Type type;
    const char* name;
    const uint16_t digest_size;
    const uint16_t armored_size;
    const uint16_t flags;

    constexpr uint16_t to_armored_size(uint16_t sz) {
      return sz / 3 * 4 + 4;
    }

    constexpr Desc(Type _type, const char* _name, uint16_t _size,
		   uint16_t _flags)
      : type(_type), name(_name),
	digest_size(_size),
	armored_size(to_armored_size(digest_size)),
	flags(_flags)
      {}

    constexpr bool aws() const {
      return (flags & FLAG_AWS_CKSUM);
    }
  }; /* Desc */

  namespace  ba = boost::algorithm;

  class Cksum {
  public:
    static constexpr std::array<Desc, 8> checksums =
    {
      Desc(Type::none, "none", 0, FLAG_NONE),
      Desc(Type::crc32, "crc32", 4, FLAG_AWS_CKSUM),
      Desc(Type::crc32c, "crc32c", 4, FLAG_AWS_CKSUM),
      Desc(Type::xxh3, "xxh3", 8, FLAG_NONE),
      Desc(Type::sha1, "sha1", 20, FLAG_AWS_CKSUM),
      Desc(Type::sha256, "sha256", 32, FLAG_AWS_CKSUM),
      Desc(Type::sha512, "sha512", 64, FLAG_NONE),
      Desc(Type::blake3, "blake3", 32, FLAG_NONE),
    };

    static constexpr uint16_t max_digest_size = 64;
    using value_type = std::array<unsigned char, max_digest_size>;

    Type type;
    value_type digest;

    Cksum(Type _type = Type::none) : type(_type) {}
    Cksum(Type _type, const char* _armored_text)
      : type(_type) {
      const auto& ckd = checksums[uint16_t(type)];
      (void) ceph_unarmor((char*) digest.begin(),
			  (char*) digest.begin() + ckd.digest_size,
			  _armored_text,
			  _armored_text + std::strlen(_armored_text));
    }

    const char* type_string() const {
      return (Cksum::checksums[uint16_t(type)]).name;
    }

    const bool aws() const {
      return (Cksum::checksums[uint16_t(type)]).aws();
    }

    std::string aws_name() const {
      return fmt::format("x-amz-checksum-{}", type_string());
    }

    std::string rgw_name() const {
      return fmt::format("x-rgw-checksum-{}", type_string());
    }

    std::string header_name() const {
      return (aws()) ? aws_name() : rgw_name();
    }

    std::string element_name() const {
      std::string ts{type_string()};
      return fmt::format("Checksum{}", boost::to_upper_copy(ts));
    }

    std::string_view raw() const {
      const auto& ckd = checksums[uint16_t(type)];
      return std::string_view((char*) digest.begin(), ckd.digest_size);
    }

    std::string to_armor() const {
      std::string hs;
      const auto& ckd = checksums[uint16_t(type)];
      hs.resize(ckd.armored_size);
      ceph_armor((char*) hs.data(), (char*) hs.data() + ckd.armored_size,
		 (char*) digest.begin(), (char*) digest.begin() +
		 ckd.digest_size);
      return hs;
    }

    std::string hex() const {
      std::string hs;
      const auto& ckd = checksums[uint16_t(type)];
      hs.reserve(ckd.digest_size * 2 + 1);
      ba::hex_lower(digest.begin(), digest.begin() + ckd.digest_size,
		    std::back_inserter(hs));
      return hs;
    }

    std::string to_base64() const {
      return rgw::to_base64(hex());
    }

    std::string to_string() const  {
      std::string hs;
      const auto& ckd = checksums[uint16_t(type)];
      return fmt::format("{{{}}}{}", ckd.name, to_base64());
    }

    void encode(buffer::list& bl) const {
      const auto& ckd = checksums[uint16_t(type)];
      ENCODE_START(1, 1, bl);
      encode(uint16_t(type), bl);
      encode(ckd.digest_size, bl);
      bl.append((char*)digest.data(), ckd.digest_size);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& p) {
      DECODE_START(1, p);
      uint16_t tt;
      decode(tt, p);
      type = cksum::Type(tt);
      decode(tt, p); /* <= max_digest_size */
      p.copy(tt, (char*)digest.data());
      DECODE_FINISH(p);
    }
  }; /* Cksum */
  WRITE_CLASS_ENCODER(Cksum);

  static inline const std::optional<rgw::cksum::Cksum> no_cksum{std::nullopt};

  static inline std::string to_string(const Type type) {
    std::string hs;
    const auto& ckd = Cksum::checksums[uint16_t(type)];
    return ckd.name;
  }

  static inline Type parse_cksum_type(const char* name)
  {
    for (const auto& ck : Cksum::checksums) {
      if (boost::iequals(ck.name, name))
	return ck.type;
    }
    return Type::none;
  } /* parse_cksum_type */

  static inline Type parse_cksum_type_hdr(const std::string_view hdr_name) {
    auto pos = hdr_name.find("x-amz-checksum-", 0);
    if (pos == std::string::npos) {
      return Type::none;
    }
    constexpr int8_t psz = sizeof("x-amz-checksum-") - 1;
    if ((hdr_name.size() - psz) > 0 ) {
      std::string ck_name{hdr_name.substr(psz)};
      return parse_cksum_type(ck_name.c_str());
    }
    return Type::none;
  } /* parse_cksum_type_hdr */

  static inline bool is_checksum_hdr(const std::string_view hdr_name) {
    return hdr_name == "x-amz-checksum-algorithm" ||
      parse_cksum_type_hdr(hdr_name) != Type::none;
  } /* is_cksum_hdr */

}} /* namespace */
