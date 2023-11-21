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

#ifndef RGW_CKSUM_H
#define RGW_CKSUM_H

#include <cstdint>
#include <stdint.h>
#include <string>
#include <array>
#include <iterator>
#include <boost/variant.hpp>
#include <boost/blank.hpp>
#include "fmt/format.h"
#include "common/ceph_crypto.h"
#include "rgw_crc_digest.h"
#include "rgw_xxh_digest.h"
#include <boost/algorithm/hex.hpp>
#include "rgw_hex.h"
#include "rgw_b64.h"

#pragma once

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

  class Desc
  {
  public:
    const Type type;
    const char* name;
    const uint16_t digest_size;
    const uint16_t armored_size;

    constexpr uint16_t to_armored_size(uint16_t sz) {
      return sz / 3 * 4 + 4;
    }

    constexpr Desc(Type _type, const char* _name, uint16_t _size)
      : type(_type), name(_name),
	digest_size(_size),
	armored_size(to_armored_size(digest_size))
      {}
  };

  namespace  ba = boost::algorithm;

  class Cksum {
  public:
    static constexpr std::array<Desc, 8> checksums =
    {
      Desc(Type::none, "none", 0),
      Desc(Type::crc32, "crc32", 4),
      Desc(Type::crc32c, "crc32c", 4),
      Desc(Type::xxh3, "xxh3", 8),
      Desc(Type::sha1, "sha1", 20),
      Desc(Type::sha256, "sha256", 32),
      Desc(Type::sha512, "sha512", 64),
      Desc(Type::blake3, "blake3", 32),
    };

    static constexpr uint16_t max_digest_size = 64;
    using value_type = std::array<unsigned char, max_digest_size>;

    Type type;
    value_type digest;

    Cksum(Type _type) : type(_type) {}

    std::string hex() {
      std::string hs;
      const auto& ckd = checksums[uint16_t(type)];
      hs.reserve(ckd.digest_size * 2 + 1);
      ba::hex_lower(digest.begin(), digest.begin() + ckd.digest_size,
		    std::back_inserter(hs));
      return hs;
    }

    std::string to_base64() {
      return rgw::to_base64(hex());
    }

    std::string to_string()  {
      std::string hs;
      const auto& ckd = checksums[uint16_t(type)];
      return fmt::format("{{{}}}{}", ckd.name, to_base64());
    }
  }; /* Cksum */

  static inline Type parse_cksum_type(const std::string& name)
  {
    for (const auto& ck : Cksum::checksums) {
      if (ck.name == name)
	return ck.type;
    }
    return Type::none;
  }

  static inline const char* to_string(const Type type)
  {
    return (Cksum::checksums[uint16_t(type)]).name;
  }

  class Digest {
  public:
    virtual void Restart() = 0;
    virtual void Update (const unsigned char *input, size_t length) = 0;
    virtual void Update(const ceph::buffer::list& bl) = 0;
    virtual void Final (unsigned char *digest) = 0;
    virtual ~Digest() {}
  };

  template<class T>
  class TDigest : public Digest
  {
    T d;
  public:
    void Restart() override { d.Restart(); }
    void Update(const unsigned char* data, uint64_t len) override {
      d.Update(data, len);
    }
    void Update(const ceph::buffer::list& bl) {
      for (auto& p : bl.buffers()) {
	d.Update((const unsigned char *)p.c_str(), p.length());
      }
    }
    void Final(unsigned char* digest) override {
      d.Final(digest);
    }
  };

  typedef TDigest<ceph::crypto::Blake3> Blake3;
  typedef TDigest<rgw::digest::Crc32> Crc32;
  typedef TDigest<rgw::digest::Crc32c> Crc32c;
  typedef TDigest<rgw::digest::XXH3> XXH3;
  typedef TDigest<ceph::crypto::SHA1> SHA1;
  typedef TDigest<ceph::crypto::SHA256> SHA256;
  typedef TDigest<ceph::crypto::SHA512> SHA512;

  typedef boost::variant<boost::blank,
			 Blake3,
			 Crc32,
			 Crc32c,
			 XXH3,
			 SHA1,
			 SHA256,
			 SHA512> DigestVariant;

  struct get_digest_ptr : public boost::static_visitor<Digest*>
  {
    get_digest_ptr() {};
    Digest* operator()(const boost::blank& b) const { return nullptr; }
    Digest* operator()(Blake3& digest) const { return &digest; }
    Digest* operator()(Crc32& digest) const { return &digest; }
    Digest* operator()(Crc32c& digest) const { return &digest; }
    Digest* operator()(XXH3& digest) const { return &digest; }
    Digest* operator()(SHA1& digest) const { return &digest; }
    Digest* operator()(SHA256& digest) const { return &digest; }
    Digest* operator()(SHA512& digest) const { return &digest; }
  };

  static inline Digest* get_digest(DigestVariant& ev)
  {
    return boost::apply_visitor(get_digest_ptr{}, ev);
  }

  static inline DigestVariant digest_factory(const Type cksum_type)
  {
    switch (cksum_type) {
    case Type::blake3:
      return Blake3();
      break;
    case Type::sha256:
      return SHA256();
      break;
    case Type::crc32:
      return Crc32();
      break;
    case Type::crc32c:
      return Crc32c();
      break;
    case Type::xxh3:
      return XXH3();
      break;
    case Type::sha512:
      return SHA512();
      break;
    case Type::sha1:
      return SHA1();
      break;
    case Type::none:
      break;
    };
    return boost::blank();
  } /* digest_factory */

  static inline Cksum finalize_digest(Digest* digest, Type type)
  {
    Cksum cksum(type);
    if (digest) {
      auto data = cksum.digest.data();
      digest->Final(data);
    }
    return cksum;
  }

}} /* namespace */

#endif /* RGW_CKSUM_H */
