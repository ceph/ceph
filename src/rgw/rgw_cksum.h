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

#include <stdint.h>
#include <string>
#include <array>
#include <functional>
#include <boost/variant.hpp>
#include <boost/blank.hpp>
#include "common/ceph_crypto.h"
#include "rgw_common.h"
#include "blake2/sse/blake2.h"

namespace rgw { namespace cksum {

  class Desc
  {
  public:
    CksumType type;
    const char* name;
    uint16_t digest_size;
    constexpr Desc(CksumType _type, const char* _name, uint16_t _size)
      : type(_type), name(_name), digest_size(_size)
      {}
  };

  class Cksum {
  public:
    static constexpr std::array<Desc, 4> checksums =
    {
      Desc(CksumType::none, "none", 0),
      Desc(CksumType::sha256, "SHA256", 32),
      Desc(CksumType::sha512, "SHA512", 64),
      Desc(CksumType::blake2bp, "Blake2B", 64)
    };

    static constexpr uint16_t max_digest_size = 64;
    using value_type = std::array<unsigned char, max_digest_size>;

    CksumType type;
    value_type digest;

    Cksum(CksumType _type) : type(_type) {}

    std::string to_string()  {
      std::string hs;
      const auto& ckd = checksums[uint16_t(type)];
      char hex[ckd.digest_size * 2 + 1];
      buf_to_hex(digest.data(), ckd.digest_size, hex);
      hs.reserve(ckd.digest_size * 2 + 1 + 10);
      hs += ckd.name;
      hs += "::";
      hs += hex;
      return std::move(hs);
    }
  }; /* Cksum */

  static inline CksumType parse_cksum_type(const std::string& name)
  {
    for (const auto& ck : Cksum::checksums) {
      if (ck.name == name)
	return ck.type;
    }
    return CksumType::none;
  }

  static inline const char* to_string(const CksumType type)
  {
    return (Cksum::checksums[uint16_t(type)]).name;
  }

  class Digest {
  public:
    virtual void Restart() = 0;
    virtual void Update (const unsigned char *input, size_t length) = 0;
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
    void Final(unsigned char* digest) override {
      d.Final(digest);
    }
  };

  typedef TDigest<ceph::crypto::Blake2B> Blake2B;
  typedef TDigest<ceph::crypto::SHA256> SHA256;
  typedef TDigest<ceph::crypto::SHA512> SHA512;
  
  typedef boost::variant<boost::blank,
			 Blake2B,
			 SHA256,
			 SHA512> DigestVariant;

  struct get_digest_ptr : public boost::static_visitor<Digest*>
  {
    Digest* operator()(const boost::blank& b) const { return nullptr; }
    Digest* operator()(Blake2B& digest) const { return &digest; }
    Digest* operator()(SHA256& digest) const { return &digest; }
    Digest* operator()(SHA512& digest) const { return &digest; }
  };

  static inline Digest* get_digest(DigestVariant& ev)
  {
    return boost::apply_visitor(get_digest_ptr{}, ev);
  }

  static inline DigestVariant digest_factory(const CksumType cksum_type)
  {
    switch (cksum_type) {
    case CksumType::blake2bp:
      return Blake2B();
      break;
    case CksumType::sha256:
      return SHA256();
      break;
    case CksumType::sha512:
      return SHA512();
      break;
    case CksumType::none:
      break;
    };
    return boost::blank();
  }

  static inline Cksum finalize_digest(Digest* digest, CksumType type)
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
