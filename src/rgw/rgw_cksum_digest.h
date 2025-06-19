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

#include <boost/variant.hpp>
#include <boost/blank.hpp>
#include "common/ceph_crypto.h"
#include "rgw_blake3_digest.h"
#include "rgw_crc_digest.h"
#include "rgw_xxh_digest.h"

#include "rgw_cksum.h"

namespace rgw { namespace cksum {

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
    TDigest() {}
    TDigest(TDigest&& rhs) noexcept
      : d(std::move(rhs.d))
    {}
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

  typedef TDigest<rgw::digest::Blake3> Blake3;
  typedef TDigest<rgw::digest::Crc32> Crc32;
  typedef TDigest<rgw::digest::Crc32c> Crc32c;
  typedef TDigest<rgw::digest::XXH3> XXH3;
  typedef TDigest<ceph::crypto::SHA1> SHA1;
  typedef TDigest<ceph::crypto::SHA256> SHA256;
  typedef TDigest<ceph::crypto::SHA512> SHA512;
  typedef TDigest<rgw::digest::Crc64Nvme> Crc64Nvme;

  typedef boost::variant<boost::blank,
			 Blake3,
			 Crc32,
			 Crc32c,
			 XXH3,
			 SHA1,
			 SHA256,
			 SHA512,
			 Crc64Nvme> DigestVariant;

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
    Digest* operator()(Crc64Nvme& digest) const { return &digest; }
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
    case Type::crc64nvme:
      return Crc64Nvme();
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
