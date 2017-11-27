// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RGW_ETAG_H
#define RGW_ETAG_H

#include <stdint.h>
#include <string>
#include <boost/variant.hpp>
#include <boost/blank.hpp>
#include "common/ceph_crypto.h"
#include "rgw_common.h"
#include "blake2/sse/blake2.h"

namespace rgw {

  enum class EtagTypes : uint16_t
  {
      none,
      md5,
      blake2bp_1,
      keccak_1,
  };

  class Etag
  {
  protected:
    Etag() = delete;
    EtagTypes type;
    Etag(EtagTypes _type) : type(_type) {}
  public:
    EtagTypes get_type() const { return type; }
    virtual void update(const char* data, uint64_t length) = 0;
    virtual std::string final() = 0;
  };

  using ceph::crypto::MD5;

  class EtagMD5 : public Etag
  {
    MD5 hash;
  public:
    EtagMD5() : Etag(EtagTypes::md5) {}

    void update(const char* data, uint64_t len) override {
      hash.Update(reinterpret_cast<const byte*>(data), len);
    }

    std::string final() override {
      unsigned char hmac[CEPH_CRYPTO_MD5_DIGESTSIZE];
      char hex[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
      hash.Final(hmac);
      buf_to_hex(hmac, CEPH_CRYPTO_MD5_DIGESTSIZE, hex);
      return std::move(std::string(hex));
    }
  };

  class EtagBlake2bp_1 : public Etag
  {
    blake2bp_state s;
  public:
    EtagBlake2bp_1() : Etag(EtagTypes::blake2bp_1) {
      (void) blake2bp_init(&s, BLAKE2B_OUTBYTES /* 64 */);
    }

    void update(const char* data, uint64_t len) override {
      blake2bp_update(&s, data, len);
    }

    std::string final() override {
      std::string hs;
      unsigned char hmac[BLAKE2B_OUTBYTES];
      char hex[BLAKE2B_OUTBYTES * 2 + 1];
      blake2bp_final(&s, &hmac, BLAKE2B_OUTBYTES);
      buf_to_hex(hmac, BLAKE2B_OUTBYTES, hex);
      hs.reserve(BLAKE2B_OUTBYTES * 2 + 1 + 10);
      hs += "BLAKE2B1:";
      hs += hex;
      return std::move(hs);
    }
  };

  class EtagKeccak_1 : public Etag
  {
  public:
    EtagKeccak_1() : Etag(EtagTypes::keccak_1) {}
  };

  typedef boost::variant<boost::blank, EtagMD5, EtagBlake2bp_1> EtagVariant;

  struct get_etag_ptr : public boost::static_visitor<Etag*>
  {
    Etag* operator()(const boost::blank& b) const { return nullptr; }
    Etag* operator()(EtagMD5& etag) const { return &etag; }
    Etag* operator()(EtagBlake2bp_1& etag) const { return &etag; }
  };

  static inline Etag* get_etag(EtagVariant& ev)
  {
    return boost::apply_visitor(get_etag_ptr{}, ev);
  }

  static inline EtagTypes parse_etag_type(const std::string& format)
  {
    if (format == "BLAKE2B1")
      return EtagTypes::blake2bp_1;
    return EtagTypes::md5;
  }

  static inline EtagVariant etag_factory(const std::string& format)
  {
    rgw::EtagTypes etag_type = parse_etag_type(format);
    switch (etag_type) {
    case EtagTypes::blake2bp_1:
      return EtagBlake2bp_1();
      break;
    default:
      return EtagMD5();
    };
    /* not reached */
  }

} /* namespace rgw */

#endif /* RGW_ETAG_H */
