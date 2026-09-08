// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <stdint.h>
#include <stdio.h>
#include "rgw_crc_digest.h"

#define XXH_INLINE_ALL 1 /* required for streaming variants */
#include "xxhash.h"

namespace rgw { namespace digest {

  class XXHash3 {
  private:
    XXH3_state_t s;

  public:
    static constexpr uint16_t digest_size = 8;

    XXHash3() {
      XXH3_INITSTATE(&s);
      Restart();
    }

    void Restart() { XXH3_64bits_reset(&s); }

    void Update(const unsigned char *data, uint64_t len) {
      XXH3_64bits_update(&s, data, len);
    }

    void Final(unsigned char* digest) {
      XXH64_hash_t final = XXH3_64bits_digest(&s);
      if constexpr (std::endian::native != std::endian::big) {
	final = rgw::digest::byteswap(final);
      }
      memcpy((char*) digest, &final, sizeof(final));
    }
  }; /* XXHash3 */

  class XXHash64 {
  private:
    XXH64_state_t s;

  public:
    static constexpr uint16_t digest_size = 8;

    XXHash64() {
      Restart();
    }

    void Restart() { XXH64_reset(&s, 0); }

    void Update(const unsigned char *data, uint64_t len) {
      XXH64_update(&s, data, len);
    }

    void Final(unsigned char* digest) {
      XXH64_hash_t final = XXH64_digest(&s);
      if constexpr (std::endian::native != std::endian::big) {
	final = rgw::digest::byteswap(final);
      }
      memcpy((char*) digest, &final, sizeof(final));
    }
  }; /* XXHash64 */

  class XXHash128 {
  private:
    XXH3_state_t s;

  public:
    static constexpr uint16_t digest_size = 16;

    XXHash128() {
      XXH3_INITSTATE(&s);
      Restart();
    }

    void Restart() { XXH3_128bits_reset(&s); }

    void Update(const unsigned char *data, uint64_t len) {
      XXH3_128bits_update(&s, data, len);
    }

    void Final(unsigned char* digest) {
      XXH128_hash_t final = XXH3_128bits_digest(&s);
      if constexpr (std::endian::native != std::endian::big) {
	final.low64 = rgw::digest::byteswap(final.low64);
	final.high64 = rgw::digest::byteswap(final.high64);
      }
      memcpy((char*) digest, &final.high64, sizeof(final.high64));
      memcpy((char*) digest + sizeof(final.high64), &final.low64, sizeof(final.low64));
    }
  }; /* XXHash128 */
}} /* namespace */
