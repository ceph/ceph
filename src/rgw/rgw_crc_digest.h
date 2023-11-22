// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include <bit>
#include <array>
#include <concepts>
#include <algorithm>
#include <stdio.h>
#include "include/crc32c.h"
#include <boost/crc.hpp>

namespace rgw { namespace digest {

  /* crib impl of c++23 std::byteswap from
   * https://en.cppreference.com/w/cpp/numeric/byteswap */
  template <std::integral T> constexpr T byteswap(T value) noexcept {
    static_assert(std::has_unique_object_representations_v<T>,
		  "T may not have padding bits");
    auto value_representation =
      std::bit_cast<std::array<std::byte, sizeof(T)>>(value);
    std::ranges::reverse(value_representation);
    return std::bit_cast<T>(value_representation);
  } /* byteswap */

  /* impl. using  boost::crc, as configured by imtzw  */
  class Crc32 {
  private:
    using crc32_type = boost::crc_optimal<
    32, 0x04C11DB7, 0xFFFFFFFF, 0xFFFFFFFF, true, true>;
    crc32_type crc;

  public:
    static constexpr uint16_t digest_size = 4;

    Crc32() { Restart(); }

    void Restart() { crc.reset(); }

    void Update(const unsigned char *data, uint64_t len) {
      crc.process_bytes(data, len);
    }

    void Final(unsigned char* digest) {
      /* XXX crc32 and cksfb utilities both treat the byteswapped result
       * as canonical--possibly this needs to be omitted when BigEndian? */
      uint32_t final = crc();
      if constexpr (std::endian::native != std::endian::big) {
	final = rgw::digest::byteswap(final);
      }
      memcpy((char*) digest, &final, sizeof(final));
    }
  }; /* Crc32 */

  /* use Ceph hw-specialized crc32c (0x1EDC6F41) */
  class Crc32c {
  private:
    uint32_t crc;

  public:
    static constexpr uint16_t digest_size = 4;
    static constexpr uint32_t initial_value = 0xffffffff;

    Crc32c() { Restart(); }

    void Restart() { crc = initial_value; }

    void Update(const unsigned char *data, uint64_t len) {
      crc = ceph_crc32c(crc, data, len);
    }

    void Final(unsigned char* digest) {
      crc = crc ^ 0xffffffff;
      if constexpr (std::endian::native != std::endian::big) {
	crc = rgw::digest::byteswap(crc);
      }
      memcpy((char*) digest, &crc, sizeof(crc));
    }
  }; /* Crc32c */
}} /* namespace */
