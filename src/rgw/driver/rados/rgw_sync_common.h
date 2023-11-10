// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <cstdint>
#include <string>

#include "include/buffer.h"
#include "include/encoding.h"

#include "common/Formatter.h"

namespace rgw::sync {
namespace buffer = ceph::buffer;
struct error_info {
  std::string source_zone;
  std::uint32_t error_code = 0;
  std::string message;

  error_info() = default;
  error_info(std::string source_zone, std::uint32_t error_code,
	     std::string message)
    : source_zone(std::move(source_zone)), error_code(error_code),
      message(std::move(message)) {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source_zone, bl);
    encode(error_code, bl);
    encode(message, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source_zone, bl);
    decode(error_code, bl);
    decode(message, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(error_info)
}
