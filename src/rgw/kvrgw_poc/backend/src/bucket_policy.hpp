// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <string>

namespace kvrgw {

constexpr uint8_t kDenyRead         = 1 << 0;
constexpr uint8_t kDenyWrite        = 1 << 1;
constexpr uint8_t kDenyList         = 1 << 2;
constexpr uint8_t kDenyDeleteBucket = 1 << 3;

// Parse AWS-format bucket policy JSON.
// Scans for Effect:"Deny" + Principal:"*" statements.
// Maps recognized actions to flag bits. Ignores unrecognized content.
uint8_t parse_policy_flags(std::string_view policy_json);

}  // namespace kvrgw
