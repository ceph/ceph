// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include "include/encoding.h"
#include "rgw_sal_fwd.h"

class XMLObj;
namespace ceph { class Formatter; }

namespace rgw::s3 {

/// S3 Object Ownership configuration
enum class ObjectOwnership : uint8_t {
  BucketOwnerEnforced,
  BucketOwnerPreferred,
  ObjectWriter,
};

/// Format ObjectOwnership configuration as a string
std::string to_string(ObjectOwnership ownership);

/// Parse ObjectOwnership configuration from a string
bool parse(std::string_view input, ObjectOwnership& ownership,
           std::string& error_message);


/// Ownership controls for a bucket, encoded in RGW_ATTR_OWNERSHIP_CONTROLS.
struct OwnershipControls {
  ObjectOwnership object_ownership = ObjectOwnership::ObjectWriter;

  bool decode_xml(XMLObj* xml, std::string& error_message);
  void dump_xml(ceph::Formatter* f) const;
};

void encode(const OwnershipControls&, bufferlist&, uint64_t f=0);
void decode(OwnershipControls&, bufferlist::const_iterator&);

/// Return the ObjectOwnership from RGW_ATTR_OWNERSHIP_CONTROLS,
/// or default to ObjectWriter.
ObjectOwnership get_object_ownership(const sal::Attrs& attrs);

} // namespace rgw::s3
