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

#include "rgw_object_ownership.h"
#include "rgw_common.h"
#include "rgw_xml.h"

namespace rgw::s3 {

std::string to_string(ObjectOwnership ownership)
{
  switch (ownership) {
    case ObjectOwnership::BucketOwnerEnforced:
      return "BucketOwnerEnforced";
    case ObjectOwnership::BucketOwnerPreferred:
      return "BucketOwnerPreferred";
    case ObjectOwnership::ObjectWriter:
      return "ObjectWriter";
    default:
      return "invalid";
  }
}

bool parse(std::string_view input, ObjectOwnership& ownership,
           std::string& error_message)
{
  if (input == "BucketOwnerEnforced") {
    ownership = ObjectOwnership::BucketOwnerEnforced;
    return true;
  }
  if (input == "BucketOwnerPreferred") {
    ownership = ObjectOwnership::BucketOwnerPreferred;
    return true;
  }
  if (input == "ObjectWriter") {
    ownership = ObjectOwnership::ObjectWriter;
    return true;
  }
  error_message = "ObjectOwnership must be one of "
      "BucketOwnerEnforced | BucketOwnerPreferred | ObjectWriter";
  return false;
}

bool OwnershipControls::decode_xml(XMLObj* xml, std::string& error_message)
{
  XMLObj* r = xml->find_first("Rule");
  if (!r) {
    error_message = "Missing required element Rule";
    return false;
  }
  XMLObj* o = r->find_first("ObjectOwnership");
  if (!o) {
    error_message = "Missing required element ObjectOwnership";
    return false;
  }
  return parse(o->get_data(), object_ownership, error_message);
}

void OwnershipControls::dump_xml(Formatter *f) const
{
  auto rule = Formatter::ObjectSection{*f, "Rule"};
  encode_xml("ObjectOwnership", to_string(object_ownership), f);
}

void encode(const OwnershipControls& c, bufferlist& bl, uint64_t f)
{
  ENCODE_START(1, 1, bl);
  encode(c.object_ownership, bl);
  ENCODE_FINISH(bl);
}

void decode(OwnershipControls& c, bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(c.object_ownership, bl);
  DECODE_FINISH(bl);
}

ObjectOwnership get_object_ownership(const sal::Attrs& attrs)
{
  auto i = attrs.find(RGW_ATTR_OWNERSHIP_CONTROLS);
  if (i == attrs.end()) {
    // default to ObjectWriter for backward compat
    return ObjectOwnership::ObjectWriter;
  }

  try {
    OwnershipControls ownership;
    auto p = i->second.cbegin();
    decode(ownership, p);
    return ownership.object_ownership;
  } catch (const buffer::error&) {
    return ObjectOwnership::ObjectWriter;
  }
}

} // namespace rgw::s3
