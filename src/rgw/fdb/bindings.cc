// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
      
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include "bindings.h"

namespace ceph::libfdb::detail {

// JFW: I don't love being here, but a bit more thought will be needed to work around it:
std::pair<std::string, std::string> to_decoded_kv_pair(const FDBKeyValue kv)
{
 std::pair<std::string, std::string> r;

 r.first.assign((const char *)kv.key, static_cast<std::string::size_type>(kv.key_length));

 ceph::libfdb::from::convert(std::span<const std::uint8_t>(kv.value, kv.value_length), r.second);

 return r;
}

} // namespace ceph::libfdb::detail

