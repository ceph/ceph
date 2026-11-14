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

#include "typed_ids.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace kvrgw {

struct TenantValue {
  tenant_id_t tenant_id{};
  int64_t created_at_unix{};
};

std::string make_tenant_value(tenant_id_t tenant_id, int64_t created_at_unix);
std::optional<TenantValue> parse_tenant_value(std::string_view data);

}  // namespace kvrgw
