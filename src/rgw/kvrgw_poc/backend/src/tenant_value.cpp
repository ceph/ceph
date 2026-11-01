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

#include "tenant_value.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <cstring>

namespace kvrgw {

namespace {

void append_uint32_be(std::string &out, uint32_t value)
{
  uint32_t net = htonl(value);
  out.append(reinterpret_cast<const char *>(&net), sizeof(net));
}

void append_int64_be(std::string &out, int64_t value)
{
  uint64_t net = htobe64(static_cast<uint64_t>(value));
  out.append(reinterpret_cast<const char *>(&net), sizeof(net));
}

uint32_t read_uint32_be(std::string_view data, size_t offset)
{
  assert(data.size() >= offset + sizeof(uint32_t));
  uint32_t net{};
  std::memcpy(&net, data.data() + offset, sizeof(net));
  return be32toh(net);
}

int64_t read_int64_be(std::string_view data, size_t offset)
{
  assert(data.size() >= offset + sizeof(int64_t));
  uint64_t net{};
  std::memcpy(&net, data.data() + offset, sizeof(net));
  return static_cast<int64_t>(be64toh(net));
}

} // namespace

std::string make_tenant_value(tenant_id_t tenant_id, int64_t created_at_unix)
{
  std::string value;
  value.reserve(sizeof(tenant_id_t) + sizeof(int64_t));
  append_uint32_be(value, tenant_id);
  append_int64_be(value, created_at_unix);
  return value;
}

std::optional<TenantValue> parse_tenant_value(std::string_view data)
{
  if (data.size() < sizeof(tenant_id_t) + sizeof(int64_t)) {
    return std::nullopt;
  }
  TenantValue value;
  value.tenant_id = read_uint32_be(data, 0);
  value.created_at_unix = read_int64_be(data, sizeof(tenant_id_t));
  return value;
}

} // namespace kvrgw
