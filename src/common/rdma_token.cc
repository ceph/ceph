// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"

#include <charconv>

namespace ceph::rdma {

namespace {

std::optional<uint64_t> parse_hex_field(std::string_view field)
{
  if (field.empty() || field.size() > 16) {
    return std::nullopt;
  }
  uint64_t v = 0;
  auto [ptr, ec] = std::from_chars(field.begin(), field.end(), v, 16);
  if (ec != std::errc() || ptr != field.end()) {
    return std::nullopt;
  }
  return v;
}

} // anonymous namespace

std::optional<token_window> parse_rdma_token(std::string_view token)
{
  if (token.empty() || token.size() > RDMA_TOKEN_MAX_LEN) {
    return std::nullopt;
  }
  const auto first = token.find(':');
  if (first == std::string_view::npos) {
    return std::nullopt;
  }
  const auto second = token.find(':', first + 1);
  if (second == std::string_view::npos) {
    return std::nullopt;
  }
  auto addr = parse_hex_field(token.substr(0, first));
  auto size = parse_hex_field(token.substr(first + 1, second - first - 1));
  if (!addr || !size) {
    return std::nullopt;
  }
  return token_window{*addr, *size};
}

} // namespace ceph::rdma
