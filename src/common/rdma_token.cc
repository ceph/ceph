// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"
#include "common/crc64nvme.h"

#include <algorithm>
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

std::optional<uint64_t> fold_crc64_ranges(std::vector<crc_range_t> ranges)
{
  if (ranges.empty()) {
    return std::nullopt;
  }
  std::sort(ranges.begin(), ranges.end(),
	    [](const crc_range_t& a, const crc_range_t& b) {
	      return a.ofs < b.ofs;
	    });
  uint64_t crc = ranges.front().crc64;
  uint64_t next = ranges.front().ofs + ranges.front().len;
  for (size_t i = 1; i < ranges.size(); ++i) {
    const auto& r = ranges[i];
    if (r.ofs != next) {
      return std::nullopt;  // gap or overlap
    }
    crc = crc64nvme_combine(crc, r.crc64, r.len);
    next += r.len;
  }
  return crc;
}

} // namespace ceph::rdma
