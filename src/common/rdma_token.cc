// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/rdma_token.h"
#include "common/crc64nvme.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <iterator>

#include <fmt/format.h>

#include "common/Formatter.h"

#ifdef WITH_OOB_OFI
// for its checks that the tokens ofi-rma writes are the ones this code takes
#include "common/ofi_rma.h"
#endif

extern "C" {
#include "common/madler/crc32iscsi.h"
}

namespace ceph::rdma {

namespace {

/// the libfabric token family without its version: "ofi" of "ofi1"
constexpr std::string_view ofi_family = [] {
  return OFI_TOKEN_TAG.substr(0, OFI_TOKEN_TAG.find_first_of("0123456789"));
}();
static_assert(!ofi_family.empty() && ofi_family.size() < OFI_TOKEN_TAG.size());

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

std::string format_cuobj_descriptor(uint64_t addr, uint32_t size,
				    uint32_t rkey, uint16_t lid,
				    uint32_t dctn,
				    std::span<const uint8_t, 16> gid)
{
  // has_gid is always 1: a writer addresses the target by its GID
  std::string token = fmt::format("{:016x}:{:08x}:{:08x}:{:04x}:{:06x}:1:",
				  addr, size, rkey, lid, dctn);
  for (uint8_t b : gid) {
    fmt::format_to(std::back_inserter(token), "{:02x}", b);
  }
  return token;
}

bool is_ofi_token(std::string_view token)
{
  const auto first = token.find(':');
  if (first == std::string_view::npos) {
    return false;
  }
  const auto second = token.find(':', first + 1);
  if (second == std::string_view::npos) {
    return false;
  }
  // the third field runs to the next colon, or to the end: "ofi" and a
  // version, which no cuObject memory key can be, since 'o' is not hex.
  // ofi-rma's ofi_rma_token_family() also wants a fourth field and a
  // version from 1 up. This is looser, so that the libfabric code, not
  // the cuObject code, declines a malformed libfabric token
  auto tag = token.substr(second + 1);
  tag = tag.substr(0, tag.find(':'));
  if (tag.size() <= ofi_family.size() || !tag.starts_with(ofi_family)) {
    return false;
  }
  return std::all_of(tag.begin() + ofi_family.size(), tag.end(), [](char c) {
    return c >= '0' && c <= '9';
  });
}

bool is_cuobj_descriptor(std::string_view token)
{
  return !is_ofi_token(token) && parse_rdma_token(token).has_value();
}

namespace {

/// fold the values that get() picks out of ranges sorted by ofs, with
/// combine(crc_a, crc_b, len_b); nullopt for a gap, an overlap or none
template <typename T, typename Get, typename Combine>
std::optional<T> fold_ranges(std::vector<crc_range_t> ranges, Get get,
			     Combine combine)
{
  if (ranges.empty()) {
    return std::nullopt;
  }
  // an empty range (a share past the end of the object) at the offset of
  // another goes first, whatever order they came in
  std::sort(ranges.begin(), ranges.end(),
	    [](const crc_range_t& a, const crc_range_t& b) {
	      return a.ofs != b.ofs ? a.ofs < b.ofs : a.len < b.len;
	    });
  T crc = get(ranges.front());
  uint64_t next = ranges.front().ofs + ranges.front().len;
  for (size_t i = 1; i < ranges.size(); ++i) {
    const auto& r = ranges[i];
    if (r.ofs != next) {
      return std::nullopt;  // gap or overlap
    }
    crc = combine(crc, get(r), r.len);
    next += r.len;
  }
  return crc;
}

} // anonymous namespace

std::optional<uint64_t> fold_crc64_ranges(std::vector<crc_range_t> ranges)
{
  return fold_ranges<uint64_t>(
    std::move(ranges), [](const crc_range_t& r) { return r.crc64; },
    [](uint64_t a, uint64_t b, uint64_t len) {
      return crc64nvme_combine(a, b, len);
    });
}

std::optional<uint32_t> fold_crc32c_ranges(std::vector<crc_range_t> ranges)
{
  return fold_ranges<uint32_t>(
    std::move(ranges), [](const crc_range_t& r) { return r.crc32c; },
    crc32c_combine);
}

uint32_t crc32c_canonical(const ceph::buffer::list& bl)
{
  // ceph_crc32c() leaves out the final inversion: S3 has both
  return bl.crc32c(0xffffffff) ^ 0xffffffff;
}

uint32_t crc32c_combine(uint32_t crc_a, uint32_t crc_b, uint64_t len_b)
{
  // madler's, which rgw combines multipart CRC32C checksums with too
  return crc32iscsi_comb(crc_a, crc_b, len_b);
}

std::string libfabric_name(std::string_view name)
{
  // every libfabric utility provider's name starts with ofi_, and no
  // core provider's name holds _ofi_
  std::string n{name};
  if (const auto pos = n.find("_ofi_"); pos != std::string::npos && pos > 0) {
    n[pos] = ';';
  }
  // verbs reaches RDM endpoints only through ofi_rxm, which libfabric
  // layers on it by itself; name it, so that the choice is not
  // libfabric's
  if (n == "verbs") {
    n = "verbs;ofi_rxm";
  }
  return n;
}

std::vector<std::string> parse_transport_list(std::string_view list)
{
  std::vector<std::string> out;
  size_t pos = 0;
  while (pos <= list.size()) {
    const size_t end = list.find_first_of(", \t", pos);
    auto name = list.substr(pos, end == std::string_view::npos ?
			    std::string_view::npos : end - pos);
    if (!name.empty()) {
      std::string n{name};
      std::transform(n.begin(), n.end(), n.begin(),
		     [](unsigned char c) { return std::tolower(c); });
      n = libfabric_name(n);
      if (std::find(out.begin(), out.end(), n) == out.end()) {
	out.push_back(std::move(n));
      }
    }
    if (end == std::string_view::npos) {
      break;
    }
    pos = end + 1;
  }
  return out;
}

bool is_libfabric_provider(std::string_view name)
{
  return !name.empty() && name != TRANSPORT_CUOBJ;
}

bool oob_transports::client_names(std::string_view t) const
{
  return std::find(client.begin(), client.end(), t) != client.end();
}

bool oob_transports::client_ofi() const
{
  return std::any_of(client.begin(), client.end(),
		     [](const std::string& t) {
		       return is_libfabric_provider(t);
		     });
}

oob_transports parse_oob_transports(std::string_view client,
				    std::string_view cluster)
{
  oob_transports t;
  t.client = parse_transport_list(client);
  const auto c = parse_transport_list(cluster);
  if (!c.empty()) {
    t.cluster = c.front();
    if (c.size() > 1) {
      t.warnings.push_back("oob_cluster_transport names one transport; "
			   "using " + t.cluster);
    }
  }
  if (t.cluster_ofi()) {
    t.ofi_provider = t.cluster;
  }
  for (const auto& n : t.client) {
    if (!is_libfabric_provider(n) || n == t.ofi_provider) {
      continue;
    }
    if (t.ofi_provider.empty()) {
      t.ofi_provider = n;
    } else {
      t.warnings.push_back("one libfabric endpoint, for " + t.ofi_provider +
			   "; " + n + " is not served");
    }
  }
  return t;
}

void delivery_t::dump(ceph::Formatter* f) const
{
  f->dump_string("token", token);
  f->dump_unsigned("base_offset", base_offset);
  f->dump_unsigned("flags", flags);
}

std::list<delivery_t> delivery_t::generate_test_instances()
{
  std::list<delivery_t> o;
  o.emplace_back();
  o.push_back({"7f0012345000:400000:ofi1:xnet.1:0200805b7f000001:1", 4096,
	       FLAG_CRC64NVME | FLAG_PULL});
  return o;
}

void crc_range_t::dump(ceph::Formatter* f) const
{
  f->dump_unsigned("ofs", ofs);
  f->dump_unsigned("len", len);
  f->dump_unsigned("crc64", crc64);
  f->dump_unsigned("crc32c", crc32c);
}

std::list<crc_range_t> crc_range_t::generate_test_instances()
{
  std::list<crc_range_t> o;
  o.emplace_back();
  o.push_back({65536, 4096, 0xae8b14860a799888ull, 0xe3069283});
  return o;
}

void oob_result_t::dump(ceph::Formatter* f) const
{
  f->dump_unsigned("bytes", bytes);
  f->dump_unsigned("crc64", crc64);
  f->dump_unsigned("flags", flags);
  f->open_array_section("ranges");
  for (const auto& r : ranges) {
    f->open_object_section("range");
    r.dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("crc32c", crc32c);
}

std::list<oob_result_t> oob_result_t::generate_test_instances()
{
  std::list<oob_result_t> o;
  o.emplace_back();
  oob_result_t r;
  r.bytes = 8192;
  r.crc64 = 0xae8b14860a799888ull;
  r.flags = FLAG_CRC64NVME | FLAG_RANGES | FLAG_CRC32C;
  r.ranges = {{0, 4096, 1, 2}, {4096, 4096, 3, 4}};
  r.crc32c = 0xe3069283;
  o.push_back(r);
  return o;
}

} // namespace ceph::rdma
