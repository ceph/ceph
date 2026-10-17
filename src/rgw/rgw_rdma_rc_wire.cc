// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rdma_rc_wire.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <iterator>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>

#include <fmt/format.h>

#include "common/crc64nvme.h"
#include "common/dout.h"
#include "common/errno.h"
#include "rgw_auth_s3.h"
#include "rgw_common.h"
#include "rgw_hex.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::rdma::rc {

namespace {

bool all_hex(std::string_view s)
{
  return !s.empty() &&
    std::all_of(s.begin(), s.end(),
                [](unsigned char c) { return std::isxdigit(c); });
}

// an unsigned number of 1 to max_digits digits in base, with no sign,
// prefix or blanks
template <typename T>
std::optional<T> parse_digits(std::string_view s, int base, size_t max_digits)
{
  if (s.empty() || s.size() > max_digits) {
    return std::nullopt;
  }
  T v = 0;
  const auto end = s.data() + s.size();
  auto [ptr, ec] = std::from_chars(s.data(), end, v, base);
  if (ec != std::errc() || ptr != end) {
    return std::nullopt;
  }
  return v;
}

// fixed-width hex field of exactly `digits` digits, never 0
std::optional<uint32_t> parse_fixed_hex(std::string_view s, size_t digits)
{
  if (s.size() != digits) {
    return std::nullopt;
  }
  auto v = parse_digits<uint32_t>(s, 16, digits);
  if (!v || *v == 0) {
    return std::nullopt;
  }
  return v;
}

void put_le(uint8_t* p, uint64_t v, size_t bytes)
{
  for (size_t i = 0; i < bytes; i++) {
    p[i] = static_cast<uint8_t>(v >> (8 * i));
  }
}

uint64_t get_le(const uint8_t* p, size_t bytes)
{
  uint64_t v = 0;
  for (size_t i = 0; i < bytes; i++) {
    v |= static_cast<uint64_t>(p[i]) << (8 * i);
  }
  return v;
}

/// 16 bytes of an IPv6 address or a GID: ::1, or IPv4-mapped 127/8
bool is_loopback_gid(const uint8_t* g)
{
  static constexpr uint8_t v4_mapped[12] = {0, 0, 0, 0, 0, 0,
                                            0, 0, 0, 0, 0xff, 0xff};
  if (std::equal(v4_mapped, v4_mapped + 12, g)) {
    return g[12] == 127;
  }
  static constexpr uint8_t v6_loopback[16] = {0, 0, 0, 0, 0, 0, 0, 0,
                                              0, 0, 0, 0, 0, 0, 0, 1};
  return std::equal(v6_loopback, v6_loopback + 16, g);
}

} // anonymous namespace

bool token_t::gid_is_zero() const
{
  return std::all_of(std::begin(gid), std::end(gid),
                     [](uint8_t b) { return b == 0; });
}

std::string encode_token(const token_t& t)
{
  std::array<uint8_t, TOKEN_BINARY_LEN> buf;
  size_t off = 0;
  buf[off++] = static_cast<uint8_t>(t.transport);
  put_le(&buf[off], t.qpn, 4); off += 4;
  std::memcpy(&buf[off], t.gid, 16); off += 16;
  put_le(&buf[off], t.rkey, 4); off += 4;
  put_le(&buf[off], t.addr, 8); off += 8;
  put_le(&buf[off], t.length, 8); off += 8;
  buf[off++] = t.port;
  put_le(&buf[off], t.lid, 2); off += 2;

  std::string out;
  out.reserve(TOKEN_HEX_LEN);
  buf_to_hex(buf, std::back_inserter(out));
  return out;
}

std::optional<token_t> decode_token(std::string_view hex)
{
  if (hex.size() != TOKEN_HEX_LEN || !all_hex(hex)) {
    return std::nullopt;
  }
  std::array<uint8_t, TOKEN_BINARY_LEN> buf;
  for (size_t i = 0; i < TOKEN_BINARY_LEN; i++) {
    const char* p = hex.data() + 2 * i;
    std::from_chars(p, p + 2, buf[i], 16);
  }
  token_t t;
  size_t off = 0;
  t.transport = static_cast<transport_t>(buf[off++]);
  t.qpn = static_cast<uint32_t>(get_le(&buf[off], 4)); off += 4;
  std::memcpy(t.gid, &buf[off], 16); off += 16;
  t.rkey = static_cast<uint32_t>(get_le(&buf[off], 4)); off += 4;
  t.addr = get_le(&buf[off], 8); off += 8;
  t.length = get_le(&buf[off], 8); off += 8;
  t.port = buf[off++];
  t.lid = static_cast<uint16_t>(get_le(&buf[off], 2));
  return t;
}

bool token_is_zero(std::string_view hex)
{
  return hex.size() == TOKEN_HEX_LEN &&
    std::all_of(hex.begin(), hex.end(), [](char c) { return c == '0'; });
}

bool is_loopback_address(std::string_view addr)
{
  std::string a{addr};
  in_addr a4{};
  in6_addr a6{};
  if (inet_pton(AF_INET, a.c_str(), &a4) == 1) {
    return (ntohl(a4.s_addr) >> 24) == 127;
  }
  // a scoped address is never a loopback one, and inet_pton() takes no
  // scope
  if (inet_pton(AF_INET6, a.c_str(), &a6) != 1) {
    return false;
  }
  // an IPv4 client of a listener on an IPv6 socket too
  return is_loopback_gid(a6.s6_addr);
}

std::optional<gid_bytes> roce_gid_of(const sockaddr* sa)
{
  if (!sa) {
    return std::nullopt;
  }
  gid_bytes g{};
  if (sa->sa_family == AF_INET6) {
    const auto* a6 = reinterpret_cast<const sockaddr_in6*>(sa);
    std::memcpy(g.data(), a6->sin6_addr.s6_addr, g.size());
    return g;
  }
  if (sa->sa_family == AF_INET) {
    const auto* a4 = reinterpret_cast<const sockaddr_in*>(sa);
    g[10] = 0xff;
    g[11] = 0xff;
    std::memcpy(g.data() + 12, &a4->sin_addr.s_addr, 4);
    return g;
  }
  return std::nullopt;
}

void add_interface_gids(const ifaddrs* ifa, gid_set& gids)
{
  for (const ifaddrs* i = ifa; i; i = i->ifa_next) {
    if (auto g = roce_gid_of(i->ifa_addr); g) {
      gids.insert(*g);
    }
  }
}

bool gid_names_host(const uint8_t (&gid)[16], const gid_set& host)
{
  if (is_loopback_gid(gid)) {
    return true;
  }
  gid_bytes g;
  std::copy(gid, gid + 16, g.begin());
  return host.contains(g);
}

bool valid_session_id(std::string_view s)
{
  return s.size() == SESSION_HEX_LEN && all_hex(s);
}

std::optional<uint32_t> parse_psn(std::string_view s)
{
  return parse_fixed_hex(s, 6);
}

std::string format_psn(uint32_t psn)
{
  return fmt::format("{:06X}", psn & 0xffffff);
}

std::optional<uint32_t> parse_cookie(std::string_view s)
{
  return parse_fixed_hex(s, 8);
}

std::string format_cookie(uint32_t cookie)
{
  return fmt::format("{:08X}", cookie);
}

std::optional<uint64_t> parse_hex(std::string_view s)
{
  return parse_digits<uint64_t>(s, 16, 16);
}

std::string format_hex(uint64_t v)
{
  return fmt::format("{:x}", v);
}

std::optional<uint64_t> parse_decimal(std::string_view s)
{
  return parse_digits<uint64_t>(s, 10, 20);
}

std::optional<target_t> parse_target(std::string_view target)
{
  if (target.empty() || target.front() != '/') {
    return std::nullopt;
  }
  target.remove_prefix(1);
  target_t t;
  if (auto q = target.find('?'); q != std::string_view::npos) {
    t.query = std::string{target.substr(q + 1)};
    target = target.substr(0, q);
  }
  const auto slash = target.find('/');
  if (slash == std::string_view::npos || slash == 0 ||
      slash + 1 == target.size()) {
    return std::nullopt;
  }
  auto bucket = percent_decode(target.substr(0, slash));
  auto key = percent_decode(target.substr(slash + 1));
  if (!bucket || !key || bucket->empty() || key->empty()) {
    return std::nullopt;
  }
  t.bucket = std::move(*bucket);
  t.key = std::move(*key);
  return t;
}

std::optional<std::string> percent_decode(std::string_view s)
{
  std::string out;
  out.reserve(s.size());
  for (size_t i = 0; i < s.size(); i++) {
    if (s[i] != '%') {
      out.push_back(s[i]);
      continue;
    }
    if (i + 2 >= s.size()) {
      return std::nullopt;
    }
    auto c = parse_digits<uint8_t>(s.substr(i + 1, 2), 16, 2);
    if (!c) {
      return std::nullopt;
    }
    out.push_back(static_cast<char>(*c));
    i += 2;
  }
  return out;
}

std::string build_target(std::string_view bucket, std::string_view key,
                         std::string_view query)
{
  // RFC 3986 unreserved characters and '/' pass through unescaped
  using rgw::auth::s3::aws4_uri_encode;
  std::string t = "/";
  t += aws4_uri_encode(std::string{bucket}, false);
  t += '/';
  t += aws4_uri_encode(std::string{key}, false);
  if (!query.empty()) {
    t += '?';
    t += query;
  }
  return t;
}

bool all_signed(std::string_view signed_list,
                const std::vector<std::string>& names)
{
  for (const auto& name : names) {
    bool found = false;
    std::string_view rest = signed_list;
    while (!rest.empty() && !found) {
      const auto semi = rest.find(';');
      found = rest.substr(0, semi) == name;
      rest = semi == std::string_view::npos ? std::string_view{}
                                            : rest.substr(semi + 1);
    }
    if (!found) {
      return false;
    }
  }
  return true;
}

int check_signed(const DoutPrefixProvider* dpp, const req_info& info)
{
  using rgw::auth::s3::AWS4_HMAC_SHA256_STR;
  std::vector<std::string> names;
  for (const auto& [k, v] : info.env->get_map()) {
    if (k.rfind("HTTP_X_AMZ_RDMA_", 0) == 0) {
      std::string n = k.substr(5);
      for (auto& c : n) {
        c = c == '_' ? '-' : static_cast<char>(std::tolower(c));
      }
      names.push_back(std::move(n));
    }
  }
  // the signature version, as the S3 authenticator tells it
  const char* auth = info.env->get("HTTP_AUTHORIZATION");
  const bool in_header = auth && auth[0];
  bool v4 = false;
  if (in_header) {
    const std::string_view a{auth};
    if (a.starts_with("AWS ")) {
      return 0;  // SigV2 signs every x-amz-* header
    }
    v4 = a.starts_with(AWS4_HMAC_SHA256_STR);
  } else {
    v4 = info.args.get("x-amz-algorithm") == AWS4_HMAC_SHA256_STR;
    if (!v4 && !info.args.get("AWSAccessKeyId").empty()) {
      return 0;  // presigned SigV2
    }
  }
  if (v4) {
    std::string_view access_key, scope, signed_headers, signature, date,
      session_token;
    if (int r = rgw::auth::s3::parse_v4_credentials(
          info, access_key, scope, signed_headers, signature, date,
          session_token, !in_header, dpp); r < 0) {
      ldpp_dout(dpp, 4) << "hipobj-rc: cannot read the signed headers: "
                        << cpp_strerror(r) << dendl;
      return -EACCES;
    }
    if (all_signed(signed_headers, names)) {
      return 0;
    }
  }
  ldpp_dout(dpp, 4) << "hipobj-rc: request carries unsigned x-amz-rdma-* headers"
                    << dendl;
  return -EACCES;
}

void landed_ranges::add(uint64_t ofs, uint64_t len)
{
  // ranges that overlap or touch merge
  ranges.union_insert(ofs, len);
}

void landed_ranges::add_placed(uint64_t ofs, uint64_t len)
{
  placed.union_insert(ofs, len);
}

void landed_ranges::confirm_placed()
{
  for (auto i = placed.begin(); i != placed.end(); ++i) {
    ranges.union_insert(i.get_start(), i.get_len());
  }
  placed.clear();
}

uint64_t landed_ranges::prefix() const
{
  if (ranges.empty() || ranges.range_start() != 0) {
    return 0;
  }
  return ranges.begin().get_len();
}

void chained_crcs::reset(const cksum_want& w)
{
  want = w;
  fold = crc_fold{w};
}

void chained_crcs::append(const void* p, uint64_t len)
{
  crcs c;
  if (want.crc64nvme) {
    c.crc64 = ceph::crc64nvme(0, p, len);
  }
  if (want.crc32c) {
    c.crc32c = crc32c_of(p, len);
  }
  fold.append(c, len);
}

std::optional<std::vector<push_t>> plan_pushes(uint64_t prefix, uint64_t pushed,
                                               uint64_t total, bool final,
                                               const push_policy& p)
{
  std::vector<push_t> out;
  const uint64_t max_write = std::max<uint64_t>(p.max_write, 1);
  if (!final) {
    const uint64_t stop = total - std::min(total, p.tail_hold);
    const uint64_t limit = std::min(prefix, stop);
    if (limit <= pushed || (limit - pushed < p.min_push && limit < stop)) {
      return out;
    }
    for (uint64_t o = pushed; o < limit; ) {
      const uint64_t n = std::min(limit - o, max_write);
      out.push_back({o, n, false});
      o += n;
    }
    return out;
  }
  if (prefix < total || pushed > total) {
    return std::nullopt;
  }
  uint64_t o = pushed;
  while (total - o > max_write) {
    out.push_back({o, max_write, false});
    o += max_write;
  }
  out.push_back({o, total - o, true});
  return out;
}

} // namespace rgw::rdma::rc
