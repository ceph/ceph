// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw/rgw_rdma_rc_wire.h"
#include "rgw/rgw_rdma_cksum.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <memory>
#include <optional>
#include <vector>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "acconfig.h"
#include "common/ceph_context.h"
#include "common/crc64nvme.h"
#include "common/dout.h"
#include "rgw/rgw_common.h"
#ifdef WITH_RDMA_RC
#include "rgw/rgw_rdma_rc_session.h"
#endif

using namespace rgw::rdma::rc;
using namespace std::chrono_literals;

namespace {

token_t sample_token()
{
  token_t t;
  t.transport = transport_t::RC;
  t.qpn = 0x000123ab;
  for (int i = 0; i < 16; i++) {
    t.gid[i] = static_cast<uint8_t>(0xfe - i);
  }
  t.rkey = 0xdeadbeef;
  t.addr = 0x00007f1234567000ULL;
  t.length = 1ULL << 30;
  t.port = 1;
  t.lid = 0x0042;
  return t;
}

} // anonymous namespace

TEST(RdmaRcWire, TokenRoundTrip)
{
  const auto t = sample_token();
  const auto hex = encode_token(t);
  ASSERT_EQ(TOKEN_HEX_LEN, hex.size());
  // transport byte first, then the little-endian qpn
  EXPECT_EQ("01ab230100", hex.substr(0, 10));
  auto back = decode_token(hex);
  ASSERT_TRUE(back);
  EXPECT_EQ(t, *back);
  EXPECT_FALSE(back->gid_is_zero());
}

TEST(RdmaRcWire, TokenLayoutPinned)
{
  // the layout is fixed by the hipObject client: 1+4+16+4+8+8+1+2
  token_t t;
  t.transport = transport_t::DC;
  t.qpn = 0x04030201;
  t.rkey = 0x08070605;
  t.addr = 0x1011121314151617ULL;
  t.length = 0x2021222324252627ULL;
  t.port = 3;
  t.lid = 0x3132;
  const std::string expected =
    "00" "01020304" "00000000000000000000000000000000" "05060708"
    "1716151413121110" "2726252423222120" "03" "3231";
  EXPECT_EQ(expected, encode_token(t));
  auto back = decode_token(expected);
  ASSERT_TRUE(back);
  EXPECT_EQ(t, *back);
  EXPECT_TRUE(back->gid_is_zero());
}

TEST(RdmaRcWire, TokenDecodeAcceptsEitherCase)
{
  const auto hex = encode_token(sample_token());
  std::string upper = hex;
  for (auto& c : upper) c = std::toupper(c);
  auto a = decode_token(hex);
  auto b = decode_token(upper);
  ASSERT_TRUE(a && b);
  EXPECT_EQ(*a, *b);
}

TEST(RdmaRcWire, TokenDecodeRejectsMalformed)
{
  const auto hex = encode_token(sample_token());
  EXPECT_FALSE(decode_token(hex.substr(1)));
  EXPECT_FALSE(decode_token(hex + "0"));
  EXPECT_FALSE(decode_token(hex + ":1:2"));
  std::string bad = hex;
  bad[10] = 'g';
  EXPECT_FALSE(decode_token(bad));
  EXPECT_FALSE(decode_token(""));
}

TEST(RdmaRcWire, ZeroTokenIsLoopbackMarker)
{
  EXPECT_TRUE(token_is_zero(std::string(TOKEN_HEX_LEN, '0')));
  EXPECT_FALSE(token_is_zero(std::string(TOKEN_HEX_LEN - 1, '0')));
  EXPECT_FALSE(token_is_zero(encode_token(sample_token())));
}

TEST(RdmaRcWire, LoopbackAddress)
{
  // only a connection from a loopback address is from a client on the
  // gateway's host
  EXPECT_TRUE(is_loopback_address("127.0.0.1"));
  EXPECT_TRUE(is_loopback_address("127.4.5.6"));
  EXPECT_TRUE(is_loopback_address("::1"));
  EXPECT_TRUE(is_loopback_address("::ffff:127.0.0.1"));
  // an address of the host's interfaces is not: a proxy on the host, or
  // a container network that translates clients, connects from one
  EXPECT_FALSE(is_loopback_address("10.0.0.5"));
  EXPECT_FALSE(is_loopback_address("::ffff:10.0.0.5"));
  EXPECT_FALSE(is_loopback_address("2001:db8::5"));
  EXPECT_FALSE(is_loopback_address("fe80::1%eth0"));
  // an IPv6 address that holds the IPv4 bytes is another address
  EXPECT_FALSE(is_loopback_address("::127.0.0.1"));
  EXPECT_FALSE(is_loopback_address("::2"));
  EXPECT_FALSE(is_loopback_address(""));
  EXPECT_FALSE(is_loopback_address("localhost"));
  EXPECT_FALSE(is_loopback_address("127.0.0.1:80"));
}

TEST(RdmaRcWire, HostGids)
{
  // the GIDs that RoCE gives the addresses of the host's interfaces
  sockaddr_in v4{};
  v4.sin_family = AF_INET;
  inet_pton(AF_INET, "10.0.0.5", &v4.sin_addr);
  sockaddr_in6 v6{};
  v6.sin6_family = AF_INET6;
  inet_pton(AF_INET6, "2001:db8::5", &v6.sin6_addr);
  sockaddr other{};
  other.sa_family = AF_UNIX;
  ifaddrs third{};
  third.ifa_addr = &other;
  ifaddrs second{};
  second.ifa_addr = reinterpret_cast<sockaddr*>(&v6);
  second.ifa_next = &third;
  ifaddrs no_addr{};
  no_addr.ifa_next = &second;
  ifaddrs first{};
  first.ifa_addr = reinterpret_cast<sockaddr*>(&v4);
  first.ifa_next = &no_addr;
  gid_set host;
  add_interface_gids(&first, host);
  EXPECT_EQ(2u, host.size());

  auto gid = [](const char* addr) {
    in6_addr a{};
    EXPECT_EQ(1, inet_pton(AF_INET6, addr, &a)) << addr;
    token_t t;
    std::memcpy(t.gid, a.s6_addr, sizeof(t.gid));
    return t;
  };
  EXPECT_TRUE(gid_names_host(gid("::ffff:10.0.0.5").gid, host));
  EXPECT_TRUE(gid_names_host(gid("2001:db8::5").gid, host));
  EXPECT_FALSE(gid_names_host(gid("::ffff:10.0.0.6").gid, host));
  EXPECT_FALSE(gid_names_host(gid("2001:db8::6").gid, host));
  EXPECT_FALSE(gid_names_host(gid("::10.0.0.5").gid, host));
  // a loopback address names the host whatever its interfaces have
  EXPECT_TRUE(gid_names_host(gid("::1").gid, gid_set{}));
  EXPECT_TRUE(gid_names_host(gid("::ffff:127.1.2.3").gid, gid_set{}));
  EXPECT_FALSE(gid_names_host(gid("::ffff:128.0.0.1").gid, gid_set{}));
  // and a GID that a port has, such as its link-local default GID
  const auto ll = gid("fe80::9c9c:a0ff:fe7a:e55d");
  gid_bytes b;
  std::copy(ll.gid, ll.gid + 16, b.begin());
  host.insert(b);
  EXPECT_TRUE(gid_names_host(ll.gid, host));
}

TEST(RdmaRcWire, SessionId)
{
  EXPECT_TRUE(valid_session_id("0123456789abcdef0123456789ABCDEF"));
  EXPECT_FALSE(valid_session_id("0123456789abcdef0123456789ABCDE"));
  EXPECT_FALSE(valid_session_id("0123456789abcdef0123456789ABCDEFa"));
  EXPECT_FALSE(valid_session_id("0123456789abcdef0123456789ABCDEz"));
  EXPECT_FALSE(valid_session_id(""));
}

TEST(RdmaRcWire, Psn)
{
  EXPECT_THAT(parse_psn("000001"), testing::Optional(1u));
  EXPECT_THAT(parse_psn("ffffff"), testing::Optional(0xffffffu));
  EXPECT_THAT(parse_psn("0A1B2C"), testing::Optional(0xa1b2cu));
  EXPECT_FALSE(parse_psn("000000"));   // zero is invalid
  EXPECT_FALSE(parse_psn("1"));        // fixed width
  EXPECT_FALSE(parse_psn("0000001"));
  EXPECT_FALSE(parse_psn("00000g"));
  EXPECT_FALSE(parse_psn("+00001"));   // digits only
  EXPECT_FALSE(parse_psn(" 00001"));
  EXPECT_FALSE(parse_psn("-00001"));
  EXPECT_EQ("000001", format_psn(1));
  EXPECT_EQ("FFFFFF", format_psn(0xffffff));
  EXPECT_EQ("000001", format_psn(0x1000001)); // masked to 24 bits
}

TEST(RdmaRcWire, Cookie)
{
  EXPECT_THAT(parse_cookie("1a2b3c4d"), testing::Optional(0x1a2b3c4du));
  EXPECT_THAT(parse_cookie("FFFFFFFF"), testing::Optional(0xffffffffu));
  EXPECT_FALSE(parse_cookie("00000000"));
  EXPECT_FALSE(parse_cookie("1a2b3c4"));
  EXPECT_FALSE(parse_cookie("1a2b3c4d5"));
  EXPECT_EQ("1A2B3C4D", format_cookie(0x1a2b3c4d));
  EXPECT_EQ("00000001", format_cookie(1));
}

TEST(RdmaRcWire, BareHex)
{
  EXPECT_THAT(parse_hex("0"), testing::Optional(0u));
  EXPECT_THAT(parse_hex("7f1234567000"),
              testing::Optional(0x7f1234567000ull));
  EXPECT_THAT(parse_hex("ffffffffffffffff"), testing::Optional(UINT64_MAX));
  EXPECT_FALSE(parse_hex(""));
  EXPECT_FALSE(parse_hex("0x10"));
  EXPECT_FALSE(parse_hex("1ffffffffffffffff"));
  EXPECT_THAT(parse_hex("DEADbeef"), testing::Optional(0xdeadbeefull));
  // digits only, at most 16 of them
  EXPECT_FALSE(parse_hex("-1"));
  EXPECT_FALSE(parse_hex("+1"));
  EXPECT_FALSE(parse_hex(" 1"));
  EXPECT_FALSE(parse_hex("1 "));
  EXPECT_FALSE(parse_hex("00000000000000001"));
  EXPECT_EQ("7f1234567000", format_hex(0x7f1234567000ull));
  EXPECT_EQ("0", format_hex(0));
}

TEST(RdmaRcWire, Decimal)
{
  EXPECT_THAT(parse_decimal("0"), testing::Optional(0u));
  EXPECT_THAT(parse_decimal("4096"), testing::Optional(4096u));
  EXPECT_THAT(parse_decimal("18446744073709551615"),
              testing::Optional(18446744073709551615ull));
  EXPECT_FALSE(parse_decimal("18446744073709551616"));
  EXPECT_FALSE(parse_decimal(""));
  EXPECT_FALSE(parse_decimal("-1"));
  EXPECT_FALSE(parse_decimal("0x10"));
  EXPECT_FALSE(parse_decimal("12 "));
  EXPECT_FALSE(parse_decimal("+1"));
  EXPECT_FALSE(parse_decimal(" 1"));
  EXPECT_FALSE(parse_decimal("ff"));
  // at most 20 digits
  EXPECT_FALSE(parse_decimal("000000000000000000001"));
}

TEST(RdmaRcWire, Target)
{
  auto t = parse_target("/bucket/dir/key.bin");
  ASSERT_TRUE(t);
  EXPECT_EQ("bucket", t->bucket);
  EXPECT_EQ("dir/key.bin", t->key);
  EXPECT_EQ("", t->query);

  t = parse_target("/b/k?versionId=abc&partNumber=2");
  ASSERT_TRUE(t);
  EXPECT_EQ("b", t->bucket);
  EXPECT_EQ("k", t->key);
  EXPECT_EQ("versionId=abc&partNumber=2", t->query);

  // percent-encoded segments decode; '+' is literal in a path
  t = parse_target("/my%2Dbucket/a%20b+c%2Fd");
  ASSERT_TRUE(t);
  EXPECT_EQ("my-bucket", t->bucket);
  EXPECT_EQ("a b+c/d", t->key);

  EXPECT_FALSE(parse_target(""));
  EXPECT_FALSE(parse_target("bucket/key"));
  EXPECT_FALSE(parse_target("/bucket"));
  EXPECT_FALSE(parse_target("/bucket/"));
  EXPECT_FALSE(parse_target("//key"));
  EXPECT_FALSE(parse_target("/bucket/key%2"));
  EXPECT_FALSE(parse_target("/bucket/key%zz"));
}

TEST(RdmaRcWire, BuildTargetRoundTrip)
{
  const std::string bucket = "my-bucket";
  const std::string key = "dir one/fi%le~.bin";
  const auto built = build_target(bucket, key, "versionId=v1");
  EXPECT_EQ("/my-bucket/dir%20one/fi%25le~.bin?versionId=v1", built);
  auto t = parse_target(built);
  ASSERT_TRUE(t);
  EXPECT_EQ(bucket, t->bucket);
  EXPECT_EQ(key, t->key);
  EXPECT_EQ("versionId=v1", t->query);
}

TEST(RdmaRcWire, BuildTargetEscapes)
{
  // RFC 3986 unreserved characters and '/' stay, every other byte is
  // escaped in upper case
  EXPECT_EQ("/b.1/A-z_0.9~/x%2By%3Fz%26%C3%A9",
            build_target("b.1", "A-z_0.9~/x+y?z&\xc3\xa9"));
  auto t = parse_target(build_target("b", "\xc3\xa9 +"));
  ASSERT_TRUE(t);
  EXPECT_EQ("\xc3\xa9 +", t->key);
}

TEST(RdmaRcWire, PercentDecode)
{
  using testing::Optional;
  EXPECT_THAT(percent_decode(""), Optional(std::string{}));
  EXPECT_THAT(percent_decode("abc"), Optional(std::string{"abc"}));
  EXPECT_THAT(percent_decode("a%2Fb%2fc"), Optional(std::string{"a/b/c"}));
  EXPECT_THAT(percent_decode("%C3%A9"), Optional(std::string{"\xc3\xa9"}));
  // '+' is literal, not a space
  EXPECT_THAT(percent_decode("a+b"), Optional(std::string{"a+b"}));
  // a malformed escape fails: an RC GET's versionId=%zz is refused,
  // and does not read the current version
  EXPECT_FALSE(percent_decode("%zz"));
  EXPECT_FALSE(percent_decode("v1%2"));
  EXPECT_FALSE(percent_decode("v1%"));
  EXPECT_FALSE(percent_decode("%+1"));
  EXPECT_FALSE(percent_decode("%-1"));
  EXPECT_FALSE(percent_decode("% 1"));
}

TEST(RdmaRcWire, ChecksumHeaders)
{
  // the check values of "123456789", rendered as S3 renders them
  EXPECT_EQ("rosUhgp5mIg=",
            rgw::rdma::cksum_crc64nvme(0xae8b14860a799888ull).to_armor());
  EXPECT_EQ("4waSgw==", rgw::rdma::cksum_crc32c(0xe3069283u).to_armor());
  EXPECT_EQ(0xe3069283u, rgw::rdma::crc32c_of("123456789", 9));

  rgw::rdma::cksum_want w;
  ASSERT_EQ(0, rgw::rdma::parse_checksum_algorithm(nullptr, &w));
  EXPECT_TRUE(w.crc64nvme);
  EXPECT_FALSE(w.crc32c);
  ASSERT_EQ(0, rgw::rdma::parse_checksum_algorithm("crc32c", &w));
  EXPECT_FALSE(w.crc64nvme);
  EXPECT_TRUE(w.crc32c);
  ASSERT_EQ(0, rgw::rdma::parse_checksum_algorithm("CRC64NVME", &w));
  EXPECT_TRUE(w.crc64nvme);
  EXPECT_FALSE(w.crc32c);
  // S3 checksums that the OSDs do not compute, and no checksum
  EXPECT_EQ(-EINVAL, rgw::rdma::parse_checksum_algorithm("SHA256", &w));
  EXPECT_EQ(-EINVAL, rgw::rdma::parse_checksum_algorithm("CRC32", &w));
  EXPECT_EQ(-EINVAL, rgw::rdma::parse_checksum_algorithm("none", &w));
  EXPECT_EQ(-EINVAL, rgw::rdma::parse_checksum_algorithm("", &w));

  // the request flags that ask the OSDs for them
  using R = librados::ObjectOperation;
  EXPECT_EQ(R::RDMA_DELIVERY_WANT_CRC64,
            rgw::rdma::delivery_want_flags({.crc64nvme = true}));
  EXPECT_EQ(R::RDMA_DELIVERY_WANT_CRC64 | R::RDMA_DELIVERY_WANT_CRC32C,
            rgw::rdma::delivery_want_flags({true, true}));
  EXPECT_EQ(0u, rgw::rdma::delivery_want_flags({}));
}

TEST(RdmaRcWire, ChainedChecksums)
{
  // an RC GET or PUT chains the CRCs of the chunks it copies, which are
  // those of the whole buffer: no pass over the session buffer
  std::vector<char> buf((1u << 20) + 13);
  for (size_t i = 0; i < buf.size(); i++) {
    buf[i] = static_cast<char>(i * 131 + (i >> 9));
  }
  const uint64_t crc64 = ceph::crc64nvme(0, buf.data(), buf.size());
  const uint32_t crc32c = rgw::rdma::crc32c_of(buf.data(), buf.size());
  for (size_t chunk : {size_t{4097}, size_t{1} << 16, buf.size()}) {
    SCOPED_TRACE(chunk);
    chained_crcs c;
    c.reset({true, true});
    for (size_t o = 0; o < buf.size(); o += chunk) {
      c.append(&buf[o], std::min(chunk, buf.size() - o));
    }
    EXPECT_THAT(c.result().crc64, testing::Optional(crc64));
    EXPECT_THAT(c.result().crc32c, testing::Optional(crc32c));
  }
}

TEST(RdmaRcWire, ChainedChecksumsWant)
{
  const char* check = "123456789";
  // none until reset() names them: an op that resets the chain before
  // it parses x-amz-rdma-checksum-algorithm gets no result, and falls
  // back to a pass over its buffer
  chained_crcs c;
  c.append(check, 9);
  EXPECT_FALSE(c.result().crc64);
  EXPECT_FALSE(c.result().crc32c);
  c.reset({});
  c.append(check, 9);
  EXPECT_FALSE(c.result().crc64);
  EXPECT_FALSE(c.result().crc32c);

  // only those asked for
  c.reset({.crc32c = true});
  c.append(check, 4);
  c.append(check + 4, 5);
  EXPECT_FALSE(c.result().crc64);
  EXPECT_THAT(c.result().crc32c, testing::Optional(0xe3069283u));

  // reset() starts over, with none before the first append
  c.reset({.crc64nvme = true});
  EXPECT_FALSE(c.result().crc64);
  EXPECT_FALSE(c.result().crc32c);
  c.append(check, 1);
  c.append(check + 1, 8);
  EXPECT_THAT(c.result().crc64, testing::Optional(0xae8b14860a799888ull));
  EXPECT_FALSE(c.result().crc32c);
}

TEST(RdmaRcWire, SignedHeaders)
{
  const std::string_view list =
    "host;x-amz-date;x-amz-rdma-cookie;x-amz-rdma-token";
  EXPECT_TRUE(all_signed(list, {"x-amz-rdma-cookie", "x-amz-rdma-token"}));
  EXPECT_TRUE(all_signed(list, {}));
  EXPECT_FALSE(all_signed(list, {"x-amz-rdma-psn"}));
  // exact entries only, not substrings
  EXPECT_FALSE(all_signed(list, {"x-amz-rdma"}));
  EXPECT_FALSE(all_signed(list, {"amz-date"}));
  // the entry may be the last one
  EXPECT_TRUE(all_signed("host;x-amz-rdma-op", {"x-amz-rdma-op"}));
}

TEST(RdmaRcWire, CheckSigned)
{
  // the control routes check the list that the SigV4 verifier signed
  // over, as its parser reads it, on the route the S3 authenticator
  // takes. Each request carries x-amz-rdma-cookie and -token, which the
  // real list signs, and some x-amz-rdma-offset, which it does not
  auto cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_ANY);
  const NoDoutPrefix dpp(cct.get(), ceph_subsys_rgw);
  char date[32];
  const time_t now = time(nullptr);
  struct tm tm;
  gmtime_r(&now, &tm);
  strftime(date, sizeof(date), "%Y%m%dT%H%M%SZ", &tm);

  const std::string real = "host;x-amz-date;x-amz-rdma-cookie;x-amz-rdma-token";
  const std::string credential =
    "Credential=AK/20260929/us-east-1/s3/aws4_request";
  const std::string v4 = "AWS4-HMAC-SHA256 ";
  const std::string presigned = std::string{} +
    "X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=AK%2F20260929%2Fus-east-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=" + date + "&X-Amz-Expires=300";
  const std::string presigned_list =
    "&X-Amz-SignedHeaders=host%3Bx-amz-date%3Bx-amz-rdma-cookie%3Bx-amz-rdma-token";

  struct request_t {
    const char* label;
    std::optional<std::string> authorization;
    std::string query;
    bool offset;
    int expected;
  };
  const request_t requests[] = {
    {"header SigV4", v4 + credential + ", SignedHeaders=" + real +
     ", Signature=abcd", {}, false, 0},
    {"header SigV4, offset unsigned", v4 + credential + ", SignedHeaders=" +
     real + ", Signature=abcd", {}, true, -EACCES},
    // a forged list ahead of the one the signature covers
    {"header SigV4, forged list", v4 + "SignedHeaders=" + real +
     ";x-amz-rdma-offset, " + credential + ", SignedHeaders=" + real +
     ", Signature=abcd", {}, true, -EACCES},
    {"header SigV4 without a signature", v4 + credential +
     ", SignedHeaders=" + real, {}, false, -EACCES},
    {"header SigV2", "AWS AK:c2lnbmF0dXJl", {}, true, 0},
    // an empty Authorization header leaves the query to authenticate
    {"presigned SigV4", "", presigned + presigned_list +
     "&X-Amz-Signature=abcd", false, 0},
    {"presigned SigV4, offset unsigned", std::nullopt, presigned +
     presigned_list + "&X-Amz-Signature=abcd", true, -EACCES},
    {"presigned SigV4 without a signature", std::nullopt, presigned +
     presigned_list, false, -EACCES},
    {"presigned SigV2", std::nullopt,
     "AWSAccessKeyId=AK&Expires=4102444800&Signature=c2lnbmF0dXJl", true, 0},
    {"not signed", std::nullopt, {}, false, -EACCES},
  };
  for (const auto& r : requests) {
    SCOPED_TRACE(r.label);
    RGWEnv env;
    env.set("REQUEST_URI", r.query.empty() ? "/.hipobj-rc/prepare" :
            "/.hipobj-rc/prepare?" + r.query);
    if (r.authorization) {
      env.set("HTTP_AUTHORIZATION", *r.authorization);
    }
    env.set("HTTP_X_AMZ_DATE", date);
    env.set("HTTP_X_AMZ_RDMA_COOKIE", "1a2b3c4d");
    env.set("HTTP_X_AMZ_RDMA_TOKEN", std::string(TOKEN_HEX_LEN, '0'));
    if (r.offset) {
      env.set("HTTP_X_AMZ_RDMA_OFFSET", "2097152");
    }
    req_info info(cct.get(), &env);
    info.args.set(info.request_params);
    ASSERT_EQ(0, info.args.parse(&dpp));
    EXPECT_EQ(r.expected, check_signed(&dpp, info));
  }
}

TEST(RdmaRcWire, LandedRanges)
{
  landed_ranges r;
  EXPECT_EQ(0u, r.prefix());
  r.add(100, 50);           // [100,150)
  EXPECT_EQ(0u, r.prefix());
  r.add(0, 60);             // [0,60)
  EXPECT_EQ(60u, r.prefix());
  EXPECT_EQ(2u, r.count());
  r.add(60, 40);            // bridges to [0,150)
  EXPECT_EQ(150u, r.prefix());
  EXPECT_EQ(1u, r.count());
  r.add(10, 20);            // inside
  EXPECT_EQ(150u, r.prefix());
  EXPECT_EQ(1u, r.count());
  r.add(300, 10);
  r.add(200, 10);
  r.add(150, 150);          // swallows [200,210) and touches [300,310)
  EXPECT_EQ(310u, r.prefix());
  EXPECT_EQ(1u, r.count());
  r.add(400, 0);            // empty adds nothing
  EXPECT_EQ(1u, r.count());
}

TEST(RdmaRcWire, LandedRangesOverlap)
{
  // a staged read after a relayed one places the same bytes again
  landed_ranges r;
  r.add(100, 100);          // [100,200)
  r.add(300, 100);          // [300,400)
  r.add(150, 200);          // overlaps both: [100,400)
  EXPECT_EQ(1u, r.count());
  EXPECT_EQ(0u, r.prefix());
  r.add(0, 100);            // touches it: [0,400)
  EXPECT_EQ(400u, r.prefix());
  r.add(0, 400);
  r.add(50, 400);           // [0,450)
  EXPECT_EQ(450u, r.prefix());
  EXPECT_EQ(1u, r.count());
}

TEST(RdmaRcWire, LandedRangesWaitForTheFence)
{
  // an OSD whose result does not say landed may reply before its bytes
  // are in the relay buffer: nothing of them goes to the client until
  // the fence passed
  landed_ranges l;
  l.add_placed(0, 100);
  l.add(100, 50);
  EXPECT_TRUE(l.unconfirmed());
  EXPECT_EQ(0u, l.prefix());
  auto w = plan_pushes(l.prefix(), 0, 150, false);
  ASSERT_TRUE(w);
  EXPECT_TRUE(w->empty());
  EXPECT_FALSE(plan_pushes(l.prefix(), 0, 150, true));
  l.confirm_placed();
  EXPECT_FALSE(l.unconfirmed());
  EXPECT_EQ(150u, l.prefix());
  w = plan_pushes(l.prefix(), 0, 150, true);
  ASSERT_TRUE(w);
  ASSERT_EQ(1u, w->size());
  EXPECT_EQ((push_t{0, 150, true}), (*w)[0]);

  // a stripe that landed counts at once, up to one that did not
  landed_ranges m;
  m.add(0, 100);
  m.add_placed(100, 50);
  m.add(150, 50);
  EXPECT_EQ(100u, m.prefix());
  // the staged path copies over what an OSD placed
  m.add(100, 50);
  EXPECT_EQ(200u, m.prefix());
  m.confirm_placed();
  EXPECT_EQ(200u, m.prefix());
  EXPECT_EQ(1u, m.count());
}

TEST(RdmaRcWire, PlanPushesStreaming)
{
  push_policy p;
  p.min_push = 100;
  p.max_write = 250;
  p.tail_hold = 10;
  // below min_push: wait
  EXPECT_THAT(plan_pushes(50, 0, 1000, false, p),
              testing::Optional(testing::IsEmpty()));
  // enough landed: push it, split at max_write
  auto w = plan_pushes(600, 0, 1000, false, p);
  ASSERT_TRUE(w);
  ASSERT_EQ(3u, w->size());
  EXPECT_EQ((push_t{0, 250, false}), (*w)[0]);
  EXPECT_EQ((push_t{250, 250, false}), (*w)[1]);
  EXPECT_EQ((push_t{500, 100, false}), (*w)[2]);
  // the tail is held back even when everything landed
  w = plan_pushes(1000, 600, 1000, false, p);
  ASSERT_TRUE(w);
  ASSERT_EQ(2u, w->size());
  EXPECT_EQ((push_t{600, 250, false}), (*w)[0]);
  EXPECT_EQ((push_t{850, 140, false}), (*w)[1]);
  // reaching the tail point pushes even a short run
  w = plan_pushes(1000, 950, 1000, false, p);
  ASSERT_TRUE(w);
  ASSERT_EQ(1u, w->size());
  EXPECT_EQ((push_t{950, 40, false}), (*w)[0]);
  // nothing new
  EXPECT_THAT(plan_pushes(1000, 990, 1000, false, p),
              testing::Optional(testing::IsEmpty()));
  // an object smaller than the tail streams nothing
  EXPECT_THAT(plan_pushes(8, 0, 8, false, p),
              testing::Optional(testing::IsEmpty()));
}

TEST(RdmaRcWire, PlanPushesFinal)
{
  push_policy p;
  p.max_write = 250;
  p.tail_hold = 10;
  auto w = plan_pushes(1000, 990, 1000, true, p);
  ASSERT_TRUE(w);
  ASSERT_EQ(1u, w->size());
  EXPECT_EQ((push_t{990, 10, true}), (*w)[0]);
  // nothing streamed: split, immediate on the last write only
  w = plan_pushes(600, 0, 600, true, p);
  ASSERT_TRUE(w);
  ASSERT_EQ(3u, w->size());
  EXPECT_EQ((push_t{0, 250, false}), (*w)[0]);
  EXPECT_EQ((push_t{250, 250, false}), (*w)[1]);
  EXPECT_EQ((push_t{500, 100, true}), (*w)[2]);
  // everything pushed already: a zero-length write carries the cookie
  w = plan_pushes(600, 600, 600, true, p);
  ASSERT_TRUE(w);
  ASSERT_EQ(1u, w->size());
  EXPECT_EQ((push_t{600, 0, true}), (*w)[0]);
  // a hole means the transfer is incomplete
  EXPECT_FALSE(plan_pushes(599, 0, 600, true, p));
}

#ifdef WITH_RDMA_RC
TEST(RdmaRcLimits, CountsSessionsBeingPrepared)
{
  // prepare() takes a place, then builds the session outside the service
  // lock before the session table holds it: the limit counts the places
  // taken, so PREPAREs from other users in that window cannot pass it
  Limits l(4, 2);
  EXPECT_TRUE(l.take("a", 1));
  EXPECT_TRUE(l.take("b", 1));
  EXPECT_TRUE(l.take("c", 1));
  EXPECT_TRUE(l.take("d", 1));
  EXPECT_EQ(4u, l.sessions());
  EXPECT_FALSE(l.take("e", 1));
  EXPECT_FALSE(l.take("f", 1));
  EXPECT_EQ(4u, l.sessions());
  l.end("b");
  EXPECT_EQ(3u, l.sessions());
  EXPECT_TRUE(l.take("e", 1));
  EXPECT_FALSE(l.take("f", 1));
}

TEST(RdmaRcLimits, PerUser)
{
  Limits l(8, 2);
  EXPECT_TRUE(l.take("a", 1));
  EXPECT_TRUE(l.take("a", 1));
  EXPECT_FALSE(l.take("a", 1));
  EXPECT_TRUE(l.take("b", 1));
  EXPECT_EQ(3u, l.sessions());
  l.end("a");
  EXPECT_TRUE(l.take("a", 1));
  EXPECT_EQ(3u, l.sessions());
  // a user that holds nothing releases nothing
  l.end("z");
  l.uncharge("z", 1);
  EXPECT_EQ(3u, l.sessions());
}

TEST(RdmaRcLimits, SixtyFourBit)
{
  // the options are 64 bits wide: a limit of 2^32 is not 0
  constexpr uint64_t big = uint64_t{1} << 32;
  Limits l(big, big + 1);
  EXPECT_TRUE(l.take("a", 1));
  EXPECT_TRUE(l.take("a", 1));
  EXPECT_EQ(2u, l.sessions());
  Limits per_user(8, big);
  EXPECT_TRUE(per_user.take("a", 1));
  // and so is a byte limit
  Limits bytes(8, 8, 3 * big);
  EXPECT_TRUE(bytes.take("a", 2 * big));
  EXPECT_TRUE(bytes.take("a", big));
  EXPECT_FALSE(bytes.take("a", 1));
}

TEST(RdmaRcLimits, ZeroAdmitsNone)
{
  Limits none(0, 4);
  EXPECT_FALSE(none.take("a", 1));
  EXPECT_EQ(0u, none.sessions());
  Limits no_user(4, 0);
  EXPECT_FALSE(no_user.take("a", 1));
  no_user.end("a");
  EXPECT_EQ(0u, no_user.sessions());
}

TEST(RdmaRcLimits, BytesPerUser)
{
  // 64 sessions of 64 MiB are twice a 2 GiB arena: the bytes that a
  // user holds are capped, not only its sessions
  constexpr uint64_t MiB = 1ull << 20;
  Limits l(1024, 64, 512 * MiB);
  for (int i = 0; i < 8; i++) {
    EXPECT_TRUE(l.take("a", 64 * MiB)) << i;
  }
  EXPECT_FALSE(l.take("a", 1 * MiB));
  EXPECT_EQ(512 * MiB, l.bytes("a"));
  // other users are not held back by it
  EXPECT_TRUE(l.take("b", 64 * MiB));
  // a refused take() charges nothing
  EXPECT_EQ(512 * MiB, l.bytes("a"));
  EXPECT_EQ(9u, l.sessions());
  l.end("a");
  l.uncharge("a", 64 * MiB);
  EXPECT_TRUE(l.take("a", 64 * MiB));
  // a byte count past what is charged does not wrap
  EXPECT_FALSE(l.take("a", UINT64_MAX));
}

TEST(RdmaRcLimits, ChargedUntilBackInTheArena)
{
  // a session that left the table frees its place at once, but its
  // buffer stays charged through its teardown and quarantine
  constexpr uint64_t MiB = 1ull << 20;
  Limits l(1024, 64, 4 * MiB);
  for (int i = 0; i < 4; i++) {
    EXPECT_TRUE(l.take("a", 1 * MiB));
  }
  for (int i = 0; i < 4; i++) {
    l.end("a");
  }
  EXPECT_EQ(0u, l.sessions());
  EXPECT_EQ(4 * MiB, l.bytes("a"));
  EXPECT_FALSE(l.take("a", 1 * MiB));
  l.uncharge("a", 1 * MiB);
  EXPECT_TRUE(l.take("a", 1 * MiB));
  EXPECT_FALSE(l.take("a", 1 * MiB));
  l.end("a");
  l.uncharge("a", 4 * MiB);
  EXPECT_EQ(0u, l.bytes("a"));
}

TEST(RdmaRcLimits, OneBufferOfAnySize)
{
  // a limit below the largest buffer does not lock a user out
  constexpr uint64_t MiB = 1ull << 20;
  Limits l(1024, 64, 16 * MiB);
  EXPECT_TRUE(l.take("a", 64 * MiB));
  EXPECT_FALSE(l.take("a", 1 * MiB));
  l.end("a");
  l.uncharge("a", 64 * MiB);
  EXPECT_TRUE(l.take("a", 8 * MiB));
  EXPECT_TRUE(l.take("a", 8 * MiB));
  EXPECT_FALSE(l.take("a", 1 * MiB));
}

TEST(RdmaRcLimits, FairShareAlone)
{
  // without a fixed limit, a user alone may hold the whole pool, as
  // the one account of a gateway that serves a KV cache does
  constexpr uint64_t MiB = 1ull << 20;
  Limits l(1024, 64, 0, 256 * MiB);
  EXPECT_EQ(256 * MiB, l.share());
  for (int i = 0; i < 4; i++) {
    EXPECT_TRUE(l.take("a", 64 * MiB)) << i;
  }
  EXPECT_FALSE(l.take("a", 1 * MiB));
  // buffers that are charged after their sessions count, but no more
  for (int i = 0; i < 4; i++) {
    l.end("a");
  }
  l.uncharge("a", 64 * MiB);
  EXPECT_TRUE(l.take("a", 64 * MiB));
}

TEST(RdmaRcLimits, FairShareAmongUsers)
{
  // each user that holds buffers may hold an equal part of the pool
  constexpr uint64_t MiB = 1ull << 20;
  Limits l(1024, 64, 0, 256 * MiB);
  EXPECT_TRUE(l.take("a", 64 * MiB));
  EXPECT_TRUE(l.take("a", 64 * MiB));
  EXPECT_TRUE(l.take("b", 64 * MiB));
  EXPECT_EQ(128 * MiB, l.share());
  EXPECT_FALSE(l.take("a", 1 * MiB));
  EXPECT_TRUE(l.take("b", 64 * MiB));
  EXPECT_FALSE(l.take("b", 1 * MiB));
  // a user that holds nothing may still take one buffer
  EXPECT_TRUE(l.take("c", 64 * MiB));
  EXPECT_FALSE(l.take("a", 1 * MiB));
  // and once b holds nothing, a and c share the pool
  l.end("b");
  l.end("b");
  l.uncharge("b", 128 * MiB);
  EXPECT_EQ(128 * MiB, l.share());
  EXPECT_FALSE(l.take("a", 1 * MiB));
  l.end("c");
  l.uncharge("c", 64 * MiB);
  EXPECT_EQ(256 * MiB, l.share());
  EXPECT_TRUE(l.take("a", 64 * MiB));
}

TEST(RdmaRcLimits, AUserThatFoundNoRoomCounts)
{
  // a holds the whole pool, and b finds no room: a may take no more
  // until b got its part, so that the buffers that come back are not
  // all a's again
  constexpr uint64_t MiB = 1ull << 20;
  const auto t0 = clock::now();
  Limits l(1024, 64, 0, 256 * MiB);
  for (int i = 0; i < 4; i++) {
    EXPECT_TRUE(l.take("a", 64 * MiB, t0));
  }
  l.wanted("b", t0);
  EXPECT_EQ(128 * MiB, l.share());
  for (int i = 0; i < 2; i++) {
    l.end("a");
    l.uncharge("a", 64 * MiB);
  }
  EXPECT_FALSE(l.take("a", 64 * MiB, t0 + 1s));
  EXPECT_TRUE(l.take("b", 64 * MiB, t0 + 1s));
  EXPECT_TRUE(l.take("b", 64 * MiB, t0 + 1s));
  EXPECT_FALSE(l.take("a", 64 * MiB, t0 + 1s));
  // a user that stopped asking no longer counts after a while
  l.end("b");
  l.end("b");
  l.uncharge("b", 128 * MiB);
  l.wanted("c", t0 + 1s);
  EXPECT_FALSE(l.take("a", 64 * MiB, t0 + 2s));
  EXPECT_TRUE(l.take("a", 64 * MiB, t0 + 1s + Limits::WANTS_FOR));
  // with a fixed limit, nothing of this applies
  Limits fixed(1024, 64, 128 * MiB, 256 * MiB);
  fixed.wanted("b", t0);
  EXPECT_EQ(128 * MiB, fixed.share());
}

TEST(RdmaRcTeardowns, WaitsForThoseThatEndedBefore)
{
  // a PREPARE that finds the arena full waits for the teardowns of the
  // sessions that ended until then, also one the reaper runs already
  Teardowns t;
  EXPECT_FALSE(t.running_upto(t.last()));
  const auto a = t.ended();
  t.begin(a);
  const auto upto = t.last();
  EXPECT_TRUE(t.running_upto(upto));
  // and not for a session that ends and is torn down meanwhile
  const auto b = t.ended();
  EXPECT_LT(a, b);
  t.begin(b);
  t.end(a);
  EXPECT_FALSE(t.running_upto(upto));
  EXPECT_TRUE(t.running_upto(t.last()));
  t.end(b);
  EXPECT_FALSE(t.running_upto(t.last()));
}

TEST(RdmaRcTeardowns, OutOfOrder)
{
  // the reaper and a PREPARE tear down side by side, and the later
  // teardown can be done first
  Teardowns t;
  const auto a = t.ended();
  const auto b = t.ended();
  t.begin(a);
  t.begin(b);
  const auto upto = t.last();
  t.end(b);
  EXPECT_TRUE(t.running_upto(upto));
  EXPECT_TRUE(t.running_upto(a));
  t.end(a);
  EXPECT_FALSE(t.running_upto(upto));
}

TEST(RdmaRcTransport, PathMtu)
{
  // 0 takes the port's active MTU
  EXPECT_EQ(IBV_MTU_4096, path_mtu_of(0, IBV_MTU_4096));
  EXPECT_EQ(IBV_MTU_1024, path_mtu_of(0, IBV_MTU_1024));
  // the clients' path MTU, below the port's
  EXPECT_EQ(IBV_MTU_256, path_mtu_of(256, IBV_MTU_4096));
  EXPECT_EQ(IBV_MTU_512, path_mtu_of(512, IBV_MTU_4096));
  EXPECT_EQ(IBV_MTU_1024, path_mtu_of(1024, IBV_MTU_4096));
  EXPECT_EQ(IBV_MTU_2048, path_mtu_of(2048, IBV_MTU_4096));
  EXPECT_EQ(IBV_MTU_4096, path_mtu_of(4096, IBV_MTU_4096));
  // sizes between the path MTUs round down. The option takes the
  // clients' path MTU, not their interface MTU, which this does not
  // map: an interface MTU of 4096 carries a RoCE path MTU of 2048
  EXPECT_EQ(IBV_MTU_1024, path_mtu_of(1500, IBV_MTU_4096));
  EXPECT_EQ(IBV_MTU_4096, path_mtu_of(9000, IBV_MTU_4096));
  EXPECT_EQ(IBV_MTU_256, path_mtu_of(511, IBV_MTU_4096));
  // never above the port's
  EXPECT_EQ(IBV_MTU_1024, path_mtu_of(4096, IBV_MTU_1024));
  EXPECT_EQ(IBV_MTU_1024, path_mtu_of(9000, IBV_MTU_1024));
  // no path MTU is that small
  EXPECT_FALSE(path_mtu_of(1, IBV_MTU_4096));
  EXPECT_FALSE(path_mtu_of(255, IBV_MTU_4096));
}
#endif
