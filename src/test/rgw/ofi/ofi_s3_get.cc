// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * S3 GET with OSD-direct delivery over libfabric (test client).
 *
 * Registers a window with a libfabric provider, puts its endpoint name
 * and key in x-amz-rdma-token, and sends an ordinary SigV4 S3 GET,
 * signed here (ofi_s3_sigv4.h) rather than by libcurl. The gateway
 * forwards the token to the OSDs, which RMA-write the object's stripes
 * into the window; this client never learns who wrote. A
 * progress thread polls the provider so the writes land while the
 * gateway waits. Then the window is checked against a local copy of
 * the object. The window starts as the complement of that copy, so a
 * byte that no OSD wrote does not match.
 *
 * The OSDs must place the object, and x-amz-rdma-bytes-transferred must
 * count every byte. A gateway that falls back to HTTP fails the test,
 * unless OFI_S3_ALLOW_FALLBACK=1: then the body is checked instead.
 *
 * The response must carry the OSDs' CRC of the delivered bytes,
 * x-amz-rdma-checksum-crc64nvme, or x-amz-rdma-checksum-crc32c when
 * OFI_S3_CKSUM=CRC32C asks for that one, equal to this client's CRC of
 * its window. OFI_S3_RANGE=<first>-<last> GETs that byte range (a Range
 * header, so a 206): the window holds the range only, and the checksum
 * covers the range only.
 *
 *   ceph_test_rgw_ofi_get <provider> <domain|-> <node|-> <endpoint> \
 *     <bucket> <key> <access> <secret> <expected-file>
 */

#include <curl/curl.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <strings.h>

#include "common/ofi_rma.h"
#include "ofi_s3_cksum.h"
#include "ofi_s3_sigv4.h"

namespace {

struct http_result {
  long status = 0;
  std::string reply;       // x-amz-rdma-reply
  std::string bytes;       // x-amz-rdma-bytes-transferred
  std::string rdma_crc64;  // x-amz-rdma-checksum-crc64nvme
  std::string rdma_crc32c; // x-amz-rdma-checksum-crc32c
  std::string body;
  std::string error;
};

size_t on_header(char* p, size_t size, size_t n, void* arg)
{
  auto* r = static_cast<http_result*>(arg);
  std::string line(p, size * n);
  auto colon = line.find(':');
  if (colon != std::string::npos) {
    std::string name = line.substr(0, colon);
    for (auto& c : name) c = std::tolower(c);
    std::string value = line.substr(colon + 1);
    while (!value.empty() && value.front() == ' ') value.erase(0, 1);
    while (!value.empty() && (value.back() == '\r' || value.back() == '\n'))
      value.pop_back();
    if (name == "x-amz-rdma-reply") r->reply = value;
    if (name == "x-amz-rdma-bytes-transferred") r->bytes = value;
    if (name == "x-amz-rdma-checksum-crc64nvme") r->rdma_crc64 = value;
    if (name == "x-amz-rdma-checksum-crc32c") r->rdma_crc32c = value;
  }
  return size * n;
}

size_t on_body(char* p, size_t size, size_t n, void* arg)
{
  static_cast<http_result*>(arg)->body.append(p, size * n);
  return size * n;
}

std::string arg_or_empty(const char* a)
{
  return std::strcmp(a, "-") == 0 ? std::string{} : std::string{a};
}

} // anonymous namespace

int main(int argc, char** argv)
{
  if (argc != 10) {
    std::fprintf(stderr, "usage: %s <provider> <domain|-> <node|-> <endpoint> "
		 "<bucket> <key> <access> <secret> <expected-file>\n", argv[0]);
    return 2;
  }
  if (!ofi_s3::self_test()) {
    std::fprintf(stderr, "CRC self-test failed\n");
    return 2;
  }
  ceph::ofi::config_t cfg;
  cfg.provider = argv[1];
  cfg.domain = arg_or_empty(argv[2]);
  cfg.node = arg_or_empty(argv[3]);
  cfg.progress_thread = true;
  const std::string endpoint = argv[4], bucket = argv[5], key = argv[6];
  const std::string access = argv[7], secret = argv[8];
  std::ifstream in(argv[9], std::ios::binary);
  std::vector<char> expected{std::istreambuf_iterator<char>(in), {}};
  if (expected.empty()) {
    std::fprintf(stderr, "empty expected file\n");
    return 2;
  }

  // the bytes asked for: the object, or the range OFI_S3_RANGE names
  std::string range;
  if (const char* r = std::getenv("OFI_S3_RANGE"); r && *r) {
    unsigned long long first = 0, last = 0;
    if (std::sscanf(r, "%llu-%llu", &first, &last) != 2 || first > last ||
	last >= expected.size()) {
      std::fprintf(stderr, "OFI_S3_RANGE %s is not a range of the object\n", r);
      return 2;
    }
    range = "bytes=" + std::to_string(first) + "-" + std::to_string(last);
    expected = std::vector<char>(expected.begin() + first,
				 expected.begin() + last + 1);
  }
  const size_t size = expected.size();
  const char* alg_env = std::getenv("OFI_S3_CKSUM");
  const bool want32c = alg_env && strcasecmp(alg_env, "CRC32C") == 0;
  const char* fallback_env = std::getenv("OFI_S3_ALLOW_FALLBACK");
  const bool allow_fallback =
    fallback_env && std::strcmp(fallback_env, "1") == 0;

  std::string err;
  auto ep = ceph::ofi::Endpoint::open(cfg, &err);
  if (!ep) {
    std::fprintf(stderr, "libfabric endpoint: %s\n", err.c_str());
    return 1;
  }
  void* mem = nullptr;
  if (posix_memalign(&mem, 4096, size) != 0) {
    return 1;
  }
  char* window = static_cast<char*>(mem);
  // no byte matches until an OSD writes it
  for (size_t i = 0; i < size; i++) {
    window[i] = ~expected[i];
  }
  ceph::ofi::Endpoint::window_t w;
  if (int r = ep->register_window(window, size, &w); r < 0) {
    std::fprintf(stderr, "register window: %s\n", ep->last_error().c_str());
    return 1;
  }
  const std::string token = ep->window_token(w, 0, size);
  std::printf("%s\nwindow %zu bytes%s%s, token %s\n", ep->describe().c_str(),
	      size, range.empty() ? "" : ", range ", range.c_str(),
	      token.c_str());

  http_result res;
  const auto t0 = std::chrono::steady_clock::now();
  CURL* c = curl_easy_init();
  std::vector<std::string> headers = {"x-amz-rdma-token: " + token};
  if (alg_env) {
    headers.push_back(std::string("x-amz-rdma-checksum-algorithm: ") +
                      alg_env);
  }
  if (!range.empty()) {
    headers.push_back("Range: " + range);
  }
  const auto req = ofi_s3::sigv4_sign(
    "GET", endpoint + "/" + bucket + "/" + key, headers,
    ofi_s3::empty_sha256(), access, secret);
  struct curl_slist* hdrs = nullptr;
  for (const auto& h : req.headers) {
    hdrs = curl_slist_append(hdrs, h.c_str());
  }
  curl_easy_setopt(c, CURLOPT_URL, req.url.c_str());
  curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
  curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, on_header);
  curl_easy_setopt(c, CURLOPT_HEADERDATA, &res);
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, on_body);
  curl_easy_setopt(c, CURLOPT_WRITEDATA, &res);
  if (CURLcode cc = curl_easy_perform(c); cc != CURLE_OK) {
    res.error = curl_easy_strerror(cc);
  }
  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &res.status);
  curl_slist_free_all(hdrs);
  curl_easy_cleanup(c);
  // writes placed before the reply was sent are in the window; take
  // the endpoint's lock so this thread reads after its progress thread
  ep->sync();
  const double ms = std::chrono::duration<double, std::milli>(
    std::chrono::steady_clock::now() - t0).count();

  std::printf("HTTP %ld, x-amz-rdma-reply %s, bytes %s, body %zu, "
	      "x-amz-rdma-checksum-crc64nvme %s, x-amz-rdma-checksum-crc32c %s, "
	      "%.1f ms\n", res.status, res.reply.c_str(), res.bytes.c_str(),
	      res.body.size(), res.rdma_crc64.c_str(), res.rdma_crc32c.c_str(),
	      ms);
  const long ok_status = range.empty() ? 200 : 206;
  if (!res.error.empty() || res.status != ok_status) {
    std::printf("FAIL: request failed (expected HTTP %ld) %s\n", ok_status,
		res.error.c_str());
    return 1;
  }
  const bool placed = res.reply == std::to_string(ok_status);
  const char* got = window;
  size_t got_len = size;
  if (!placed) {
    // the gateway fell back to HTTP: the body is the object, or the range
    got = res.body.data();
    got_len = res.body.size();
    std::printf("fallback: object came back over HTTP\n");
    if (!allow_fallback) {
      std::printf("FAIL: the OSDs did not place the data "
		  "(OFI_S3_ALLOW_FALLBACK=1 accepts the HTTP body)\n");
      return 1;
    }
  } else if (res.bytes != std::to_string(size)) {
    std::printf("FAIL: x-amz-rdma-bytes-transferred %s, expected %zu\n",
		res.bytes.c_str(), size);
    return 1;
  }
  if (got_len != size || std::memcmp(got, expected.data(), size) != 0) {
    size_t i = 0;
    while (i < std::min(got_len, size) && got[i] == expected[i]) i++;
    std::printf("FAIL: mismatch at byte %zu\n", i);
    return 1;
  }
  if (placed) {
    // the OSDs' CRCs of what they placed, against this client's of what
    // it found in its window: the one asked for must be there, and any
    // other must match too
    const std::string mine64 = ofi_s3::armor_crc64nvme(window, size);
    const std::string mine32c = ofi_s3::armor_crc32c(window, size);
    const std::string& asked = want32c ? res.rdma_crc32c : res.rdma_crc64;
    if (asked.empty() ||
	(!res.rdma_crc64.empty() && res.rdma_crc64 != mine64) ||
	(!res.rdma_crc32c.empty() && res.rdma_crc32c != mine32c)) {
      std::printf("FAIL: the delivered checksums (%s, %s) are not the "
		  "window's (%s, %s)\n", res.rdma_crc64.c_str(),
		  res.rdma_crc32c.c_str(), mine64.c_str(), mine32c.c_str());
      return 1;
    }
  }
  std::printf("PASS: %zu bytes verified (%s)\n", size,
	      placed ? (want32c ?
			"placed by the OSDs over libfabric, CRC32C checked" :
			"placed by the OSDs over libfabric, CRC64NVME checked")
		     : "HTTP fallback");
  return 0;
}
