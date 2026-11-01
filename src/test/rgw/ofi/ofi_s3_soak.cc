// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
 * Soak client for S3 GETs with OSD-direct delivery over libfabric.
 *
 * Every object holds self-describing data: 8-byte little-endian words
 * (0xC5 << 56) | (id << 40) | word index. Any byte found anywhere can be
 * traced to the object and offset it came from.
 *
 * Every window holds a poison pattern. Before each GET, the client
 * poisons again the bytes that the window's last GET asked for, and
 * checks that the token's range and a guard band on each side of it
 * (--guard, 1 MiB by default) hold the poison. After the GET, the bytes
 * the GET asked for must hold the object's data, and the rest of that
 * range must still be poison. A GET the OSDs placed must also report
 * the OSDs' CRC-64/NVME of the bytes, x-amz-rdma-checksum-crc64nvme,
 * equal to this client's CRC of them, when the gateway sends one. Before
 * a window goes into quarantine, and at the end of the run, the client
 * checks that the rest of the window is poison too, so a stray write
 * anywhere is found, if later. A quarantined window is poisoned again and
 * kept out of use for --quarantine-ms (10 s by default), and after a GET
 * that failed, for that long past its fence (see below); a byte that
 * changes then is a late write. Only after that is the window used again.
 *
 *   ceph_test_rgw_ofi_soak put <endpoint> <bucket> <access> <secret>
 *                              <manifest-out> [--crc] [--big]
 *   ceph_test_rgw_ofi_soak run <provider> <domain|-> <node|-> <endpoint>
 *                              <bucket> <access> <secret> <manifest>
 *                              [options]
 *     --threads N --windows M --window-size BYTES --quarantine-ms MS
 *     --duration S --count N --range-prob P --subwin-prob P
 *     --small-token-prob P --timeout-ms MS --seed N --log FILE
 *     --only-ids a,b,c --stats-s S --quarantine-prob P --fence-ms MS --rekey
 *     --guard BYTES --inject-stray N
 *   ceph_test_rgw_ofi_soak gen <id> <size>   object data on stdout
 *   ceph_test_rgw_ofi_soak crc <id> <size>   CRC-64/NVME of the object
 *
 * put writes the built-in set of objects over plain HTTP, and the
 * manifest that run reads: one line for each object, with its key, id,
 * size, part size (0 for a single PUT, else a multipart upload of parts
 * of that size) and 1 when the PUT sent x-amz-checksum-crc64nvme. --big
 * adds an object of 100 MiB.
 *
 * --read-size N [--read-align A]: every GET is a range of N bytes at a
 * random offset that is a multiple of A (default N), clipped at the end.
 *
 * --inject-stray N tests the soak itself: after every Nth GET, the client
 * writes one word of object data into the window, outside what the GET's
 * checks covered, unless the window holds such a word already. Each such
 * word must come back as one violation.
 *
 * --rekey re-registers a window (new key) as soon as its GET is over, so a
 * late duplicate of that GET's writes, which carries the old key, cannot
 * land.
 *
 * A GET that got a response (data placed, or the object over HTTP)
 * must leave nothing to land afterwards. A GET that failed without a
 * response (a reset, a timeout) gives no such promise: an OSD may still
 * write until the pool's rdma_delivery_lease plus rdma_delivery_drain
 * plus osd_oob_cutoff_late_tolerance after it received the GET's last
 * stripe operation, which can be just before the GET failed. So writes
 * may land until --fence-ms after the GET ended (9000 by default, for the
 * defaults of 5 s, 3 s and 1000 ms). A late write before that is counted
 * as allowed; after it, it is a violation. Where OSDs are stopped (a
 * thrasher's SIGSTOP), add the longest stop to the fence.
 *
 * run prints a STATS line every --stats-s seconds, and a FINAL line at
 * the end, and each violation on a VIOLATION line as it finds it. It
 * exits 0 without violations, 3 with any, 1 when it cannot set up its
 * endpoint and windows, and 2 on a usage error. SIGINT and SIGTERM end
 * the run early, as --duration does.
 */

#include <curl/curl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <sys/mman.h>

#include "common/ofi_rma.h"
#include "ofi_s3_cksum.h"
#include "ofi_s3_sigv4.h"

using clk = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {

constexpr uint64_t CONTENT_TAG = 0xC5;
constexpr uint64_t POISON_TAG = 0x9D;
constexpr uint64_t IDX_MASK = (1ull << 40) - 1;

inline uint64_t content_word(uint32_t id, uint64_t i)
{
  return (CONTENT_TAG << 56) | (uint64_t(id & 0xffff) << 40) | (i & IDX_MASK);
}

inline uint64_t poison_word(uint64_t seq)
{
  return (POISON_TAG << 56) | (seq & ((1ull << 56) - 1));
}

inline unsigned char word_byte(uint64_t w, unsigned b)
{
  return (unsigned char)(w >> (8 * b));
}

// object bytes [off, off+len) of object id
void fill_content(uint32_t id, uint64_t off, uint64_t len, char* out)
{
  uint64_t b = off, end = off + len;
  char* p = out;
  while (b < end && (b & 7)) {
    *p++ = word_byte(content_word(id, b >> 3), b & 7);
    b++;
  }
  while (end - b >= 8) {
    uint64_t w = content_word(id, b >> 3);
    std::memcpy(p, &w, 8);
    p += 8;
    b += 8;
  }
  while (b < end) {
    *p++ = word_byte(content_word(id, b >> 3), b & 7);
    b++;
  }
}

void fill_poison(char* win, size_t len, uint64_t seq)
{
  uint64_t w = poison_word(seq);
  size_t n = len / 8;
  auto* q = reinterpret_cast<uint64_t*>(win);
  for (size_t i = 0; i < n; i++) {
    q[i] = w;
  }
  for (size_t i = n * 8; i < len; i++) {
    win[i] = word_byte(w, i & 7);
  }
}

inline unsigned char poison_byte(uint64_t seq, size_t pos)
{
  return word_byte(poison_word(seq), pos & 7);
}

// poison window bytes [from, to), as fill_poison() of the whole window
// puts them
void poison_range(char* win, size_t from, size_t to, uint64_t seq)
{
  const uint64_t w = poison_word(seq);
  size_t i = from;
  for (; i < to && (i & 7); i++) {
    win[i] = word_byte(w, i & 7);
  }
  for (; to - i >= 8; i += 8) {
    std::memcpy(win + i, &w, 8);
  }
  for (; i < to; i++) {
    win[i] = word_byte(w, i & 7);
  }
}

// the first byte of window bytes [from, to) that is not poison, or
// SIZE_MAX
size_t first_non_poison(const char* win, size_t from, size_t to,
                        uint64_t seq)
{
  const uint64_t pw = poison_word(seq);
  for (size_t i = from; i < to; ) {
    if ((i & 7) == 0 && to - i >= 8) {
      uint64_t v;
      std::memcpy(&v, win + i, 8);
      if (v == pw) {
        i += 8;
        continue;
      }
    }
    if ((unsigned char)win[i] != poison_byte(seq, i)) {
      return i;
    }
    i++;
  }
  return SIZE_MAX;
}

std::vector<char> object_data(uint32_t id, uint64_t size)
{
  std::vector<char> data(size);
  fill_content(id, 0, size, data.data());
  return data;
}

// a string as a JSON string literal
std::string jstr(const std::string& s)
{
  std::string o = "\"";
  for (char c : s) {
    if (c == '"' || c == '\\') {
      o += '\\';
      o += c;
    } else if ((unsigned char)c < 0x20) {
      char b[8];
      std::snprintf(b, sizeof(b), "\\u%04x", (unsigned char)c);
      o += b;
    } else {
      o += c;
    }
  }
  return o + "\"";
}

// describe what sits around window byte q: find content words by their tag
std::string decode_at(const char* win, size_t winlen, size_t q)
{
  std::ostringstream o;
  size_t lo = q >= 15 ? q - 15 : 0, hi = std::min(winlen, q + 16);
  bool any = false;
  for (size_t e = lo + 7; e < hi; e++) {
    if ((unsigned char)win[e] != CONTENT_TAG &&
        (unsigned char)win[e] != POISON_TAG) {
      continue;
    }
    uint64_t w;
    std::memcpy(&w, win + e - 7, 8);
    if ((w >> 56) == CONTENT_TAG) {
      uint32_t id = (w >> 40) & 0xffff;
      uint64_t idx = w & IDX_MASK;
      o << " [win " << (e - 7) << ": obj " << id << " off " << idx * 8 << "]";
      any = true;
      break;
    } else if ((w >> 56) == POISON_TAG && ((e - 7) & 7) == 0) {
      o << " [win " << (e - 7) << ": poison seq "
        << (w & ((1ull << 56) - 1)) << "]";
      any = true;
      break;
    }
  }
  if (!any) {
    o << " [bytes";
    for (size_t i = (q >= 4 ? q - 4 : 0); i < std::min(winlen, q + 4); i++) {
      char b[4];
      std::snprintf(b, sizeof(b), " %02x", (unsigned char)win[i]);
      o << b;
    }
    o << "]";
  }
  return o.str();
}

// ---------------------------------------------------------------- HTTP

struct http_result {
  long status = 0;
  std::string reply;   // x-amz-rdma-reply
  std::string bytes;   // x-amz-rdma-bytes-transferred
  std::string crc64;   // x-amz-rdma-checksum-crc64nvme
  std::string etag, body, error;
};

size_t on_header(char* p, size_t size, size_t n, void* arg)
{
  auto* r = static_cast<http_result*>(arg);
  std::string line(p, size * n);
  auto colon = line.find(':');
  if (colon != std::string::npos) {
    std::string name = line.substr(0, colon);
    for (auto& c : name) {
      c = std::tolower(c);
    }
    std::string v = line.substr(colon + 1);
    while (!v.empty() && v.front() == ' ') {
      v.erase(0, 1);
    }
    while (!v.empty() && (v.back() == '\r' || v.back() == '\n')) {
      v.pop_back();
    }
    if (name == "x-amz-rdma-reply") {
      r->reply = v;
    } else if (name == "x-amz-rdma-bytes-transferred") {
      r->bytes = v;
    } else if (name == "x-amz-rdma-checksum-crc64nvme") {
      r->crc64 = v;
    } else if (name == "etag") {
      r->etag = v;
    }
  }
  return size * n;
}

size_t on_body(char* p, size_t size, size_t n, void* arg)
{
  static_cast<http_result*>(arg)->body.append(p, size * n);
  return size * n;
}

struct upload_src {
  const char* p;
  size_t left;
};

size_t on_read(char* buf, size_t size, size_t n, void* arg)
{
  auto* s = static_cast<upload_src*>(arg);
  size_t k = std::min(size * n, s->left);
  std::memcpy(buf, s->p, k);
  s->p += k;
  s->left -= k;
  return k;
}

// a connection to the gateway; its requests are signed with SigV4 here,
// not by libcurl (see ofi_s3_sigv4.h)
struct S3 {
  std::string endpoint, bucket, access, secret;
  long timeout_ms = 120000;
  CURL* c = nullptr;

  S3(std::string ep, std::string b, std::string a, std::string s)
    : endpoint(std::move(ep)), bucket(std::move(b)), access(std::move(a)),
      secret(std::move(s))
  {
    c = curl_easy_init();
  }
  ~S3() { curl_easy_cleanup(c); }
  S3(const S3&) = delete;
  S3& operator=(const S3&) = delete;

  http_result request(const std::string& method, const std::string& key,
                      const std::string& query,
                      std::vector<std::string> headers, const char* body,
                      size_t body_len, bool upload)
  {
    http_result r;
    curl_easy_reset(c);
    std::string url = endpoint + "/" + bucket + (key.empty() ? "" : "/" + key) +
      (query.empty() ? "" : "?" + query);
    headers.push_back("Expect:");
    const auto req = ofi_s3::sigv4_sign(
      method, url, headers,
      body ? ofi_s3::sha256_hex(body, body_len) : ofi_s3::empty_sha256(),
      access, secret);
    curl_easy_setopt(c, CURLOPT_URL, req.url.c_str());
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS, timeout_ms);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    struct curl_slist* hs = nullptr;
    for (auto& h : req.headers) {
      hs = curl_slist_append(hs, h.c_str());
    }
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hs);
    curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, on_header);
    curl_easy_setopt(c, CURLOPT_HEADERDATA, &r);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, on_body);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &r);
    upload_src src{body, body_len};
    if (upload) {
      curl_easy_setopt(c, CURLOPT_UPLOAD, 1L);
      curl_easy_setopt(c, CURLOPT_READFUNCTION, on_read);
      curl_easy_setopt(c, CURLOPT_READDATA, &src);
      curl_easy_setopt(c, CURLOPT_INFILESIZE_LARGE, (curl_off_t)body_len);
    } else if (method == "POST") {
      curl_easy_setopt(c, CURLOPT_POST, 1L);
      curl_easy_setopt(c, CURLOPT_POSTFIELDS, body ? body : "");
      curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, (long)body_len);
    } else if (method != "GET") {
      curl_easy_setopt(c, CURLOPT_CUSTOMREQUEST, method.c_str());
    }
    if (CURLcode cc = curl_easy_perform(c); cc != CURLE_OK) {
      r.error = curl_easy_strerror(cc);
    }
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &r.status);
    curl_slist_free_all(hs);
    return r;
  }
};

// ---------------------------------------------------------------- objects

struct object_t {
  std::string key;
  uint32_t id;
  uint64_t size;
  uint64_t part;
  bool crc;
};

std::vector<object_t> builtin_set(bool crc, bool big)
{
  const uint64_t Mi = 1 << 20;
  std::vector<object_t> v = {
    {"", 1, 1, 0, crc}, {"", 2, 4095, 0, crc}, {"", 3, 4097, 0, crc},
    {"", 4, 65537, 0, crc}, {"", 5, Mi, 0, crc}, {"", 6, 2 * Mi + 17, 0, crc},
    {"", 7, 4 * Mi - 1, 0, crc}, {"", 8, 4 * Mi, 0, crc},
    {"", 9, 4 * Mi + 1, 0, crc}, {"", 10, 5242887, 0, crc},
    {"", 11, 12 * Mi, 0, crc}, {"", 12, 16 * Mi, 0, crc},
    {"", 13, 16 * Mi + 1, 0, crc}, {"", 14, 40 * Mi, 0, crc},
    {"", 15, 33 * Mi + 12345, 5 * Mi + 7, false},
    {"", 16, 20 * Mi + 3, 8 * Mi, false}, {"", 17, 0, 0, false},
  };
  if (big) {
    v.push_back({"", 18, 100 * Mi + 5, 0, crc});
  }
  for (auto& o : v) {
    o.key = "soak-" + std::to_string(o.id) + "-" + std::to_string(o.size) +
      (o.part ? "-mp" : "");
  }
  return v;
}

int put_object(S3& s3, const object_t& o)
{
  if (!o.part) {
    const auto data = object_data(o.id, o.size);
    std::vector<std::string> h = {"Content-Type: application/octet-stream"};
    if (o.crc) {
      h.push_back("x-amz-checksum-crc64nvme: " +
                  ofi_s3::armor_crc64nvme(data.data(), data.size()));
    }
    auto r = s3.request("PUT", o.key, "", h, data.data(), data.size(), true);
    if (r.status != 200) {
      std::fprintf(stderr, "PUT %s: %ld %s %s\n", o.key.c_str(), r.status,
                   r.error.c_str(), r.body.c_str());
      return -1;
    }
    return 0;
  }
  auto r = s3.request("POST", o.key, "uploads", {}, nullptr, 0, false);
  auto a = r.body.find("<UploadId>"), b = r.body.find("</UploadId>");
  if (r.status != 200 || a == std::string::npos || b == std::string::npos) {
    std::fprintf(stderr, "initiate %s: %ld %s %s\n", o.key.c_str(), r.status,
                 r.error.c_str(), r.body.c_str());
    return -1;
  }
  std::string upload = r.body.substr(a + 10, b - a - 10);
  std::string xml = "<CompleteMultipartUpload>";
  int n = 0;
  for (uint64_t off = 0; off < o.size; off += o.part) {
    uint64_t len = std::min(o.part, o.size - off);
    std::vector<char> data(len);
    fill_content(o.id, off, len, data.data());
    ++n;
    auto pr = s3.request("PUT", o.key,
                         "partNumber=" + std::to_string(n) + "&uploadId=" +
                         upload, {}, data.data(), len, true);
    if (pr.status != 200) {
      std::fprintf(stderr, "part %d of %s: %ld %s %s\n", n, o.key.c_str(),
                   pr.status, pr.error.c_str(), pr.body.c_str());
      return -1;
    }
    xml += "<Part><PartNumber>" + std::to_string(n) + "</PartNumber><ETag>" +
      pr.etag + "</ETag></Part>";
  }
  xml += "</CompleteMultipartUpload>";
  auto cr = s3.request("POST", o.key, "uploadId=" + upload, {}, xml.data(),
                       xml.size(), false);
  if (cr.status != 200 || cr.body.find("<Error>") != std::string::npos) {
    std::fprintf(stderr, "complete %s: %ld %s %s\n", o.key.c_str(), cr.status,
                 cr.error.c_str(), cr.body.c_str());
    return -1;
  }
  return 0;
}

std::vector<object_t> read_manifest(const std::string& path)
{
  std::vector<object_t> v;
  std::ifstream f(path);
  object_t o{};
  int crc = 0;
  while (f >> o.key >> o.id >> o.size >> o.part >> crc) {
    o.crc = crc;
    v.push_back(o);
  }
  return v;
}

// ---------------------------------------------------------------- soak

struct opts_t {
  int threads = 2, windows = 8;
  size_t window_size = 64 << 20;
  int quarantine_ms = 10000;
  double quarantine_prob = 1.0;  // share of GETs whose window is watched
  // pool rdma_delivery_lease + rdma_delivery_drain +
  // osd_oob_cutoff_late_tolerance
  int fence_ms = 9000;
  bool rekey = false;   // new key for every GET
  uint64_t read_size = 0, read_align = 0;  // aligned fixed-size ranged GETs
  double duration_s = 60;
  long count = -1;
  double range_prob = 0.3, subwin_prob = 0.3, small_token_prob = 0.03;
  long timeout_ms = 60000;
  uint64_t seed = 1;
  std::string log;
  std::set<uint32_t> only;
  double stats_s = 10;
  // poison checked on each side of a token, before and after its GET
  uint64_t guard = 1 << 20;
  long inject_stray = 0;  // a stray write after every Nth GET, as a test
};

std::atomic<bool> stop{false};
void on_signal(int) { stop = true; }

struct window_slot {
  ceph::ofi::Endpoint::window_t w;
  enum { FREE, BUSY, QUARANTINE } state = FREE;
  // the poison the window holds, but in [dirty_off, dirty_off+dirty_len),
  // the bytes its last GET asked for; repoison when it may hold other
  // bytes, as after a violation or a late write
  uint64_t pseq = 0;
  uint64_t dirty_off = 0, dirty_len = 0;
  bool repoison = false;
  bool stray = false;  // --inject-stray wrote a word that no check found yet
  uint64_t qseq = 0;
  clk::time_point qstart, qend;
  clk::time_point allowed_until;  // writes may still land until then
  clk::time_point last_scan;
  std::string last_op;  // what used it last, for late-write reports
  size_t late_reported = 0;
};

struct soak_t {
  opts_t o;
  ceph::ofi::Endpoint* ep = nullptr;
  std::vector<object_t> objs;
  std::vector<window_slot> slots;
  std::mutex m;
  std::condition_variable cv;
  std::atomic<uint64_t> seq{1};
  std::atomic<bool> workers_done{false};
  std::mutex logm;
  FILE* logf = nullptr;
  // counters
  std::atomic<uint64_t> rekey_us{0}, rekeys{0};
  std::atomic<uint64_t> ops{0}, placed{0}, http{0}, errors{0}, violations{0},
    late{0}, late_allowed{0}, suspect{0}, watched{0}, placed_bytes{0},
    http_bytes{0}, small_tok{0}, crc_checked{0}, injected{0};
  std::map<std::string, uint64_t> errkinds;
  clk::time_point t0 = clk::now();

  void logline(const std::string& s)
  {
    std::lock_guard l(logm);
    if (logf) {
      std::fputs(s.c_str(), logf);
      std::fputc('\n', logf);
      std::fflush(logf);
    }
  }

  double now_s() const
  {
    return std::chrono::duration<double>(clk::now() - t0).count();
  }

  void violation(const std::string& what)
  {
    violations++;
    std::printf("VIOLATION t=%.1f %s\n", now_s(), what.c_str());
    std::fflush(stdout);
    logline("{\"t\":" + std::to_string(now_s()) + ",\"violation\":" +
            jstr(what) + "}");
  }

  void error_kind(const std::string& k)
  {
    errors++;
    std::lock_guard l(m);
    errkinds[k]++;
  }

  int take_window()
  {
    std::unique_lock l(m);
    for (;;) {
      for (size_t i = 0; i < slots.size(); i++) {
        if (slots[i].state == window_slot::FREE) {
          slots[i].state = window_slot::BUSY;
          return (int)i;
        }
      }
      if (stop) {
        return -1;
      }
      cv.wait_for(l, 200ms);
    }
  }

  // check a quarantined window: anything but its poison is a late write
  void scan_quarantine(window_slot& s, bool final)
  {
    ep->sync();
    const auto now = clk::now();
    const char* p = s.w.ptr;
    uint64_t pw = poison_word(s.qseq);
    const uint64_t* q = reinterpret_cast<const uint64_t*>(p);
    size_t n = s.w.len / 8, bad = 0, first = SIZE_MAX;
    for (size_t i = 0; i < n; i++) {
      if (q[i] != pw) {
        bad++;
        if (first == SIZE_MAX) {
          first = i * 8;
        }
      }
    }
    if (bad > s.late_reported) {
      double ms = std::chrono::duration<double, std::milli>(
        now - s.qstart).count();
      double past = std::chrono::duration<double, std::milli>(
        s.last_scan - s.allowed_until).count();
      std::string what = std::to_string(bad - s.late_reported) +
        " words changed in window " + std::to_string(&s - slots.data()) +
        ", seen " + std::to_string((long)ms) + " ms after its GET ended (" +
        s.last_op + "); first at" + decode_at(p, s.w.len, first) +
        (final ? " (final scan)" : "");
      if (s.last_scan >= s.allowed_until) {
        late += bad - s.late_reported;
        violation("late write " + std::to_string((long)past) +
                  "+ ms after the fence: " + what);
      } else if (now <= s.allowed_until) {
        late_allowed += bad - s.late_reported;
        std::printf("allowed late write (within the fence): %s\n",
                    what.c_str());
        logline("{\"t\":" + std::to_string(now_s()) + ",\"late_allowed\":" +
                jstr(what) + "}");
      } else {
        suspect += bad - s.late_reported;
        std::printf("SUSPECT late write (landed between scans across the "
                    "fence): %s\n", what.c_str());
        logline("{\"t\":" + std::to_string(now_s()) + ",\"suspect\":" +
                jstr(what) + "}");
      }
      std::fflush(stdout);
      s.late_reported = bad;
    }
    s.last_scan = now;
  }

  // the last scan of a quarantined window, then back to use; a window
  // that a late write changed is poisoned again before its next GET
  void end_quarantine(window_slot& s)
  {
    scan_quarantine(s, true);
    std::lock_guard l(m);
    s.repoison = s.repoison || s.late_reported > 0;
    s.state = window_slot::FREE;
    s.late_reported = 0;
  }

  // the whole window but its dirty bytes must be poison: the checks of a
  // GET cover only its token and the guard bands
  void scan_window(window_slot& s, const std::string& opdesc)
  {
    const char* p = s.w.ptr;
    size_t bad = first_non_poison(p, 0, s.dirty_off, s.pseq);
    if (bad == SIZE_MAX) {
      bad = first_non_poison(p, s.dirty_off + s.dirty_len, s.w.len, s.pseq);
    }
    if (bad != SIZE_MAX) {
      violation("write outside the requested range at window byte " +
                std::to_string(bad) + ", found by a scan of window " +
                std::to_string(&s - slots.data()) + " after " + opdesc +
                " (from it, or from a GET since the window's last scan):" +
                decode_at(p, s.w.len, bad));
      s.repoison = true;
    }
  }

  // at the end of the run: scan every window that is not in quarantine
  void scan_all()
  {
    ep->sync();
    for (auto& s : slots) {
      if (s.state == window_slot::FREE && !s.repoison) {
        scan_window(s, "the last GET of the run");
      }
    }
  }

  void monitor()
  {
    while (!stop) {
      std::this_thread::sleep_for(250ms);
      std::vector<int> due, watch;
      {
        std::lock_guard l(m);
        for (size_t i = 0; i < slots.size(); i++) {
          if (slots[i].state != window_slot::QUARANTINE) {
            continue;
          }
          (clk::now() >= slots[i].qend ? due : watch).push_back((int)i);
        }
      }
      for (int i : watch) {
        scan_quarantine(slots[i], false);
      }
      for (int i : due) {
        end_quarantine(slots[i]);
      }
      if (!due.empty()) {
        cv.notify_all();
      }
    }
    // drain: once the workers are done, finish every quarantine, so that
    // late writes at the end are seen too
    for (;;) {
      const bool last = workers_done;
      std::vector<int> due, watch;
      {
        std::lock_guard l(m);
        for (size_t i = 0; i < slots.size(); i++) {
          if (slots[i].state != window_slot::QUARANTINE) {
            continue;
          }
          (clk::now() >= slots[i].qend ? due : watch).push_back((int)i);
        }
      }
      if (last && due.empty() && watch.empty()) {
        break;
      }
      for (int i : watch) {
        scan_quarantine(slots[i], false);
      }
      for (int i : due) {
        end_quarantine(slots[i]);
      }
      std::this_thread::sleep_for(200ms);
    }
  }

  // A GET that fell back or failed is always watched: that is where a late
  // write would come from. Placed GETs are sampled, but only while fewer
  // than half the spare windows are in quarantine, so the workers never
  // starve for windows and the load stays continuous.
  void release_to_quarantine(int wi, const std::string& opdesc, bool sampled,
                             bool important, bool failed,
                             clk::time_point ended)
  {
    auto& s = slots[wi];
    if (o.rekey) {
      // the GET is over: retire its key before this memory is written
      // again, so a late duplicate that still carries the old key cannot
      // land
      auto r0 = clk::now();
      if (int r = ep->rekey_window(s.w); r < 0) {
        std::printf("re-key: %s (%s)\n", std::strerror(-r),
                    ep->last_error().c_str());
        stop = true;
      }
      rekey_us += std::chrono::duration_cast<std::chrono::microseconds>(
        clk::now() - r0).count();
      rekeys++;
    }
    bool watch = important;
    if (!watch && sampled) {
      std::lock_guard l(m);
      int q = 0;
      for (auto& x : slots) {
        q += x.state == window_slot::QUARANTINE;
      }
      watch = q < std::max(1, ((int)slots.size() - o.threads - 1) / 2);
    }
    if (watch) {
      watched++;
    }
    if (!watch) {
      // reuse at once: the next GET poisons the dirty bytes again, and the
      // window's next scan catches a stray write
      std::lock_guard l(m);
      s.state = window_slot::FREE;
      cv.notify_all();
      return;
    }
    if (!s.repoison) {
      scan_window(s, opdesc);
    }
    if (s.repoison) {
      s.pseq = seq++;
      fill_poison(s.w.ptr, s.w.len, s.pseq);
      s.stray = false;
      s.repoison = false;
    } else {
      poison_range(s.w.ptr, s.dirty_off, s.dirty_off + s.dirty_len, s.pseq);
    }
    s.dirty_off = s.dirty_len = 0;
    s.qseq = s.pseq;
    s.last_op = opdesc;
    s.late_reported = 0;
    std::lock_guard l(m);
    s.qstart = clk::now();
    s.last_scan = s.qstart;
    s.allowed_until = failed ? ended + std::chrono::milliseconds(o.fence_ms)
                             : s.qstart;
    s.qend = std::max(s.qstart + std::chrono::milliseconds(o.quarantine_ms),
                      s.allowed_until +
                      std::chrono::milliseconds(o.quarantine_ms));
    s.state = window_slot::QUARANTINE;
  }

  void worker(int tid, S3* s3)
  {
    std::mt19937_64 rng(o.seed * 7919 + tid);
    std::uniform_real_distribution<double> U(0, 1);
    std::vector<char> exp(1 << 20);
    std::vector<size_t> eligible;
    for (size_t i = 0; i < objs.size(); i++) {
      if (objs[i].size <= o.window_size &&
          (o.only.empty() || o.only.count(objs[i].id))) {
        eligible.push_back(i);
      }
    }
    if (eligible.empty()) {
      std::printf("no objects fit the window\n");
      stop = true;
      return;
    }
    while (!stop) {
      if (o.count >= 0 && (long)ops.load() >= o.count) {
        stop = true;
        break;
      }
      int wi = take_window();
      if (wi < 0) {
        break;
      }
      auto& slot = slots[wi];
      const auto& ob = objs[eligible[rng() % eligible.size()]];
      // the request
      uint64_t off = 0, len = ob.size;
      bool ranged = ob.size > 1 && U(rng) < o.range_prob;
      if (o.read_size && ob.size > 0) {
        const uint64_t a = o.read_align ? o.read_align : o.read_size;
        const uint64_t nslots = (ob.size + a - 1) / a;
        off = (rng() % nslots) * a;
        len = std::min<uint64_t>(o.read_size, ob.size - off);
        ranged = !(off == 0 && len == ob.size);
      } else if (ranged) {
        off = rng() % ob.size;
        uint64_t maxlen = ob.size - off;
        double r = U(rng);
        len = r < 0.3 ? 1 + rng() % std::min<uint64_t>(maxlen, 9000)
            : r < 0.6 ? 1 + rng() % std::min<uint64_t>(maxlen, 5 << 20)
            : 1 + rng() % maxlen;
      }
      // where the token puts it
      uint64_t toklen = std::max<uint64_t>(len, 1), tokoff = 0;
      bool small = len > 1 && U(rng) < o.small_token_prob;
      if (small) {
        toklen = len - 1 - rng() % std::min<uint64_t>(len - 1, 4096);
      }
      if (U(rng) < o.subwin_prob) {
        uint64_t slack = slot.w.len - toklen;
        tokoff = slack ? rng() % (slack + 1) : 0;
        if (!small && U(rng) < 0.5) {
          toklen += rng() % (slot.w.len - tokoff - toklen + 1);
        }
      }
      // the window holds its poison, but the bytes its last GET asked for
      if (slot.repoison) {
        slot.pseq = seq++;
        fill_poison(slot.w.ptr, slot.w.len, slot.pseq);
        slot.stray = false;
        slot.repoison = false;
      } else {
        poison_range(slot.w.ptr, slot.dirty_off,
                     slot.dirty_off + slot.dirty_len, slot.pseq);
      }
      slot.dirty_off = slot.dirty_len = 0;
      std::string token = ep->window_token(slot.w, tokoff, toklen);
      std::vector<std::string> h = {"x-amz-rdma-token: " + token};
      if (ranged) {
        h.push_back("Range: bytes=" + std::to_string(off) + "-" +
                    std::to_string(off + len - 1));
      }
      std::ostringstream desc;
      desc << "op " << ops.load() << " t" << tid << " obj " << ob.id << " ["
           << off << "+" << len << "] tok [" << tokoff << "+" << toklen << "]"
           << (small ? " small" : "");
      // the bytes that this GET's checks cover must hold the poison before
      // it, so that a stray write of an earlier GET there is found before
      // this GET's data covers it
      const uint64_t lo = tokoff > o.guard ? tokoff - o.guard : 0;
      const uint64_t hi = std::min<uint64_t>(slot.w.len,
                                             tokoff + toklen + o.guard);
      if (size_t b = first_non_poison(slot.w.ptr, lo, hi, slot.pseq);
          b != SIZE_MAX) {
        violation("write outside the requested range at window byte " +
                  std::to_string(b) + ", found before " + desc.str() +
                  " (from a GET since the window's last scan):" +
                  decode_at(slot.w.ptr, slot.w.len, b));
        slot.pseq = seq++;
        fill_poison(slot.w.ptr, slot.w.len, slot.pseq);
        slot.stray = false;
      }
      const uint64_t fill = slot.pseq;
      auto t1 = clk::now();
      auto r = s3->request("GET", ob.key, "", h, nullptr, 0, false);
      // writes placed before the reply are in the window: take the
      // endpoint's lock so this thread reads after its progress thread
      ep->sync();
      const auto t2 = clk::now();
      double ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
      ops++;
      std::string mode, result = "ok", detail;
      const long want_status = ranged ? 206 : 200;
      if (!r.error.empty() || r.status != want_status) {
        mode = "error";
        result = "err";
        detail = r.error.empty() ? "HTTP " + std::to_string(r.status)
                                 : r.error;
        error_kind(detail);
        // the window may hold partial correct data; still check nothing
        // strays
      } else if (r.reply == std::to_string(want_status)) {
        // the gateway answers a placed ranged GET with x-amz-rdma-reply 206
        mode = "placed";
        placed++;
        placed_bytes += len;
        if (small) {
          result = "violation";
          violation("placed although the token was smaller than the range: " +
                    desc.str());
        }
      } else {
        mode = "http";
        http++;
        http_bytes += len;
        if (small) {
          small_tok++;
        }
        if (r.body.size() != len) {
          result = "violation";
          violation("HTTP body " + std::to_string(r.body.size()) +
                    " bytes, want " + std::to_string(len) + ": " +
                    desc.str());
        } else {
          for (uint64_t c = 0; c < len && result == "ok"; c += exp.size()) {
            uint64_t n = std::min<uint64_t>(exp.size(), len - c);
            fill_content(ob.id, off + c, n, exp.data());
            if (std::memcmp(exp.data(), r.body.data() + c, n) != 0) {
              uint64_t i = 0;
              while (i < n && exp[i] == r.body[c + i]) {
                i++;
              }
              result = "violation";
              violation("HTTP body wrong at byte " + std::to_string(c + i) +
                        ": " + desc.str());
            }
          }
        }
      }
      // the window: inside the span the OSDs may write, the first
      // min(len, toklen) bytes of the token, either our data (placed: must
      // be) or poison (not placed); outside it, poison only. A token
      // smaller than the range ends inside the window, where the range
      // need not.
      if (result != "violation") {
        const char* w = slot.w.ptr;
        bool must = mode == "placed";
        const uint64_t span = std::min(len, toklen);
        if (must && r.bytes != std::to_string(len)) {
          detail += " bytes-transferred " + r.bytes;
          result = "violation";
          violation("x-amz-rdma-bytes-transferred " + r.bytes + ", want " +
                    std::to_string(len) + ": " + desc.str());
        }
        // inside [tokoff, tokoff+span)
        for (uint64_t c = 0; c < span && result != "violation";
             c += exp.size()) {
          uint64_t n = std::min<uint64_t>(exp.size(), span - c);
          fill_content(ob.id, off + c, n, exp.data());
          const char* got = w + tokoff + c;
          if (std::memcmp(exp.data(), got, n) == 0) {
            continue;
          }
          for (uint64_t i = 0; i < n; i++) {
            if (got[i] == exp[i]) {
              continue;
            }
            if (!must &&
                (unsigned char)got[i] == poison_byte(fill, tokoff + c + i)) {
              continue;
            }
            result = "violation";
            violation(std::string(must ? "placed data wrong"
                                       : "stray bytes in window after "
                                         "fallback") +
                      " at request byte " + std::to_string(c + i) + ":" +
                      decode_at(w, slot.w.len, tokoff + c + i) + " " +
                      desc.str());
            break;
          }
        }
        // the OSDs' CRC of what they placed, against this client's CRC of
        // the bytes it found
        if (must && result != "violation" && !r.crc64.empty()) {
          crc_checked++;
          const std::string mine = ofi_s3::armor_crc64nvme(w + tokoff, len);
          if (mine != r.crc64) {
            result = "violation";
            violation("x-amz-rdma-checksum-crc64nvme " + r.crc64 +
                      ", the window's is " + mine + ": " + desc.str());
          }
        }
        // outside [tokoff, tokoff+span): the rest of the token and a
        // guard band on each side must be poison. The scan of the window
        // before its next quarantine checks the rest.
        if (result != "violation") {
          auto check_poison = [&](uint64_t from, uint64_t to) {
            const size_t i = first_non_poison(w, from, to, fill);
            if (i != SIZE_MAX) {
              result = "violation";
              violation("write outside the requested range at window "
                        "byte " + std::to_string(i) + " (range is [" +
                        std::to_string(tokoff) + "," +
                        std::to_string(tokoff + span) + ")):" +
                        decode_at(w, slot.w.len, i) + " " + desc.str());
            }
          };
          check_poison(tokoff > o.guard ? tokoff - o.guard : 0, tokoff);
          if (result != "violation") {
            check_poison(tokoff + span,
                         std::min<uint64_t>(slot.w.len,
                                            tokoff + toklen + o.guard));
          }
        }
      }
      slot.dirty_off = tokoff;
      slot.dirty_len = std::min(len, toklen);
      if (result == "violation") {
        // what broke the window may be anywhere in it
        slot.repoison = true;
      } else if (o.inject_stray > 0 && !slot.stray &&
                 (long)ops.load() % o.inject_stray == 0) {
        // a stray write outside what this GET's checks covered, which a
        // later check or scan of the window must find
        uint64_t at = UINT64_MAX;
        if (((hi + 7) & ~7ull) + 8 <= slot.w.len) {
          at = (hi + 7) & ~7ull;
        } else if (lo >= 16) {
          at = (lo - 8) & ~7ull;
        }
        if (at != UINT64_MAX) {
          const uint64_t word = content_word(ob.id, at >> 3);
          std::memcpy(slot.w.ptr + at, &word, 8);
          slot.stray = true;
          injected++;
        }
      }
      std::ostringstream js;
      js << "{\"t\":" << now_s() << ",\"tid\":" << tid << ",\"win\":" << wi
         << ",\"obj\":" << ob.id << ",\"size\":" << ob.size << ",\"off\":"
         << off << ",\"len\":" << len << ",\"ranged\":" << ranged
         << ",\"tokoff\":" << tokoff << ",\"toklen\":" << toklen
         << ",\"small\":" << small << ",\"status\":" << r.status
         << ",\"reply\":" << jstr(r.reply) << ",\"mode\":" << jstr(mode)
         << ",\"result\":" << jstr(result) << ",\"ms\":" << ms
         << ",\"detail\":" << jstr(detail) << "}";
      logline(js.str());
      release_to_quarantine(wi, desc.str() + " " + mode,
                            U(rng) < o.quarantine_prob, mode != "placed",
                            mode == "error", t2);
    }
  }

  void print_stats(const char* tag)
  {
    double t = now_s();
    std::string ek;
    {
      std::lock_guard l(m);
      for (auto& [k, v] : errkinds) {
        ek += " [" + k + "]=" + std::to_string(v);
      }
    }
    std::printf("%s t=%.0fs ops=%lu placed=%lu http=%lu (small-token %lu) "
                "errors=%lu violations=%lu late=%lu late-allowed=%lu "
                "suspect=%lu watched=%lu crc-checked=%lu placed %.0f MiB/s "
                "http %.0f MiB/s%s\n", tag, t,
                (unsigned long)ops.load(), (unsigned long)placed.load(),
                (unsigned long)http.load(), (unsigned long)small_tok.load(),
                (unsigned long)errors.load(), (unsigned long)violations.load(),
                (unsigned long)late.load(), (unsigned long)late_allowed.load(),
                (unsigned long)suspect.load(), (unsigned long)watched.load(),
                (unsigned long)crc_checked.load(),
                placed_bytes.load() / 1048576.0 / std::max(t, 1e-3),
                http_bytes.load() / 1048576.0 / std::max(t, 1e-3), ek.c_str());
    std::fflush(stdout);
  }
};

uint64_t parse_size(const std::string& s)
{
  char* e;
  double v = std::strtod(s.c_str(), &e);
  switch (*e) {
  case 'k': case 'K': v *= 1024; break;
  case 'm': case 'M': v *= 1048576; break;
  case 'g': case 'G': v *= 1073741824; break;
  }
  return (uint64_t)v;
}

// startup stages go to stderr unbuffered, with a timestamp, so a client
// that stalls before its first STATS line still says where
void stage(const char* fmt, ...)
{
  static const auto t0 = clk::now();
  va_list ap;
  va_start(ap, fmt);
  std::fprintf(stderr, "[stage %.3f] ",
               std::chrono::duration<double>(clk::now() - t0).count());
  std::vfprintf(stderr, fmt, ap);
  std::fputc('\n', stderr);
  va_end(ap);
}

int cmd_run(int argc, char** argv)
{
  if (argc < 10) {
    std::fprintf(stderr, "run: missing arguments\n");
    return 2;
  }
  soak_t s;
  ceph::ofi::config_t cfg;
  cfg.provider = argv[2];
  cfg.domain = std::strcmp(argv[3], "-") ? argv[3] : "";
  cfg.node = std::strcmp(argv[4], "-") ? argv[4] : "";
  cfg.progress_thread = true;
  std::string endpoint = argv[5], bucket = argv[6], access = argv[7],
    secret = argv[8];
  s.objs = read_manifest(argv[9]);
  for (int i = 10; i < argc; i++) {
    std::string a = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "%s needs a value\n", a.c_str());
        std::exit(2);
      }
      return argv[++i];
    };
    try {
      if (a == "--threads") s.o.threads = std::stoi(next());
      else if (a == "--windows") s.o.windows = std::stoi(next());
      else if (a == "--window-size") s.o.window_size = parse_size(next());
      else if (a == "--quarantine-ms") s.o.quarantine_ms = std::stoi(next());
      else if (a == "--quarantine-prob") s.o.quarantine_prob = std::stod(next());
      else if (a == "--fence-ms") s.o.fence_ms = std::stoi(next());
      else if (a == "--rekey") s.o.rekey = true;
      else if (a == "--guard") s.o.guard = parse_size(next());
      else if (a == "--inject-stray") s.o.inject_stray = std::stol(next());
      else if (a == "--read-size") s.o.read_size = parse_size(next());
      else if (a == "--read-align") s.o.read_align = parse_size(next());
      else if (a == "--duration") s.o.duration_s = std::stod(next());
      else if (a == "--count") s.o.count = std::stol(next());
      else if (a == "--range-prob") s.o.range_prob = std::stod(next());
      else if (a == "--subwin-prob") s.o.subwin_prob = std::stod(next());
      else if (a == "--small-token-prob") s.o.small_token_prob = std::stod(next());
      else if (a == "--timeout-ms") s.o.timeout_ms = std::stol(next());
      else if (a == "--seed") s.o.seed = std::stoull(next());
      else if (a == "--log") s.o.log = next();
      else if (a == "--stats-s") s.o.stats_s = std::stod(next());
      else if (a == "--only-ids") {
        std::stringstream ss(next());
        std::string t;
        while (std::getline(ss, t, ',')) {
          s.o.only.insert(std::stoul(t));
        }
      } else {
        std::fprintf(stderr, "unknown option %s\n", a.c_str());
        return 2;
      }
    } catch (const std::exception& e) {
      std::fprintf(stderr, "%s: bad value (%s)\n", a.c_str(), e.what());
      return 2;
    }
  }
  if (s.objs.empty()) {
    std::fprintf(stderr, "empty manifest\n");
    return 2;
  }
  if (s.o.threads < 1 || s.o.window_size == 0) {
    std::fprintf(stderr, "--threads and --window-size must be positive\n");
    return 2;
  }
  if (s.o.windows < s.o.threads) {
    s.o.windows = s.o.threads;
  }
  std::string err;
  stage("opening endpoint (provider %s, domain %s)", cfg.provider.c_str(),
        cfg.domain.c_str());
  auto ep = ceph::ofi::Endpoint::open(cfg, &err);
  if (!ep) {
    std::printf("libfabric: %s\n", err.c_str());
    std::fflush(stdout);
    return 1;
  }
  stage("endpoint open");
  s.ep = ep.get();
  std::printf("endpoint: %s; %d threads, %d windows of %zu bytes, "
              "quarantine %d ms, fence %d ms\n", ep->describe().c_str(),
              s.o.threads, s.o.windows, s.o.window_size, s.o.quarantine_ms,
              s.o.fence_ms);
  s.slots.resize(s.o.windows);
  for (auto& sl : s.slots) {
    void* p = mmap(nullptr, s.o.window_size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (p == MAP_FAILED) {
      std::perror("mmap");
      return 1;
    }
    sl.pseq = s.seq++;
    fill_poison((char*)p, s.o.window_size, sl.pseq);
    stage("registering window %zu", (size_t)(&sl - s.slots.data()));
    if (int r = ep->register_window((char*)p, s.o.window_size, &sl.w); r < 0) {
      std::printf("register_window: %s (%s)\n", std::strerror(-r),
                  ep->last_error().c_str());
      return 1;
    }
  }
  stage("windows registered");
  std::fflush(stdout);
  if (!s.o.log.empty()) {
    s.logf = std::fopen(s.o.log.c_str(), "a");
  }
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  std::vector<std::unique_ptr<S3>> conns;
  std::vector<std::thread> ts;
  std::thread mon([&] { s.monitor(); });
  for (int t = 0; t < s.o.threads; t++) {
    conns.push_back(std::make_unique<S3>(endpoint, bucket, access, secret));
    conns.back()->timeout_ms = s.o.timeout_ms;
  }
  for (int t = 0; t < s.o.threads; t++) {
    ts.emplace_back([&, t] { s.worker(t, conns[t].get()); });
  }
  auto end = clk::now() + std::chrono::duration<double>(s.o.duration_s);
  auto next_stats = clk::now() + std::chrono::duration<double>(s.o.stats_s);
  while (!stop && (s.o.count >= 0 || clk::now() < end)) {
    std::this_thread::sleep_for(100ms);
    if (clk::now() >= next_stats) {
      s.print_stats("STATS");
      next_stats += std::chrono::duration<double>(s.o.stats_s);
    }
  }
  stop = true;
  for (auto& t : ts) {
    t.join();
  }
  s.workers_done = true;
  mon.join();
  s.scan_all();
  auto st = ep->stats();
  s.print_stats("FINAL");
  std::printf("endpoint stats: windows %lu resets %lu\n",
              (unsigned long)st.windows, (unsigned long)st.resets);
  if (s.injected) {
    std::printf("injected %lu stray writes\n",
                (unsigned long)s.injected.load());
  }
  if (s.rekeys) {
    std::printf("rekey: %lu re-keys (%lu in place, %lu re-registered), "
                "mean %.1f us\n", (unsigned long)s.rekeys.load(),
                (unsigned long)st.rekeys_in_place,
                (unsigned long)st.rekeys_reregistered,
                (double)s.rekey_us.load() / s.rekeys.load());
  }
  for (auto& sl : s.slots) {
    ep->deregister_window(sl.w.id);
  }
  if (s.logf) {
    std::fclose(s.logf);
  }
  return s.violations ? 3 : 0;
}

int cmd_put(int argc, char** argv)
{
  if (argc < 7) {
    std::fprintf(stderr, "put: missing arguments\n");
    return 2;
  }
  bool crc = false, big = false;
  for (int i = 7; i < argc; i++) {
    if (!std::strcmp(argv[i], "--crc")) {
      crc = true;
    } else if (!std::strcmp(argv[i], "--big")) {
      big = true;
    } else {
      std::fprintf(stderr, "unknown option %s\n", argv[i]);
      return 2;
    }
  }
  S3 s3(argv[2], argv[3], argv[4], argv[5]);
  // the bucket; it may exist
  s3.request("PUT", "", "", {}, nullptr, 0, true);
  auto set = builtin_set(crc, big);
  std::ofstream man(argv[6]);
  int fails = 0;
  for (auto& o : set) {
    if (put_object(s3, o) < 0) {
      fails++;
      continue;
    }
    man << o.key << " " << o.id << " " << o.size << " " << o.part << " "
        << (o.crc ? 1 : 0) << "\n";
    std::printf("put %s (%lu bytes%s%s)\n", o.key.c_str(),
                (unsigned long)o.size, o.part ? ", multipart" : "",
                o.crc ? ", crc64nvme" : "");
  }
  return fails ? 1 : 0;
}

} // anonymous namespace

int main(int argc, char** argv)
{
  if (!ofi_s3::self_test()) {
    std::fprintf(stderr, "CRC self-test failed\n");
    return 2;
  }
  curl_global_init(CURL_GLOBAL_DEFAULT);
  if (argc >= 2 && !std::strcmp(argv[1], "run")) {
    return cmd_run(argc, argv);
  }
  if (argc >= 2 && !std::strcmp(argv[1], "put")) {
    return cmd_put(argc, argv);
  }
  if (argc == 4 && !std::strcmp(argv[1], "gen")) {
    uint32_t id = std::stoul(argv[2]);
    uint64_t size = parse_size(argv[3]);
    std::vector<char> b(1 << 20);
    for (uint64_t o = 0; o < size; o += b.size()) {
      uint64_t n = std::min<uint64_t>(b.size(), size - o);
      fill_content(id, o, n, b.data());
      fwrite(b.data(), 1, n, stdout);
    }
    return 0;
  }
  if (argc == 4 && !std::strcmp(argv[1], "crc")) {
    const auto data = object_data(std::stoul(argv[2]), parse_size(argv[3]));
    std::printf("%016lx %s\n",
                (unsigned long)ofi_s3::crc64nvme(data.data(), data.size()),
                ofi_s3::armor_crc64nvme(data.data(), data.size()).c_str());
    return 0;
  }
  std::fprintf(stderr, "usage: see the comment at the top of "
               "src/test/rgw/ofi/ofi_s3_soak.cc\n");
  return 2;
}
