// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

/*
 * AWS Signature Version 4 for the S3 requests of the ofi test clients.
 *
 * The clients sign their requests here, and not with libcurl's
 * CURLOPT_AWS_SIGV4, because what libcurl signs depends on its version.
 * The libcurl 7.76.1 of CentOS Stream 9 signs x-amz-content-sha256 but
 * does not send it, and it does not sign the other x-amz-* headers, such
 * as x-amz-rdma-token. The gateway refuses both, so every request of the
 * clients failed there with 403.
 *
 * A request signs every header that it sends with a value, and host,
 * x-amz-date and x-amz-content-sha256. x-amz-content-sha256 is the
 * SHA-256 of the body, so the gateway checks the body too.
 */

#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <algorithm>
#include <cstdio>
#include <ctime>
#include <string>
#include <utility>
#include <vector>

namespace ofi_s3 {

inline std::string to_hex(const unsigned char* p, size_t n)
{
  static const char* digits = "0123456789abcdef";
  std::string out;
  for (size_t i = 0; i < n; i++) {
    out += digits[p[i] >> 4];
    out += digits[p[i] & 15];
  }
  return out;
}

/// the SHA-256 of n bytes at p, in lower-case hex
inline std::string sha256_hex(const void* p, size_t n)
{
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int len = 0;
  EVP_Digest(p, n, md, &len, EVP_sha256(), nullptr);
  return to_hex(md, len);
}

/// the SHA-256 of an empty body
inline const std::string& empty_sha256()
{
  static const std::string h = sha256_hex("", 0);
  return h;
}

inline std::string hmac_sha256(const std::string& key, const std::string& msg)
{
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int len = 0;
  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const unsigned char*>(msg.data()), msg.size(), md,
       &len);
  return std::string(reinterpret_cast<const char*>(md), len);
}

/// s with every byte but the unreserved ones (and '/', when slash) as %XX
inline std::string uri_encode(const std::string& s, bool slash)
{
  std::string out;
  for (unsigned char c : s) {
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
        c == '~' || (slash && c == '/')) {
      out += static_cast<char>(c);
    } else {
      char b[4];
      std::snprintf(b, sizeof(b), "%%%02X", c);
      out += b;
    }
  }
  return out;
}

inline std::string lower(std::string s)
{
  for (auto& c : s) {
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c - 'A' + 'a');
    }
  }
  return s;
}

/// a header value as SigV4 signs it: no blanks around it, and one space
/// for each run of blanks inside it
inline std::string trim_value(const std::string& v)
{
  std::string out;
  bool blank = false;
  for (char c : v) {
    if (c == ' ' || c == '\t') {
      blank = !out.empty();
      continue;
    }
    if (blank) {
      out += ' ';
      blank = false;
    }
    out += c;
  }
  return out;
}

struct signed_request {
  std::string url;                   // the URL to request, encoded
  std::vector<std::string> headers;  // every header to send
};

/// Sign a request to S3 with SigV4.
///
/// url is scheme://host[:port]/path[?query], with the path and the query
/// not yet encoded. headers are "Name: value" lines: each one with a value
/// is signed, and one without, such as "Expect:", which libcurl takes as
/// "do not send this header", is passed on unsigned. payload_sha256 is
/// the SHA-256 of the body in hex. The result holds the encoded URL, and
/// the headers with Host, x-amz-date, x-amz-content-sha256 and
/// Authorization added.
inline signed_request sigv4_sign(const std::string& method,
                                 const std::string& url,
                                 const std::vector<std::string>& headers,
                                 const std::string& payload_sha256,
                                 const std::string& access,
                                 const std::string& secret,
                                 const std::string& region = "us-east-1",
                                 std::time_t now = std::time(nullptr))
{
  // scheme://authority/path?query
  const auto scheme_end = url.find("://");
  const size_t auth_start = scheme_end == std::string::npos ?
    0 : scheme_end + 3;
  const size_t path_start = std::min(url.find('/', auth_start), url.size());
  const std::string origin = url.substr(0, path_start);
  const std::string authority = url.substr(auth_start,
                                           path_start - auth_start);
  std::string rest = url.substr(path_start);
  std::string path = rest, query;
  if (auto q = rest.find('?'); q != std::string::npos) {
    path = rest.substr(0, q);
    query = rest.substr(q + 1);
  }
  if (path.empty()) {
    path = "/";
  }
  const std::string canonical_uri = uri_encode(path, true);

  // the query: each parameter encoded, sorted for the signature
  std::vector<std::pair<std::string, std::string>> params;
  std::string sent_query;
  for (size_t at = 0; !query.empty() && at <= query.size(); ) {
    size_t end = std::min(query.find('&', at), query.size());
    const std::string item = query.substr(at, end - at);
    at = end + 1;
    if (item.empty()) {
      continue;
    }
    const auto eq = item.find('=');
    const std::string name = uri_encode(item.substr(0, eq), false);
    const std::string value = eq == std::string::npos ?
      std::string{} : uri_encode(item.substr(eq + 1), false);
    params.emplace_back(name, value);
    sent_query += (sent_query.empty() ? "" : "&") + name +
      (eq == std::string::npos ? "" : "=" + value);
  }
  std::sort(params.begin(), params.end());
  std::string canonical_query;
  for (const auto& [name, value] : params) {
    canonical_query += (canonical_query.empty() ? "" : "&") + name + "=" +
      value;
  }

  char amz_date[32], date[16];
  std::tm tm{};
  gmtime_r(&now, &tm);
  std::strftime(amz_date, sizeof(amz_date), "%Y%m%dT%H%M%SZ", &tm);
  std::strftime(date, sizeof(date), "%Y%m%d", &tm);

  signed_request out;
  out.url = origin + canonical_uri +
    (sent_query.empty() ? "" : "?" + sent_query);
  out.headers = headers;
  // the headers to sign, by lower-case name
  std::vector<std::pair<std::string, std::string>> signed_hdrs;
  bool has_host = false;
  for (const auto& h : headers) {
    const auto colon = h.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    const std::string name = lower(trim_value(h.substr(0, colon)));
    const std::string value = trim_value(h.substr(colon + 1));
    if (value.empty()) {
      continue;
    }
    has_host = has_host || name == "host";
    signed_hdrs.emplace_back(name, value);
  }
  if (!has_host) {
    out.headers.push_back("Host: " + authority);
    signed_hdrs.emplace_back("host", authority);
  }
  out.headers.push_back(std::string("x-amz-date: ") + amz_date);
  signed_hdrs.emplace_back("x-amz-date", amz_date);
  out.headers.push_back("x-amz-content-sha256: " + payload_sha256);
  signed_hdrs.emplace_back("x-amz-content-sha256", payload_sha256);
  std::stable_sort(signed_hdrs.begin(), signed_hdrs.end(),
                   [](const auto& a, const auto& b) {
                     return a.first < b.first;
                   });
  // a header sent twice is signed once, with its values joined by commas
  std::string canonical_headers, signed_names;
  for (size_t i = 0; i < signed_hdrs.size(); ) {
    const std::string& name = signed_hdrs[i].first;
    std::string value = signed_hdrs[i].second;
    for (i++; i < signed_hdrs.size() && signed_hdrs[i].first == name; i++) {
      value += "," + signed_hdrs[i].second;
    }
    canonical_headers += name + ":" + value + "\n";
    signed_names += (signed_names.empty() ? "" : ";") + name;
  }

  const std::string canonical_request = method + "\n" + canonical_uri +
    "\n" + canonical_query + "\n" + canonical_headers + "\n" + signed_names +
    "\n" + payload_sha256;
  const std::string scope = std::string(date) + "/" + region +
    "/s3/aws4_request";
  const std::string string_to_sign = std::string("AWS4-HMAC-SHA256\n") +
    amz_date + "\n" + scope + "\n" +
    sha256_hex(canonical_request.data(), canonical_request.size());
  const std::string key = hmac_sha256(
    hmac_sha256(hmac_sha256(hmac_sha256("AWS4" + secret, date), region),
                "s3"),
    "aws4_request");
  const std::string sig = hmac_sha256(key, string_to_sign);
  out.headers.push_back(
    "Authorization: AWS4-HMAC-SHA256 Credential=" + access + "/" + scope +
    ", SignedHeaders=" + signed_names + ", Signature=" +
    to_hex(reinterpret_cast<const unsigned char*>(sig.data()), sig.size()));
  return out;
}

} // namespace ofi_s3
