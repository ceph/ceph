// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * The SigV4 signing of the ofi test clients (ofi_s3_sigv4.h), against the
 * examples of the S3 API reference, "Signature Calculations for the
 * Authorization Header: Transferring Payload in a Single Chunk".
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "ofi_s3_sigv4.h"

namespace {

const std::string access_key = "AKIAIOSFODNN7EXAMPLE";
const std::string secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const std::string endpoint = "https://examplebucket.s3.amazonaws.com";
// 20130524T000000Z
const std::time_t when = 1369353600;

std::string header(const ofi_s3::signed_request& r, const std::string& name)
{
  for (const auto& h : r.headers) {
    if (h.compare(0, name.size() + 1, name + ":") == 0) {
      return h.substr(name.size() + 2);
    }
  }
  return {};
}

std::string signature(const ofi_s3::signed_request& r)
{
  const std::string a = header(r, "Authorization");
  const auto at = a.find("Signature=");
  return at == std::string::npos ? std::string{} : a.substr(at + 10);
}

} // anonymous namespace

TEST(OfiS3SigV4, Sha256)
{
  EXPECT_EQ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ofi_s3::empty_sha256());
  const std::string body = "Welcome to Amazon S3.";
  EXPECT_EQ("44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
            ofi_s3::sha256_hex(body.data(), body.size()));
}

TEST(OfiS3SigV4, GetObject)
{
  const auto r = ofi_s3::sigv4_sign(
    "GET", endpoint + "/test.txt", {"Range: bytes=0-9"},
    ofi_s3::empty_sha256(), access_key, secret_key, "us-east-1", when);
  EXPECT_EQ(endpoint + "/test.txt", r.url);
  EXPECT_EQ("examplebucket.s3.amazonaws.com", header(r, "Host"));
  EXPECT_EQ("20130524T000000Z", header(r, "x-amz-date"));
  EXPECT_EQ(ofi_s3::empty_sha256(), header(r, "x-amz-content-sha256"));
  EXPECT_EQ("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/"
            "us-east-1/s3/aws4_request, SignedHeaders=host;range;"
            "x-amz-content-sha256;x-amz-date, Signature="
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
            header(r, "Authorization"));
}

TEST(OfiS3SigV4, PutObject)
{
  // the key is test$file.text: the path is encoded, and so is the URL
  const std::string body = "Welcome to Amazon S3.";
  const auto r = ofi_s3::sigv4_sign(
    "PUT", endpoint + "/test$file.text",
    {"Date: Fri, 24 May 2013 00:00:00 GMT",
     "x-amz-storage-class: REDUCED_REDUNDANCY"},
    ofi_s3::sha256_hex(body.data(), body.size()), access_key, secret_key,
    "us-east-1", when);
  EXPECT_EQ(endpoint + "/test%24file.text", r.url);
  EXPECT_NE(std::string::npos, header(r, "Authorization").find(
              "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;"
              "x-amz-storage-class,"));
  EXPECT_EQ("98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
            signature(r));
}

TEST(OfiS3SigV4, Subresource)
{
  // a parameter without a value is signed as "lifecycle="
  const auto r = ofi_s3::sigv4_sign(
    "GET", endpoint + "/?lifecycle", {}, ofi_s3::empty_sha256(), access_key,
    secret_key, "us-east-1", when);
  EXPECT_EQ(endpoint + "/?lifecycle", r.url);
  EXPECT_EQ("fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
            signature(r));
}

TEST(OfiS3SigV4, Query)
{
  // the parameters are sorted for the signature, and sent as given
  const auto r = ofi_s3::sigv4_sign(
    "GET", endpoint + "/?prefix=J&max-keys=2", {}, ofi_s3::empty_sha256(),
    access_key, secret_key, "us-east-1", when);
  EXPECT_EQ(endpoint + "/?prefix=J&max-keys=2", r.url);
  EXPECT_EQ("34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
            signature(r));
}

TEST(OfiS3SigV4, SignsEveryHeaderWithAValue)
{
  // an x-amz-* header that the gateway must find signed, and a header
  // without a value, which libcurl does not send, so it is not signed
  const auto r = ofi_s3::sigv4_sign(
    "GET", "http://localhost:8000/b/k",
    {"x-amz-rdma-token: tok", "X-Amz-Rdma-Checksum-Algorithm:  CRC32C ",
     "Expect:"},
    ofi_s3::empty_sha256(), access_key, secret_key, "us-east-1", when);
  EXPECT_EQ("localhost:8000", header(r, "Host"));
  EXPECT_NE(std::string::npos, header(r, "Authorization").find(
              "SignedHeaders=host;x-amz-content-sha256;x-amz-date;"
              "x-amz-rdma-checksum-algorithm;x-amz-rdma-token,"));
  // the caller's headers go out as they came, with the signature's after
  ASSERT_GE(r.headers.size(), 7u);
  EXPECT_EQ("x-amz-rdma-token: tok", r.headers[0]);
  EXPECT_EQ("Expect:", r.headers[2]);
}
