// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "acconfig.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librbd/crypto/luks/Header.h"
#include <endian.h>
#include "json_spirit/json_spirit.h"

namespace librbd {
namespace crypto {
namespace luks {

struct TestMockCryptoLuksHeader : public TestMockFixture {
  const size_t OBJECT_SIZE = 4 * 1024 * 1024;
  const char* passphrase_cstr = "password";

  MockImageCtx* mock_image_ctx;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
  }

  void TearDown() override {
    delete mock_image_ctx;
    TestMockFixture::TearDown();
  }
};

TEST_F(TestMockCryptoLuksHeader, FormatLoadRoundTrip) {
  Header format_header(mock_image_ctx->cct);
  ASSERT_EQ(0, format_header.init());
  ASSERT_EQ(0, format_header.format(CRYPT_LUKS2, "aes", nullptr, 64,
                                    "xts-plain64", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, format_header.add_keyslot(
      passphrase_cstr, strlen(passphrase_cstr)));

  uint64_t orig_data_offset = format_header.get_data_offset();
  int orig_sector_size = format_header.get_sector_size();

  ceph::bufferlist header_bl;
  ASSERT_LT(0, format_header.read(&header_bl));

  Header load_header(mock_image_ctx->cct);
  ASSERT_EQ(0, load_header.init());
  ASSERT_EQ(0, load_header.write(header_bl));
  ASSERT_EQ(0, load_header.load(CRYPT_LUKS2));

  ASSERT_EQ(orig_data_offset, load_header.get_data_offset());
  ASSERT_EQ(orig_sector_size, load_header.get_sector_size());
  ASSERT_STREQ("aes", load_header.get_cipher());
  ASSERT_STREQ("xts-plain64", load_header.get_cipher_mode());

  char volume_key[96];
  size_t volume_key_size = sizeof(volume_key);
  ASSERT_EQ(0, load_header.read_volume_key(
      passphrase_cstr, strlen(passphrase_cstr),
      volume_key, &volume_key_size));
  ASSERT_EQ(64u, volume_key_size);
}

TEST_F(TestMockCryptoLuksHeader, FormatLoadRoundTripAES128) {
  Header format_header(mock_image_ctx->cct);
  ASSERT_EQ(0, format_header.init());
  ASSERT_EQ(0, format_header.format(CRYPT_LUKS2, "aes", nullptr, 32,
                                    "xts-plain64", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, format_header.add_keyslot(
      passphrase_cstr, strlen(passphrase_cstr)));

  ceph::bufferlist header_bl;
  ASSERT_LT(0, format_header.read(&header_bl));

  Header load_header(mock_image_ctx->cct);
  ASSERT_EQ(0, load_header.init());
  ASSERT_EQ(0, load_header.write(header_bl));
  ASSERT_EQ(0, load_header.load(CRYPT_LUKS2));

  ASSERT_STREQ("aes", load_header.get_cipher());
  ASSERT_STREQ("xts-plain64", load_header.get_cipher_mode());

  char volume_key[96];
  size_t volume_key_size = sizeof(volume_key);
  ASSERT_EQ(0, load_header.read_volume_key(
      passphrase_cstr, strlen(passphrase_cstr),
      volume_key, &volume_key_size));
  ASSERT_EQ(32u, volume_key_size);
}

TEST_F(TestMockCryptoLuksHeader, FormatLoadRoundTripLUKS1) {
  Header format_header(mock_image_ctx->cct);
  ASSERT_EQ(0, format_header.init());
  ASSERT_EQ(0, format_header.format(CRYPT_LUKS1, "aes", nullptr, 64,
                                    "xts-plain64", 512, OBJECT_SIZE, true));
  ASSERT_EQ(0, format_header.add_keyslot(
      passphrase_cstr, strlen(passphrase_cstr)));

  uint64_t orig_data_offset = format_header.get_data_offset();

  ceph::bufferlist header_bl;
  ASSERT_LT(0, format_header.read(&header_bl));

  Header load_header(mock_image_ctx->cct);
  ASSERT_EQ(0, load_header.init());
  ASSERT_EQ(0, load_header.write(header_bl));
  ASSERT_EQ(0, load_header.load(CRYPT_LUKS1));

  ASSERT_EQ(orig_data_offset, load_header.get_data_offset());
  ASSERT_EQ(512, load_header.get_sector_size());
  ASSERT_STREQ("aes", load_header.get_cipher());
  ASSERT_STREQ("xts-plain64", load_header.get_cipher_mode());

  char volume_key[96];
  size_t volume_key_size = sizeof(volume_key);
  ASSERT_EQ(0, load_header.read_volume_key(
      passphrase_cstr, strlen(passphrase_cstr),
      volume_key, &volume_key_size));
  ASSERT_EQ(64u, volume_key_size);
}

TEST_F(TestMockCryptoLuksHeader, LoadFromSecondaryWhenPrimaryCorrupted) {
  Header format_header(mock_image_ctx->cct);
  ASSERT_EQ(0, format_header.init());
  ASSERT_EQ(0, format_header.format(CRYPT_LUKS2, "aes", nullptr, 64,
                                    "xts-plain64", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, format_header.add_keyslot(
      passphrase_cstr, strlen(passphrase_cstr)));

  uint64_t orig_data_offset = format_header.get_data_offset();

  ceph::bufferlist header_bl;
  ASSERT_LT(0, format_header.read(&header_bl));

  // corrupt the primary header's magic bytes (first 6 bytes);
  // libcryptsetup should fall back to the secondary header copy
  ceph::bufferlist corrupt_bl;
  corrupt_bl.append(header_bl);
  char* data = corrupt_bl.c_str();
  memset(data, 0, 6);

  Header load_header(mock_image_ctx->cct);
  ASSERT_EQ(0, load_header.init());
  ASSERT_EQ(0, load_header.write(corrupt_bl));
  ASSERT_EQ(0, load_header.load(CRYPT_LUKS2));

  ASSERT_EQ(orig_data_offset, load_header.get_data_offset());
  ASSERT_STREQ("aes", load_header.get_cipher());
  ASSERT_STREQ("xts-plain64", load_header.get_cipher_mode());

  char volume_key[96];
  size_t volume_key_size = sizeof(volume_key);
  ASSERT_EQ(0, load_header.read_volume_key(
      passphrase_cstr, strlen(passphrase_cstr),
      volume_key, &volume_key_size));
  ASSERT_EQ(64u, volume_key_size);
}

TEST_F(TestMockCryptoLuksHeader, LoadFailsWhenBothHeadersCorrupted) {
  Header format_header(mock_image_ctx->cct);
  ASSERT_EQ(0, format_header.init());
  ASSERT_EQ(0, format_header.format(CRYPT_LUKS2, "aes", nullptr, 64,
                                    "xts-plain64", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, format_header.add_keyslot(
      passphrase_cstr, strlen(passphrase_cstr)));

  ceph::bufferlist header_bl;
  ASSERT_LT(0, format_header.read(&header_bl));

  // read hdr_size to locate the secondary header
  ceph::bufferlist corrupt_bl;
  corrupt_bl.append(header_bl);
  char* data = corrupt_bl.c_str();
  uint64_t hdr_size;
  memcpy(&hdr_size, data + 8, sizeof(hdr_size));
  hdr_size = be64toh(hdr_size);

  // corrupt both primary and secondary magic bytes
  memset(data, 0, 6);
  memset(data + hdr_size, 0, 6);

  Header load_header(mock_image_ctx->cct);
  ASSERT_EQ(0, load_header.init());
  ASSERT_EQ(0, load_header.write(corrupt_bl));
  // with both headers corrupted, load must fail
  ASSERT_NE(0, load_header.load(CRYPT_LUKS2));
}

#ifdef HAVE_CRYPT_FORMAT_INLINE

// Reference JSON segments from crypt_format_inline() output.
// Generated using patched cryptsetup 2.9.0-git with CRYPTSETUP_FAKE_DIF_TAG_SIZE=256.
// Only the structural fields (segments, config.requirements) are compared;
// volatile fields (keyslots, digests, tokens, UUIDs, salts) are ignored.

// Config 1: AES-256-XTS + hmac(sha256), 96-byte key, 1 keyslot, 4MB alignment
static const char* REF_JSON_AES256_XTS_HMAC_SHA256_1KS = R"json({"keyslots":{"0":{"type":"luks2","key_size":96,"af":{"type":"luks1","stripes":4000,"hash":"sha256"},"area":{"type":"raw","offset":"32768","size":"385024","encryption":"aes-xts-plain64","key_size":64},"kdf":{"type":"pbkdf2","hash":"sha256","iterations":1000,"salt":"yZsRNy5mstVt6t44BjOXBVxYWh5x6ya8b4mh17/GoiE="}}},"tokens":{},"segments":{"0":{"type":"crypt","offset":"16777216","size":"dynamic","iv_tweak":"0","encryption":"aes-xts-random","sector_size":4096,"integrity":{"type":"hmac(sha256)","journal_encryption":"none","journal_integrity":"none"}}},"digests":{"0":{"type":"pbkdf2","keyslots":["0"],"segments":["0"],"hash":"sha256","iterations":1000,"salt":"NeBcMFlA5gjEIE/+zolZ4PQvQ8+pwmKQOnvOwMprEQs=","digest":"ECHXn4n7e354pK948UQcWaWuTWXR1IqBeKyEG241Dac="}},"config":{"json_size":"12288","keyslots_size":"16744448","requirements":{"mandatory":["inline-hw-tags"]}}})json";

// Config 2: AES-128-XTS + hmac(sha256), 64-byte key, 1 keyslot
static const char* REF_JSON_AES128_XTS_HMAC_SHA256_1KS = R"json({"keyslots":{"0":{"type":"luks2","key_size":64,"af":{"type":"luks1","stripes":4000,"hash":"sha256"},"area":{"type":"raw","offset":"32768","size":"258048","encryption":"aes-xts-plain64","key_size":64},"kdf":{"type":"pbkdf2","hash":"sha256","iterations":1000,"salt":"709jMWoz7M2ZLKIdZgkuhEoY2x2Qjp7aeqU4dncm0m4="}}},"tokens":{},"segments":{"0":{"type":"crypt","offset":"16777216","size":"dynamic","iv_tweak":"0","encryption":"aes-xts-random","sector_size":4096,"integrity":{"type":"hmac(sha256)","journal_encryption":"none","journal_integrity":"none"}}},"digests":{"0":{"type":"pbkdf2","keyslots":["0"],"segments":["0"],"hash":"sha256","iterations":1000,"salt":"nPp6TeJdiu/ebx9XcOv5fJiZat2yHarLaV+D1kYZFN8=","digest":"L+auHjZfJ15JoLN2rEOfNZNlk0JKLtRGX0G/d7yKdho="}},"config":{"json_size":"12288","keyslots_size":"16744448","requirements":{"mandatory":["inline-hw-tags"]}}})json";

// Config 3: AES-256-XTS + hmac(sha512), 128-byte key, 1 keyslot
static const char* REF_JSON_AES256_XTS_HMAC_SHA512_1KS = R"json({"keyslots":{"0":{"type":"luks2","key_size":128,"af":{"type":"luks1","stripes":4000,"hash":"sha256"},"area":{"type":"raw","offset":"32768","size":"512000","encryption":"aes-xts-plain64","key_size":64},"kdf":{"type":"pbkdf2","hash":"sha256","iterations":1000,"salt":"p49tiQdNW5d5ibYb8XhKGWNSyRS1n6J4RDF8/apKlaA="}}},"tokens":{},"segments":{"0":{"type":"crypt","offset":"16777216","size":"dynamic","iv_tweak":"0","encryption":"aes-xts-random","sector_size":4096,"integrity":{"type":"hmac(sha512)","journal_encryption":"none","journal_integrity":"none"}}},"digests":{"0":{"type":"pbkdf2","keyslots":["0"],"segments":["0"],"hash":"sha256","iterations":1000,"salt":"0LfrPlhROaQkAPOyieJyN2PI/nRqf9KIwlQpXFzvfN8=","digest":"OlN5SMLQDTh307BYZX8rummcOIQhHbJmzCq0DFkDu3s="}},"config":{"json_size":"12288","keyslots_size":"16744448","requirements":{"mandatory":["inline-hw-tags"]}}})json";

// Config 4: AES-256-XTS + hmac(sha256), 96-byte key, 3 keyslots
static const char* REF_JSON_AES256_XTS_HMAC_SHA256_3KS = R"json({"keyslots":{"0":{"type":"luks2","key_size":96,"af":{"type":"luks1","stripes":4000,"hash":"sha256"},"area":{"type":"raw","offset":"32768","size":"385024","encryption":"aes-xts-plain64","key_size":64},"kdf":{"type":"pbkdf2","hash":"sha256","iterations":1000,"salt":"B73ZfuzuP7rRNtrtlMJwawdx8QbL+/soe0cOqlMCdGA="}},"1":{"type":"luks2","key_size":96,"af":{"type":"luks1","stripes":4000,"hash":"sha256"},"area":{"type":"raw","offset":"417792","size":"385024","encryption":"aes-xts-plain64","key_size":64},"kdf":{"type":"pbkdf2","hash":"sha256","iterations":1000,"salt":"rkVWIPhIhQC6duJFWcGtw/4Lefy1pc2a3tgXKDcENRY="}},"2":{"type":"luks2","key_size":96,"af":{"type":"luks1","stripes":4000,"hash":"sha256"},"area":{"type":"raw","offset":"802816","size":"385024","encryption":"aes-xts-plain64","key_size":64},"kdf":{"type":"pbkdf2","hash":"sha256","iterations":1000,"salt":"Mp0iRmiD7Vyr3P+QOIp8omdUMqysiRxlB7X5kfczlSo="}}},"tokens":{},"segments":{"0":{"type":"crypt","offset":"16777216","size":"dynamic","iv_tweak":"0","encryption":"aes-xts-random","sector_size":4096,"integrity":{"type":"hmac(sha256)","journal_encryption":"none","journal_integrity":"none"}}},"digests":{"0":{"type":"pbkdf2","keyslots":["0","1","2"],"segments":["0"],"hash":"sha256","iterations":1000,"salt":"/+w+78ArdB6tZ6APuGEEFwHyJPOemOqV7TQJFzXhYWw=","digest":"a1JHN31lCa8xqMtP+lUFvz+n5c46EN44c1YSsxvO2os="}},"config":{"json_size":"12288","keyslots_size":"16744448","requirements":{"mandatory":["inline-hw-tags"]}}})json";

// Config 5: AES-256-XTS + hmac(sha256), custom label="test-vol", 8MB alignment
static const char* REF_JSON_AES256_XTS_HMAC_SHA256_LABEL = R"json({"keyslots":{"0":{"type":"luks2","key_size":96,"af":{"type":"luks1","stripes":4000,"hash":"sha256"},"area":{"type":"raw","offset":"32768","size":"385024","encryption":"aes-xts-plain64","key_size":64},"kdf":{"type":"pbkdf2","hash":"sha256","iterations":1000,"salt":"z59SqtsvXHj70Ag1FkKlnIoqwv4OXAAG5nvinn6d3Gw="}}},"tokens":{},"segments":{"0":{"type":"crypt","offset":"16777216","size":"dynamic","iv_tweak":"0","encryption":"aes-xts-random","sector_size":4096,"integrity":{"type":"hmac(sha256)","journal_encryption":"none","journal_integrity":"none"}}},"digests":{"0":{"type":"pbkdf2","keyslots":["0"],"segments":["0"],"hash":"sha256","iterations":1000,"salt":"dF8jso7DaRS0X/5ravOsxMJmFc6lSL+E5/h4ErSzOMg=","digest":"asmRWmrQYds0VgUTT6m38FNzuzs9eP/9/K5r8XtofEQ="}},"config":{"json_size":"12288","keyslots_size":"16744448","requirements":{"mandatory":["inline-hw-tags"]}}})json";

// Config 6: AES-256-GCM + aead, 32-byte key, 1 keyslot
static const char* REF_JSON_AES256_GCM_AEAD_1KS = R"json({"keyslots":{"0":{"type":"luks2","key_size":32,"af":{"type":"luks1","stripes":4000,"hash":"sha256"},"area":{"type":"raw","offset":"32768","size":"131072","encryption":"aes-xts-plain64","key_size":64},"kdf":{"type":"pbkdf2","hash":"sha256","iterations":1000,"salt":"0s0JdPYxbEvsOyomG8oTAG9jxcY46sxrcG6ggIyFzlo="}}},"tokens":{},"segments":{"0":{"type":"crypt","offset":"16777216","size":"dynamic","iv_tweak":"0","encryption":"aes-gcm-random","sector_size":4096,"integrity":{"type":"aead","journal_encryption":"none","journal_integrity":"none"}}},"digests":{"0":{"type":"pbkdf2","keyslots":["0"],"segments":["0"],"hash":"sha256","iterations":1000,"salt":"pt/T5fD1LqOdSJATwf2e6nWeN/GRhOMHF7zJraoDWxg=","digest":"AvQ6vMUPl2lRMGcBPlKGmQCeZimSloppTbP0YnpBNO4="}},"config":{"json_size":"12288","keyslots_size":"16744448","requirements":{"mandatory":["inline-hw-tags"]}}})json";

// Extract the LUKS2 JSON from a header bufferlist (primary header only).
static std::string extract_luks2_json(ceph::bufferlist& bl) {
  ceph_assert(bl.length() > 4096);
  const char* data = bl.c_str();
  uint64_t hdr_size;
  memcpy(&hdr_size, data + 8, sizeof(hdr_size));
  hdr_size = be64toh(hdr_size);
  ceph_assert(hdr_size >= 4096 && hdr_size <= bl.length());
  const char* json_area = data + 4096;
  size_t json_len = strnlen(json_area, hdr_size - 4096);
  return std::string(json_area, json_len);
}

// Compare structural fields of two LUKS2 inline integrity JSONs.
// Checks segments, integrity, requirements — ignores keyslot/digest contents.
static void compare_inline_json_structure(
    const std::string& our_json,
    const std::string& ref_json,
    const char* expected_encryption,
    const char* expected_integrity,
    int expected_keyslot_count) {
  json_spirit::mValue our_root, ref_root;
  ASSERT_TRUE(json_spirit::read(our_json, our_root));
  ASSERT_TRUE(json_spirit::read(ref_json, ref_root));

  auto& our_obj = our_root.get_obj();
  auto& ref_obj = ref_root.get_obj();

  // Compare segments.0 structure
  auto& our_seg0 = our_obj.at("segments").get_obj().at("0").get_obj();
  auto& ref_seg0 = ref_obj.at("segments").get_obj().at("0").get_obj();

  ASSERT_EQ(ref_seg0.at("type").get_str(), our_seg0.at("type").get_str());
  ASSERT_EQ(std::string(expected_encryption), our_seg0.at("encryption").get_str());
  ASSERT_EQ(ref_seg0.at("encryption").get_str(), our_seg0.at("encryption").get_str());
  ASSERT_EQ(ref_seg0.at("sector_size").get_int(), our_seg0.at("sector_size").get_int());
  ASSERT_EQ(std::string("dynamic"), our_seg0.at("size").get_str());

  // Compare iv_tweak if present in reference
  if (ref_seg0.count("iv_tweak")) {
    ASSERT_TRUE(our_seg0.count("iv_tweak") > 0);
    ASSERT_EQ(ref_seg0.at("iv_tweak").get_str(), our_seg0.at("iv_tweak").get_str());
  }

  // Compare integrity section
  auto& our_integrity = our_seg0.at("integrity").get_obj();
  auto& ref_integrity = ref_seg0.at("integrity").get_obj();

  ASSERT_EQ(std::string(expected_integrity), our_integrity.at("type").get_str());
  ASSERT_EQ(ref_integrity.at("type").get_str(), our_integrity.at("type").get_str());
  ASSERT_EQ(ref_integrity.at("journal_encryption").get_str(),
            our_integrity.at("journal_encryption").get_str());
  ASSERT_EQ(ref_integrity.at("journal_integrity").get_str(),
            our_integrity.at("journal_integrity").get_str());

  // Compare config.requirements.mandatory
  auto& our_config = our_obj.at("config").get_obj();
  auto& ref_config = ref_obj.at("config").get_obj();
  auto& our_mandatory = our_config.at("requirements").get_obj().at("mandatory").get_array();
  auto& ref_mandatory = ref_config.at("requirements").get_obj().at("mandatory").get_array();
  ASSERT_EQ(ref_mandatory.size(), our_mandatory.size());
  for (size_t i = 0; i < ref_mandatory.size(); i++) {
    ASSERT_EQ(ref_mandatory[i].get_str(), our_mandatory[i].get_str());
  }

  // Compare keyslot count
  auto& our_keyslots = our_obj.at("keyslots").get_obj();
  ASSERT_EQ(expected_keyslot_count, (int)our_keyslots.size());

  // Both should have digests
  ASSERT_TRUE(our_obj.count("digests") > 0);
  ASSERT_TRUE(ref_obj.count("digests") > 0);
}

TEST_F(TestMockCryptoLuksHeader, InlineIntegrityRoundTrip) {
  // format with cipher_null/ecb then rewrite for inline integrity
  Header format_header(mock_image_ctx->cct);
  ASSERT_EQ(0, format_header.init());
  ASSERT_EQ(0, format_header.format(CRYPT_LUKS2, "cipher_null", nullptr, 96,
                                    "ecb", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, format_header.add_keyslot(
      passphrase_cstr, strlen(passphrase_cstr)));
  ASSERT_EQ(0, format_header.rewrite_segment_for_inline(
      "aes", "xts-random", "hmac(sha256)"));

  uint64_t orig_data_offset = format_header.get_data_offset();

  // read the header and load in a fresh instance
  ceph::bufferlist header_bl;
  ASSERT_LT(0, format_header.read(&header_bl));

  Header load_header(mock_image_ctx->cct);
  ASSERT_EQ(0, load_header.init());
  ASSERT_EQ(0, load_header.write(header_bl));
  ASSERT_EQ(0, load_header.load(CRYPT_LUKS2));

  // verify segment shows the rewritten cipher/mode
  ASSERT_STREQ("aes", load_header.get_cipher());
  ASSERT_STREQ("xts-random", load_header.get_cipher_mode());
  ASSERT_EQ(orig_data_offset, load_header.get_data_offset());
  ASSERT_EQ(4096, load_header.get_sector_size());

  // verify volume key extraction works with 96-byte key
  char volume_key[96];
  size_t volume_key_size = sizeof(volume_key);
  ASSERT_EQ(0, load_header.read_volume_key(
      passphrase_cstr, strlen(passphrase_cstr),
      volume_key, &volume_key_size));
  ASSERT_EQ(96u, volume_key_size);
}

TEST_F(TestMockCryptoLuksHeader, InlineIntegrityLoadAfterReopen) {
  // simulate the full format → persist → load cycle
  Header format_header(mock_image_ctx->cct);
  ASSERT_EQ(0, format_header.init());
  ASSERT_EQ(0, format_header.format(CRYPT_LUKS2, "cipher_null", nullptr, 96,
                                    "ecb", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, format_header.add_keyslot(
      passphrase_cstr, strlen(passphrase_cstr)));
  ASSERT_EQ(0, format_header.rewrite_segment_for_inline(
      "aes", "xts-random", "hmac(sha256)"));

  // read header as if persisting to image
  ceph::bufferlist header_bl;
  ASSERT_LT(0, format_header.read(&header_bl));
  uint64_t data_offset = format_header.get_data_offset();

  // simulate a fresh load (as LoadRequest would)
  Header load_header(mock_image_ctx->cct);
  ASSERT_EQ(0, load_header.init());
  ASSERT_EQ(0, load_header.write(header_bl));
  ASSERT_EQ(0, load_header.load(CRYPT_LUKS2));

  ASSERT_EQ(data_offset, load_header.get_data_offset());
  ASSERT_EQ(4096, load_header.get_sector_size());
  ASSERT_STREQ("aes", load_header.get_cipher());
  ASSERT_STREQ("xts-random", load_header.get_cipher_mode());

  // extract volume key
  char volume_key[96];
  size_t volume_key_size = sizeof(volume_key);
  ASSERT_EQ(0, load_header.read_volume_key(
      passphrase_cstr, strlen(passphrase_cstr),
      volume_key, &volume_key_size));
  ASSERT_EQ(96u, volume_key_size);
}

// Comparison tests: verify rewrite_segment_for_inline() output matches
// crypt_format_inline() reference output structurally.

TEST_F(TestMockCryptoLuksHeader, CompareInlineAES256_HMAC_SHA256) {
  Header h(mock_image_ctx->cct);
  ASSERT_EQ(0, h.init());
  ASSERT_EQ(0, h.format(CRYPT_LUKS2, "cipher_null", nullptr, 96,
                         "ecb", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, h.add_keyslot(passphrase_cstr, strlen(passphrase_cstr)));
  ASSERT_EQ(0, h.rewrite_segment_for_inline(
      "aes", "xts-random", "hmac(sha256)"));

  ceph::bufferlist bl;
  ASSERT_LT(0, h.read(&bl));
  std::string our_json = extract_luks2_json(bl);

  compare_inline_json_structure(our_json, REF_JSON_AES256_XTS_HMAC_SHA256_1KS,
      "aes-xts-random", "hmac(sha256)", 1);
}

TEST_F(TestMockCryptoLuksHeader, CompareInlineAES128_HMAC_SHA256) {
  Header h(mock_image_ctx->cct);
  ASSERT_EQ(0, h.init());
  ASSERT_EQ(0, h.format(CRYPT_LUKS2, "cipher_null", nullptr, 64,
                         "ecb", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, h.add_keyslot(passphrase_cstr, strlen(passphrase_cstr)));
  ASSERT_EQ(0, h.rewrite_segment_for_inline(
      "aes", "xts-random", "hmac(sha256)"));

  ceph::bufferlist bl;
  ASSERT_LT(0, h.read(&bl));
  std::string our_json = extract_luks2_json(bl);

  compare_inline_json_structure(our_json, REF_JSON_AES128_XTS_HMAC_SHA256_1KS,
      "aes-xts-random", "hmac(sha256)", 1);
}

TEST_F(TestMockCryptoLuksHeader, CompareInlineAES256_HMAC_SHA512) {
  Header h(mock_image_ctx->cct);
  ASSERT_EQ(0, h.init());
  ASSERT_EQ(0, h.format(CRYPT_LUKS2, "cipher_null", nullptr, 128,
                         "ecb", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, h.add_keyslot(passphrase_cstr, strlen(passphrase_cstr)));
  ASSERT_EQ(0, h.rewrite_segment_for_inline(
      "aes", "xts-random", "hmac(sha512)"));

  ceph::bufferlist bl;
  ASSERT_LT(0, h.read(&bl));
  std::string our_json = extract_luks2_json(bl);

  compare_inline_json_structure(our_json, REF_JSON_AES256_XTS_HMAC_SHA512_1KS,
      "aes-xts-random", "hmac(sha512)", 1);
}

TEST_F(TestMockCryptoLuksHeader, CompareInlineMultipleKeyslots) {
  Header h(mock_image_ctx->cct);
  ASSERT_EQ(0, h.init());
  ASSERT_EQ(0, h.format(CRYPT_LUKS2, "cipher_null", nullptr, 96,
                         "ecb", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, h.add_keyslot("password", 8));
  ASSERT_EQ(0, h.add_keyslot("pass2", 5));
  ASSERT_EQ(0, h.add_keyslot("pass3", 5));
  ASSERT_EQ(0, h.rewrite_segment_for_inline(
      "aes", "xts-random", "hmac(sha256)"));

  ceph::bufferlist bl;
  ASSERT_LT(0, h.read(&bl));
  std::string our_json = extract_luks2_json(bl);

  compare_inline_json_structure(our_json, REF_JSON_AES256_XTS_HMAC_SHA256_3KS,
      "aes-xts-random", "hmac(sha256)", 3);
}

TEST_F(TestMockCryptoLuksHeader, CompareInlineCustomLabel) {
  Header h(mock_image_ctx->cct);
  ASSERT_EQ(0, h.init());
  ASSERT_EQ(0, h.format(CRYPT_LUKS2, "cipher_null", nullptr, 96,
                         "ecb", 4096, 8 * 1024 * 1024, true));
  ASSERT_EQ(0, h.add_keyslot(passphrase_cstr, strlen(passphrase_cstr)));
  ASSERT_EQ(0, h.rewrite_segment_for_inline(
      "aes", "xts-random", "hmac(sha256)"));

  ceph::bufferlist bl;
  ASSERT_LT(0, h.read(&bl));
  std::string our_json = extract_luks2_json(bl);

  compare_inline_json_structure(our_json, REF_JSON_AES256_XTS_HMAC_SHA256_LABEL,
      "aes-xts-random", "hmac(sha256)", 1);
}

TEST_F(TestMockCryptoLuksHeader, CompareInlineAES256_GCM) {
  Header h(mock_image_ctx->cct);
  ASSERT_EQ(0, h.init());
  ASSERT_EQ(0, h.format(CRYPT_LUKS2, "cipher_null", nullptr, 32,
                         "ecb", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, h.add_keyslot(passphrase_cstr, strlen(passphrase_cstr)));
  ASSERT_EQ(0, h.rewrite_segment_for_inline(
      "aes", "gcm-random", "aead"));

  ceph::bufferlist bl;
  ASSERT_LT(0, h.read(&bl));
  std::string our_json = extract_luks2_json(bl);

  compare_inline_json_structure(our_json, REF_JSON_AES256_GCM_AEAD_1KS,
      "aes-gcm-random", "aead", 1);
}

TEST_F(TestMockCryptoLuksHeader, InlineIntegrityVerifyCryptsetupParsing) {
  // Verify that cryptsetup's own parsing logic correctly interprets our
  // rewritten header — the same code path that feeds parameters to dm-crypt.
  Header format_header(mock_image_ctx->cct);
  ASSERT_EQ(0, format_header.init());
  ASSERT_EQ(0, format_header.format(CRYPT_LUKS2, "cipher_null", nullptr, 96,
                                    "ecb", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, format_header.add_keyslot(
      passphrase_cstr, strlen(passphrase_cstr)));
  ASSERT_EQ(0, format_header.rewrite_segment_for_inline(
      "aes", "xts-random", "hmac(sha256)"));

  ceph::bufferlist header_bl;
  ASSERT_LT(0, format_header.read(&header_bl));

  Header load_header(mock_image_ctx->cct);
  ASSERT_EQ(0, load_header.init());
  ASSERT_EQ(0, load_header.write(header_bl));
  ASSERT_EQ(0, load_header.load(CRYPT_LUKS2));

  struct crypt_params_integrity ip;
  memset(&ip, 0, sizeof(ip));
  ASSERT_EQ(0, load_header.get_integrity_info(&ip));

  ASSERT_STREQ("hmac(sha256)", ip.integrity);
  ASSERT_EQ(48u, ip.tag_size);       // 16 IV + 32 HMAC
  ASSERT_EQ(4096u, ip.sector_size);
}

TEST_F(TestMockCryptoLuksHeader, LoadInlineFromSecondaryWhenPrimaryCorrupted) {
  // format with inline integrity rewrite, then corrupt primary header
  Header format_header(mock_image_ctx->cct);
  ASSERT_EQ(0, format_header.init());
  ASSERT_EQ(0, format_header.format(CRYPT_LUKS2, "cipher_null", nullptr, 96,
                                    "ecb", 4096, OBJECT_SIZE, true));
  ASSERT_EQ(0, format_header.add_keyslot(
      passphrase_cstr, strlen(passphrase_cstr)));
  ASSERT_EQ(0, format_header.rewrite_segment_for_inline(
      "aes", "xts-random", "hmac(sha256)"));

  uint64_t orig_data_offset = format_header.get_data_offset();

  ceph::bufferlist header_bl;
  ASSERT_LT(0, format_header.read(&header_bl));

  // corrupt the primary header's magic bytes
  ceph::bufferlist corrupt_bl;
  corrupt_bl.append(header_bl);
  char* data = corrupt_bl.c_str();
  memset(data, 0, 6);

  Header load_header(mock_image_ctx->cct);
  ASSERT_EQ(0, load_header.init());
  ASSERT_EQ(0, load_header.write(corrupt_bl));
  ASSERT_EQ(0, load_header.load(CRYPT_LUKS2));

  // verify the inline integrity fields survived via secondary header
  ASSERT_EQ(orig_data_offset, load_header.get_data_offset());
  ASSERT_EQ(4096, load_header.get_sector_size());
  ASSERT_STREQ("aes", load_header.get_cipher());
  ASSERT_STREQ("xts-random", load_header.get_cipher_mode());

  char volume_key[96];
  size_t volume_key_size = sizeof(volume_key);
  ASSERT_EQ(0, load_header.read_volume_key(
      passphrase_cstr, strlen(passphrase_cstr),
      volume_key, &volume_key_size));
  ASSERT_EQ(96u, volume_key_size);
}

#endif // HAVE_CRYPT_FORMAT_INLINE

} // namespace luks
} // namespace crypto
} // namespace librbd
