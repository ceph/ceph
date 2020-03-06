// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <array>
#include <iostream>
#include <string_view>

#include "include/buffer.h"
#include "gtest/gtest.h"
#include "common/ceph_crypto.h"
#include "common/ceph_context.h"
#include "global/global_context.h"

#include "msg/async/crypto_onwire_aesgcm.h"

#define dout_context g_ceph_context

class CryptoEnvironment: public ::testing::Environment {
public:
  void SetUp() override {
    ceph::crypto::init();
  }
};

struct aes_gcm_sample_t {
  static constexpr std::string_view key_and_nonce {
    "mock of crypto material for deriving key and nonce for AES GCM"
  };

  static constexpr std::string_view plaintext {
    "mock of plain text for AES GCM cipher"
  };

  // AES GCM doesn't do padding. The size of ciphertext is actually the same
  // as its corresponding plaintext except the AE (authenticated encryption)
  // tag at the end.
  //
  // These outputs have been collected before the nonce fix.
  static constexpr std::array<unsigned char,
                              std::size(plaintext)> ciphertext {
    0x4d, 0xa2, 0xa6, 0x1b, 0xa5, 0x2e, 0x20, 0x0d,
    0xa3, 0x3e, 0x56, 0x6f, 0x36, 0x8c, 0xf3, 0x43,
    0x1a, 0xe5, 0x81, 0x55, 0xb2, 0x31, 0x8c, 0x79,
    0xe5, 0x16, 0xae, 0xab, 0x80, 0xab, 0xd9, 0xe4,
    0x13, 0x91, 0xad, 0x44, 0x7d
  };

  static constexpr std::array<unsigned char,
                              ceph::crypto::onwire::AESGCM_TAG_LEN> tag {
    0xf4, 0x91, 0x9e, 0x37, 0x0e, 0xdc, 0xa8, 0xb2,
    0xc6, 0xeb, 0xf8, 0x03, 0xe9, 0x62, 0x42, 0xc5
  };
};

template <class T>
static std::unique_ptr<T> create_crypto_handler(CephContext* const cct)
{
  using ceph::crypto::onwire::key_t;
  using ceph::crypto::onwire::nonce_t;

  const auto& connection_secret = aes_gcm_sample_t::key_and_nonce;
  ceph_assert_always(
    connection_secret.length() >= sizeof(key_t) + sizeof(nonce_t));
  const char* secbuf = connection_secret.data();

  key_t key;
  {
    ::memcpy(key.data(), secbuf, sizeof(key));
    secbuf += sizeof(key);
  }

  nonce_t nonce;
  {
    ::memcpy(&nonce, secbuf, sizeof(nonce));
    secbuf += sizeof(nonce);
  }

  return std::make_unique<T>(cct, key, nonce, /* new_nonce_format = */false);
}

template <std::size_t N>
static ceph::bufferlist to_bl(const std::array<unsigned char, N>& arr)
{
  ceph::bufferlist bl;
  bl.push_back(ceph::buffer::copy(reinterpret_cast<const char*>(arr.data()),
                                  arr.size())); 
  return bl;
}

static ceph::bufferlist to_bl(const std::string_view& sv)
{
  ceph::bufferlist bl;
  bl.push_back(ceph::buffer::copy(sv.data(), sv.size())); 
  return bl;
}

static std::pair<ceph::bufferlist, ceph::bufferlist> split2ciphertext_and_tag(
  ceph::bufferlist&& ciphertext_with_tag)
{
  ceph_assert_always(
    ciphertext_with_tag.length() > ceph::crypto::onwire::AESGCM_TAG_LEN);
  const std::size_t ciphertext_size = \
    ciphertext_with_tag.length() - ceph::crypto::onwire::AESGCM_TAG_LEN;

  ceph::bufferlist ciphertext;
  ceph::bufferlist tag;
  ciphertext_with_tag.splice(0, ciphertext_size, &ciphertext);

  // ciphertext has been moved out; the remaning is tag.
  tag = std::move(ciphertext_with_tag);

  return std::make_pair(ciphertext, tag);
}

using AES128GCM_OnWireTxHandler = \
  ceph::crypto::onwire::AES128GCM_OnWireTxHandler;
using AES128GCM_OnWireRxHandler = \
  ceph::crypto::onwire::AES128GCM_OnWireRxHandler;

struct ciphertext_generator_t {
  std::unique_ptr<AES128GCM_OnWireTxHandler> tx;

  ciphertext_generator_t()
    : tx(create_crypto_handler<AES128GCM_OnWireTxHandler>(g_ceph_context)) {
  }

  ceph::bufferlist from_recorded_plaintext() {
    auto plainchunk = to_bl(aes_gcm_sample_t::plaintext);
    auto lengths = { plainchunk.length() };
    tx->reset_tx_handler(std::begin(lengths), std::end(lengths));
    tx->authenticated_encrypt_update(std::move(plainchunk));
    return tx->authenticated_encrypt_final();
  }
};

TEST(AESGCMTxHandler, fits_recording)
{
  ciphertext_generator_t ctg;
  auto ciphertext_with_tag = ctg.from_recorded_plaintext();
  auto plaintext = to_bl(aes_gcm_sample_t::plaintext);

  using ceph::crypto::onwire::AESGCM_TAG_LEN;
  const auto plaintext_size = aes_gcm_sample_t::plaintext.size();
  ASSERT_EQ(plaintext_size + AESGCM_TAG_LEN,
            ciphertext_with_tag.length());
  ASSERT_EQ(0, ::memcmp(ciphertext_with_tag.c_str(),
                        aes_gcm_sample_t::ciphertext.data(),
                        aes_gcm_sample_t::ciphertext.size()));
  ASSERT_EQ(0, ::memcmp(ciphertext_with_tag.c_str() + plaintext_size,
                        aes_gcm_sample_t::tag.data(),
                        aes_gcm_sample_t::tag.size()));
  // let's ensure the input bufferlist is untouched.
  ASSERT_TRUE(plaintext.contents_equal(aes_gcm_sample_t::plaintext.data(),
                                       aes_gcm_sample_t::plaintext.size()));
}

TEST(AESGCMTxHandler, nonce_is_being_updated)
{
  // we want to ensure that two ciphertexts (and their tags!) from same
  // plaintext are truly different across two final'led rounds. This is
  // expected because each reset() should trigger nonce update.
  ciphertext_generator_t ctg;
  auto [ ciphertext1, tag1 ] = \
    split2ciphertext_and_tag(ctg.from_recorded_plaintext());
  auto [ ciphertext2, tag2 ] = \
    split2ciphertext_and_tag(ctg.from_recorded_plaintext());

  ASSERT_FALSE(ciphertext1.contents_equal(ciphertext2));
  ASSERT_FALSE(tag1.contents_equal(tag2));

  // extra assertions. Just to ensure that split2ciphertext_and_tag()
  // hasn't messed up.
  ASSERT_EQ(ciphertext1.length(), aes_gcm_sample_t::ciphertext.size());
  ASSERT_EQ(tag1.length(), aes_gcm_sample_t::tag.size());
  ASSERT_EQ(0, ::memcmp(ciphertext1.c_str(),
                        aes_gcm_sample_t::ciphertext.data(),
                        aes_gcm_sample_t::ciphertext.size()));
  ASSERT_EQ(0, ::memcmp(tag1.c_str(),
                        aes_gcm_sample_t::tag.data(),
                        aes_gcm_sample_t::tag.size()));
}

static ceph::bufferlist authenticated_decrypt_update(
  AES128GCM_OnWireRxHandler& rx,
  ceph::bufferlist&& input_bl)
{
  rx.authenticated_decrypt_update(input_bl);
  return std::move(input_bl);
}

static ceph::bufferlist authenticated_decrypt_update_final(
  AES128GCM_OnWireRxHandler& rx,
  ceph::bufferlist&& input_bl)
{
  rx.authenticated_decrypt_update_final(input_bl);
  return std::move(input_bl);
}

TEST(AESGCMRxHandler, singly_chunked_fits_recording)
{
  // decrypt and authenticate at once – using the authenticated_decrypt_update_final
  auto rx = create_crypto_handler<AES128GCM_OnWireRxHandler>(g_ceph_context);
  rx->reset_rx_handler();

  ceph::bufferlist ciphertext_with_tag;
  {
    // claim_append() needs l-value reference.
    auto ciphertext = to_bl(aes_gcm_sample_t::ciphertext);
    auto tag = to_bl(aes_gcm_sample_t::tag);
    ciphertext_with_tag.claim_append(ciphertext);
    ciphertext_with_tag.claim_append(tag);
  }

  // If tag doesn't match, exception will be thrown.
  auto plaintext = authenticated_decrypt_update_final(
    *rx, std::move(ciphertext_with_tag));
  ASSERT_EQ(plaintext.length(), aes_gcm_sample_t::plaintext.size());
  ASSERT_TRUE(plaintext.contents_equal(aes_gcm_sample_t::plaintext.data(),
                                       aes_gcm_sample_t::plaintext.size()));
}

TEST(AESGCMRxHandler, mismatched_tag)
{
  auto rx = create_crypto_handler<AES128GCM_OnWireRxHandler>(g_ceph_context);
  rx->reset_rx_handler();

  ceph::bufferlist ciphertext_with_badtag;
  {
    // claim_append() needs l-value reference.
    auto ciphertext = to_bl(aes_gcm_sample_t::ciphertext);
    ceph::bufferlist tag;
    tag.append_zero(ceph::crypto::onwire::AESGCM_TAG_LEN);
    ciphertext_with_badtag.claim_append(ciphertext);
    ciphertext_with_badtag.claim_append(tag);
  }

  // If tag doesn't match, exception will be thrown.
  ASSERT_THROW(
    authenticated_decrypt_update_final(*rx, std::move(ciphertext_with_badtag)),
    ceph::crypto::onwire::MsgAuthError);
}

TEST(AESGCMRxHandler, multi_chunked_fits_recording)
{
  // verify whether the ciphertext matches plaintext over the entire
  // space of chunk sizes. by chunk we understood the fragment passed
  // to authenticated_decrypt_update() – the auth tag in this test is
  // provided separately.
  for (std::size_t chunk_size = 1;
       chunk_size <= std::size(aes_gcm_sample_t::ciphertext);
       chunk_size++) {
    auto rx = create_crypto_handler<AES128GCM_OnWireRxHandler>(g_ceph_context);

    rx->reset_rx_handler();

    ceph::bufferlist plaintext;
    ceph::bufferlist ciphertext = to_bl(aes_gcm_sample_t::ciphertext);
    while (ciphertext.length() >= chunk_size) {
      ceph::bufferlist cipherchunk;
      ciphertext.splice(0, chunk_size, &cipherchunk);

      ceph::bufferlist plainchunk = authenticated_decrypt_update(
        *rx, std::move(cipherchunk));
      plaintext.claim_append(plainchunk);
    }

    if (ciphertext.length() > 0) {
      ceph::bufferlist last_plainchunk = authenticated_decrypt_update(
        *rx, std::move(ciphertext));
      plaintext.claim_append(last_plainchunk);
    }

    // if tag doesn't match, exception will be thrown.
    auto final_plainchunk = \
      authenticated_decrypt_update_final(*rx, to_bl(aes_gcm_sample_t::tag));
    ASSERT_EQ(0, final_plainchunk.length());
    ASSERT_TRUE(plaintext.contents_equal(aes_gcm_sample_t::plaintext.data(),
                                         aes_gcm_sample_t::plaintext.size()));
  }
}

TEST(AESGCMRxHandler, reset_is_compatible_with_tx)
{
  ciphertext_generator_t ctg;
  auto rx = create_crypto_handler<AES128GCM_OnWireRxHandler>(g_ceph_context);

  for (std::size_t i = 0; i < 5; i++) {
    rx->reset_rx_handler();

    auto ciphertext_with_tag = ctg.from_recorded_plaintext();

    // If tag doesn't match, exception will be thrown.
    ceph::bufferlist plaintext;
    EXPECT_NO_THROW({
      plaintext = authenticated_decrypt_update_final(
        *rx, std::move(ciphertext_with_tag));
    });
    ASSERT_TRUE(plaintext.contents_equal(aes_gcm_sample_t::plaintext.data(),
                                         aes_gcm_sample_t::plaintext.size()));
  }
}

TEST(AESGCMRxHandler, reset_with_cipertext_and_tag_separated)
{
  ciphertext_generator_t ctg;
  auto rx = create_crypto_handler<AES128GCM_OnWireRxHandler>(g_ceph_context);

  for (std::size_t i = 0; i < 5; i++) {
    rx->reset_rx_handler();

    auto [ ciphertext, tag ] = \
      split2ciphertext_and_tag(ctg.from_recorded_plaintext());
    ceph::bufferlist plaintext;
    EXPECT_NO_THROW({
      plaintext = authenticated_decrypt_update(*rx, std::move(ciphertext));
    });
    ASSERT_TRUE(plaintext.contents_equal(aes_gcm_sample_t::plaintext.data(),
                                         aes_gcm_sample_t::plaintext.size()));

    // If tag doesn't match, exception will be thrown.
    ceph::bufferlist final_plaintext;
    EXPECT_NO_THROW({
      final_plaintext = authenticated_decrypt_update_final(*rx, std::move(tag));
    });
  }
}
