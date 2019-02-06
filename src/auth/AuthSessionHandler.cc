// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "common/debug.h"
#include "AuthSessionHandler.h"
#include "cephx/CephxSessionHandler.h"
#ifdef HAVE_GSSAPI
#include "krb/KrbSessionHandler.hpp"
#endif
#include "none/AuthNoneSessionHandler.h"
#include "unknown/AuthUnknownSessionHandler.h"

#include "common/ceph_crypto.h"
#define dout_subsys ceph_subsys_auth


AuthSessionHandler *get_auth_session_handler(
  CephContext *cct, int protocol,
  const CryptoKey& key,
  uint64_t features)
{

  // Should add code to only print the SHA1 hash of the key, unless in secure debugging mode

  ldout(cct,10) << "In get_auth_session_handler for protocol " << protocol << dendl;
 
  switch (protocol) {
  case CEPH_AUTH_CEPHX:
    // if there is no session key, there is no session handler.
    if (key.get_type() == CEPH_CRYPTO_NONE) {
      return nullptr;
    }
    return new CephxSessionHandler(cct, key, features);
  case CEPH_AUTH_NONE:
    return new AuthNoneSessionHandler();
  case CEPH_AUTH_UNKNOWN:
    return new AuthUnknownSessionHandler();
#ifdef HAVE_GSSAPI
  case CEPH_AUTH_GSS: 
    return new KrbSessionHandler();
#endif
  default:
    return nullptr;
  }
}


#ifdef USE_OPENSSL
# include <openssl/evp.h>
#endif

// http://www.mindspring.com/~dmcgrew/gcm-nist-6.pdf
// https://www.openssl.org/docs/man1.0.2/crypto/EVP_aes_128_gcm.html#GCM-mode
// https://wiki.openssl.org/index.php/EVP_Authenticated_Encryption_and_Decryption
class AES128GCM_StreamHandler : public AuthStreamHandler {
  CephContext* const cct;
  std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)> ectx;

  static constexpr const std::size_t AES_KEY_LEN{16};
  static constexpr const std::size_t STREAM_AES_IV_LEN{16};
  static constexpr const std::size_t STREAM_GCM_TAG_LEN{16};
  static constexpr const std::size_t AES_BLOCK_LEN{16};

  std::array<char, AES_KEY_LEN> connection_secret;

  // using GCC's "Tetra Integer" mode here
  ceph::uint128_t nonce;
  static_assert(sizeof(nonce) == STREAM_AES_IV_LEN);

public:
  AES128GCM_StreamHandler(CephContext* const cct,
			  const AuthConnectionMeta& auth_meta,
			  const ceph::uint128_t& nonce)
    : cct(cct),
      ectx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free),
      nonce(nonce) {
    ceph_assert_always(auth_meta.connection_secret.length() >= AES_KEY_LEN);
    ceph_assert_always(ectx);

    std::copy_n(std::cbegin(auth_meta.connection_secret), AES_KEY_LEN,
		std::begin(connection_secret));
    //EVP_CIPHER_CTX_reset(ectx.get());
  }

  ~AES128GCM_StreamHandler() override {
    memset(&nonce, 0, sizeof(nonce));
    //connection_secret.zero();
  }

  std::size_t calculate_padded_size(const std::size_t length) {
    // we need to take into account the PKCS#7 padding. There *always* will
    // be at least one byte of padding. This stays even to input aligned to
    // AES_BLOCK_LEN. Otherwise we would face ambiguities during decryption.
    // To exemplify:
    //   length := 10 -> p2align(10, 16) + 16 == 16 (06 bytes for padding),
    //   length := 16 -> p2align(16, 16) + 16 == 32 (16 bytes for padding).
    //
    // Technically padding will be added by OpenSSL but its format and even
    // presence is a part of the public interface, I believe.
    return p2align(length, AES_BLOCK_LEN) + AES_BLOCK_LEN;
  }

  std::size_t calculate_payload_size(const std::size_t length) override {
    // as we're doing *authenticated encryption* additional space is needed
    // to store Auth Tag. OpenSSL defaults this to 96 bits (12 bytes).
    const std::size_t authenticated_and_encrypted_size = \
      calculate_padded_size(length) + STREAM_GCM_TAG_LEN;
    return authenticated_and_encrypted_size;
  }

  void authenticated_encrypt(ceph::bufferlist& payload) override;
  void authenticated_decrypt(char* payload, uint32_t& length) override;
};

void AES128GCM_StreamHandler::authenticated_encrypt(ceph::bufferlist& payload)
{
  if (1 != EVP_EncryptInit_ex(ectx.get(), EVP_aes_128_gcm(),
			      nullptr, nullptr, nullptr)) {
    throw std::runtime_error("EVP_EncryptInit_ex failed");
  }

  if (1 != EVP_CIPHER_CTX_ctrl(ectx.get(), EVP_CTRL_GCM_SET_IVLEN,
			       STREAM_AES_IV_LEN, nullptr) ) {
    throw std::runtime_error("EVP_CIPHER_CTX_ctrl failed");
  }

  if(1 != EVP_EncryptInit_ex(ectx.get(), nullptr, nullptr,
	reinterpret_cast<const unsigned char*>(connection_secret.data()),
	reinterpret_cast<const unsigned char*>(&nonce))) {
    throw std::runtime_error("EVP_EncryptInit_ex failed");
  }

  // TODO: consider in-place transformata
  auto out_tmp = \
    ceph::buffer::ptr_node::create(calculate_payload_size(payload.length()));

  // append necessary padding accordingly to PKCS#7
  {
    const auto padding_size = \
      calculate_padded_size(payload.length()) - payload.length();
    auto filler = payload.append_hole(padding_size);
    ::memset(filler.c_str(), static_cast<char>(padding_size), padding_size);
  }

  int update_len = 0;
  if(1 != EVP_EncryptUpdate(ectx.get(),
	reinterpret_cast<unsigned char*>(out_tmp->c_str()),
	&update_len,
	reinterpret_cast<const unsigned char*>(payload.c_str()),
	payload.length())) {
    throw std::runtime_error("EVP_EncryptUpdate failed");
  }

  int final_len = 0;
  if(1 != EVP_EncryptFinal_ex(ectx.get(),
	reinterpret_cast<unsigned char*>(out_tmp->c_str() + update_len),
	&final_len)) {
    throw std::runtime_error("EVP_EncryptFinal_ex failed");
  }

  if(1 != EVP_CIPHER_CTX_ctrl(ectx.get(),
	EVP_CTRL_GCM_GET_TAG, STREAM_GCM_TAG_LEN,
	out_tmp->c_str() + update_len + final_len)) {
    throw std::runtime_error("EVP_CIPHER_CTX_ctrl failed");
  }

#if 0
  if (1 != EVP_CIPHER_CTX_cleanup(ectx.get())) {
    throw std::runtime_error("EVP_CIPHER_CTX_cleanup failed");
  }
#endif

  ldout(cct, 15) << __func__
		 << " payload.length()=" << payload.length()
		 << " out_tmp->length()=" << out_tmp->length()
		 << " update_len=" << update_len
		 << " final_len=" << final_len
		 << dendl;
  ceph_assert(update_len + final_len + STREAM_GCM_TAG_LEN == out_tmp->length());

  payload.clear();
  payload.push_back(std::move(out_tmp));
  nonce++;
}

void AES128GCM_StreamHandler::authenticated_decrypt(
  char* payload,
  uint32_t& length)
{
  ceph_assert(length > 0);
  ceph_assert(length % AES_BLOCK_LEN == 0);
  if (1 != EVP_DecryptInit_ex(ectx.get(), EVP_aes_128_gcm(),
			      nullptr, nullptr, nullptr)) {
    throw std::runtime_error("EVP_DecryptInit_ex failed");
  }

  if (1 != EVP_CIPHER_CTX_ctrl(ectx.get(), EVP_CTRL_GCM_SET_IVLEN,
			       STREAM_AES_IV_LEN, nullptr) ) {
    throw std::runtime_error("EVP_CIPHER_CTX_ctrl failed");
  }

  if(1 != EVP_DecryptInit_ex(ectx.get(), nullptr, nullptr,
	reinterpret_cast<const unsigned char*>(connection_secret.data()),
	reinterpret_cast<const unsigned char*>(&nonce))) {
    throw std::runtime_error("EVP_DecryptInit_ex failed");
  }

  // TODO: consider in-place transformata
  auto out_tmp = \
    ceph::buffer::ptr_node::create(length - STREAM_GCM_TAG_LEN);

  int update_len = 0;
  if (1 != EVP_DecryptUpdate(ectx.get(),
	reinterpret_cast<unsigned char*>(out_tmp->c_str()),
	&update_len,
	reinterpret_cast<const unsigned char*>(payload),
	length - STREAM_GCM_TAG_LEN)) {
    throw std::runtime_error("EVP_DecryptUpdate failed");
  }

  if (1 != EVP_CIPHER_CTX_ctrl(ectx.get(), EVP_CTRL_GCM_SET_TAG,
	STREAM_GCM_TAG_LEN,
	payload + length - STREAM_GCM_TAG_LEN)) {
    throw std::runtime_error("EVP_CIPHER_CTX_ctrl failed");
  }

  int final_len = 0;
  if (0 >= EVP_DecryptFinal_ex(ectx.get(),
	reinterpret_cast<unsigned char*>(out_tmp->c_str() + update_len),
	&final_len)) {
  ldout(cct, 15) << __func__
		 << " length=" << length
		 << " out_tmp->length()=" << out_tmp->length()
		 << " update_len=" << update_len
		 << " final_len=" << final_len
		 << dendl;
    throw std::runtime_error("EVP_DecryptFinal_ex failed");
  } else {
    ceph_assert_always(update_len + final_len + STREAM_GCM_TAG_LEN == length);
    ceph_assert_always((update_len + final_len) % AES_BLOCK_LEN == 0);

    // BE CAREFUL: we cannot expose any single bit of information about
    // the cause of failure. Otherwise we'll face padding oracle attack.
    // See: https://en.wikipedia.org/wiki/Padding_oracle_attack.
    const auto pad_len = \
      std::min<std::uint8_t>((*out_tmp)[update_len + final_len - 1], AES_BLOCK_LEN);

    // TODO: move to a new interface after dropping AES-CBC-HMAC-SHA256
    length = update_len + final_len - pad_len;
    memcpy(payload, out_tmp->c_str(), length);
    nonce++;
  }
}


AuthStreamHandler::rxtx_t AuthStreamHandler::create_stream_handler_pair(
  CephContext* cct,
  const class AuthConnectionMeta& auth_meta)
{
  if (auth_meta.is_mode_secure()) {
    // CLEANME, CLEANME CLEANME
    ceph_assert_always(
      auth_meta.connection_secret.length() >= 16 + 2*sizeof(ceph::uint128_t));

    ceph::uint128_t rx_nonce;
    ::memcpy(&rx_nonce, auth_meta.connection_secret.c_str() + 16, sizeof(rx_nonce));

    ceph::uint128_t tx_nonce;
    ::memcpy(&tx_nonce, auth_meta.connection_secret.c_str() + 16 + sizeof(rx_nonce),
      sizeof(tx_nonce));
    return {
      std::make_unique<AES128GCM_StreamHandler>(cct, auth_meta, rx_nonce),
      std::make_unique<AES128GCM_StreamHandler>(cct, auth_meta, tx_nonce)
    };
  } else {
    return { nullptr, nullptr };
  }
}
















#if 1
// https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf
class AES128GCM_OnWireTxHandler : public ceph::crypto::onwire::TxHandler {
#if 0
  std::size_t calculate_padded_size(const std::size_t length) {
    // we need to take into account the PKCS#7 padding. There *always* will
    // be at least one byte of padding. This stays even to input aligned to
    // AES_BLOCK_LEN. Otherwise we would face ambiguities during decryption.
    // To exemplify:
    //   length := 10 -> p2align(10, 16) + 16 == 16 (06 bytes for padding),
    //   length := 16 -> p2align(16, 16) + 16 == 32 (16 bytes for padding).
    //
    // Technically padding will be added by OpenSSL but its format and even
    // presence is a part of the public interface, I believe.
    return p2align(length, AES_BLOCK_LEN) + AES_BLOCK_LEN;
  }

  std::size_t calculate_ciphertext_size(const std::size_t size) override {
    // as we're doing *authenticated encryption* additional space is needed
    // to store Auth Tag. OpenSSL defaults this to 96 bits (12 bytes).
    const std::size_t authenticated_and_encrypted_size = \
      calculate_padded_size(size) + AESGCM_TAG_LEN;
    return authenticated_and_encrypted_size;
  }
#endif

  static constexpr const std::size_t AESGCM_KEY_LEN{16};
  static constexpr const std::size_t AESGCM_IV_LEN{12};
  static constexpr const std::size_t AESGCM_TAG_LEN{16};
  static constexpr const std::size_t AESGCM_BLOCK_LEN{16};

  using nonce_t = std::array<unsigned char, AESGCM_IV_LEN>;

  CephContext* const cct;
  std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)> ectx;
  ceph::bufferlist buffer;
  // using GCC's "Tetra Integer" mode here
  ceph::uint128_t nonce;

public:
  AES128GCM_OnWireTxHandler(CephContext* const cct,
			    const AuthConnectionMeta& auth_meta,
			    const ceph::uint128_t& nonce)
    : cct(cct),
      ectx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free),
      nonce(nonce) {
    ceph_assert_always(auth_meta.connection_secret.length() >= AESGCM_KEY_LEN);
    ceph_assert_always(ectx);

    if (1 != EVP_EncryptInit_ex(ectx.get(), EVP_aes_128_gcm(),
			        nullptr, nullptr, nullptr)) {
      throw std::runtime_error("EVP_EncryptInit_ex failed");
    }

    if(1 != EVP_EncryptInit_ex(ectx.get(), nullptr, nullptr,
	reinterpret_cast<const unsigned char*>(auth_meta.connection_secret.c_str()),
	nullptr)) {
	//reinterpret_cast<const unsigned char*>(&nonce))) {
      throw std::runtime_error("EVP_EncryptInit_ex failed");
    }
  }

  ~AES128GCM_OnWireTxHandler() override {
    memset(&nonce, 0, sizeof(nonce));
  }

  std::uint32_t calculate_segment_size(std::uint32_t size) override
  {
    return size;
  }

  void reset_tx_handler(
    std::initializer_list<std::uint32_t> update_size_sequence) override;

  ceph::bufferlist::contiguous_filler reserve(const std::uint32_t size) override {
    return buffer.append_hole(size);
  }

  void authenticated_encrypt_update(ceph::bufferlist&& plaintext) override;
  ceph::bufferlist authenticated_encrypt_final() override;
};

void AES128GCM_OnWireTxHandler::reset_tx_handler(
  std::initializer_list<std::uint32_t> update_size_sequence)
{
  if(1 != EVP_EncryptInit_ex(ectx.get(), nullptr, nullptr, nullptr,
      reinterpret_cast<const unsigned char*>(&nonce))) {
    throw std::runtime_error("EVP_EncryptInit_ex failed");
  }

  buffer.reserve(std::accumulate(std::begin(update_size_sequence),
    std::end(update_size_sequence), AESGCM_TAG_LEN));
  nonce++;
}

void AES128GCM_OnWireTxHandler::authenticated_encrypt_update(
  ceph::bufferlist&& plaintext)
{
#if 0
  // append necessary padding accordingly to PKCS#7
  {
    const auto padding_size = \
      calculate_padded_size(payload.length()) - payload.length();
    auto filler = payload.append_hole(padding_size);
    ::memset(filler.c_str(), static_cast<char>(padding_size), padding_size);
  }
#endif

  int update_len = 0;
  auto filler = buffer.append_hole(plaintext.length());
  if(1 != EVP_EncryptUpdate(ectx.get(),
	reinterpret_cast<unsigned char*>(filler.c_str()),
	&update_len,
	reinterpret_cast<const unsigned char*>(plaintext.c_str()),
	plaintext.length())) {
    throw std::runtime_error("EVP_EncryptUpdate failed");
  }
  ceph_assert_always(update_len == plaintext.length());
}

ceph::bufferlist AES128GCM_OnWireTxHandler::authenticated_encrypt_final()
{
  int final_len = 0;
  auto filler = buffer.append_hole(AESGCM_BLOCK_LEN);
  if(1 != EVP_EncryptFinal_ex(ectx.get(),
	reinterpret_cast<unsigned char*>(filler.c_str()),
	&final_len)) {
    throw std::runtime_error("EVP_EncryptFinal_ex failed");
  }
  ceph_assert_always(final_len == 0);

  static_assert(AESGCM_BLOCK_LEN == AESGCM_TAG_LEN);
  if(1 != EVP_CIPHER_CTX_ctrl(ectx.get(),
	EVP_CTRL_GCM_GET_TAG, AESGCM_TAG_LEN,
	filler.c_str())) {
    throw std::runtime_error("EVP_CIPHER_CTX_ctrl failed");
  }

  ldout(cct, 15) << __func__
		 << " buffer.length()=" << buffer.length()
		 << " final_len=" << final_len
		 << dendl;
  //ceph_assert(update_len + final_len + AESGCM_TAG_LEN == out_tmp->length());
  return std::move(buffer);
}


// RX PART
class AES128GCM_OnWireRxHandler : public ceph::crypto::onwire::RxHandler {
  static constexpr const std::size_t AESGCM_KEY_LEN{16};
  static constexpr const std::size_t AESGCM_IV_LEN{12};
  static constexpr const std::size_t AESGCM_TAG_LEN{16};
  static constexpr const std::size_t AESGCM_BLOCK_LEN{16};

  using nonce_t = std::array<unsigned char, AESGCM_IV_LEN>;

  CephContext* const cct;
  std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)> ectx;
  // using GCC's "Tetra Integer" mode here
  ceph::uint128_t nonce;

public:
  AES128GCM_OnWireRxHandler(CephContext* const cct,
			    const AuthConnectionMeta& auth_meta,
			    const ceph::uint128_t& nonce)
    : cct(cct),
      ectx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free),
      nonce(nonce) {
    ceph_assert_always(auth_meta.connection_secret.length() >= AESGCM_KEY_LEN);
    ceph_assert_always(ectx);

    if (1 != EVP_DecryptInit_ex(ectx.get(), EVP_aes_128_gcm(),
			        nullptr, nullptr, nullptr)) {
      throw std::runtime_error("EVP_DecryptInit_ex failed");
    }

    if(1 != EVP_DecryptInit_ex(ectx.get(), nullptr, nullptr,
	  reinterpret_cast<const unsigned char*>(auth_meta.connection_secret.c_str()),
	  nullptr)) {
      throw std::runtime_error("EVP_DecryptInit_ex failed");
    }
  }

  ~AES128GCM_OnWireRxHandler() override {
    memset(&nonce, 0, sizeof(nonce));
  }

  void reset_rx_handler() override;
  ceph::bufferlist authenticated_decrypt_update(
    ceph::bufferlist&& ciphertext,
    std::uint32_t alignment) override;
  ceph::bufferlist authenticated_decrypt_update_final(
    ceph::bufferlist&& ciphertext,
    std::uint32_t alignment) override;
};

void AES128GCM_OnWireRxHandler::reset_rx_handler()
{
  if(1 != EVP_EncryptInit_ex(ectx.get(), nullptr, nullptr, nullptr,
      reinterpret_cast<const unsigned char*>(&nonce))) {
    throw std::runtime_error("EVP_EncryptInit_ex failed");
  }
  nonce++;
}

ceph::bufferlist AES128GCM_OnWireRxHandler::authenticated_decrypt_update(
  ceph::bufferlist&& ciphertext,
  std::uint32_t alignment)
{
  ceph_assert(ciphertext.length() > 0);
  ceph_assert(ciphertext.length() % AESGCM_BLOCK_LEN == 0);

  // TODO: consider in-place transformata
  //auto plaintext = ceph::buffer::ptr_node::create_aligned(
  auto plaintext = ceph::buffer::ptr_node::create(
    ciphertext.length());
  //  ciphertext.length(), alignment);

  int update_len = 0;
  if (1 != EVP_DecryptUpdate(ectx.get(),
	reinterpret_cast<unsigned char*>(plaintext->c_str()),
	&update_len,
	reinterpret_cast<const unsigned char*>(ciphertext.c_str()),
	ciphertext.length())) {
    throw std::runtime_error("EVP_DecryptUpdate failed");
  }
  ceph_assert_always(ciphertext.length() == update_len);
  ceph::bufferlist outbl;
  outbl.push_back(std::move(plaintext));
  return std::move(outbl);
}


ceph::bufferlist AES128GCM_OnWireRxHandler::authenticated_decrypt_update_final(
  ceph::bufferlist&& ciphertext_and_tag,
  std::uint32_t alignment)
{
#if 0
#endif

  const auto cnt_len = ciphertext_and_tag.length();
  const auto tag_off = cnt_len - AESGCM_TAG_LEN;
  ceph_assert(cnt_len >= AESGCM_TAG_LEN);

  // we need to ensure the tag is stored in continuous  memory. We could
  // optimize bufferlist to check-xor-copy instead.
  std::array<char, AESGCM_TAG_LEN> auth_tag;
  ciphertext_and_tag.copy(tag_off, AESGCM_TAG_LEN, auth_tag.data());

  if (1 != EVP_CIPHER_CTX_ctrl(ectx.get(), EVP_CTRL_GCM_SET_TAG,
	AESGCM_TAG_LEN, auth_tag.data())) {
    throw std::runtime_error("EVP_CIPHER_CTX_ctrl failed");
  }

  // drop the auth tag
  ciphertext_and_tag.splice(0, tag_off);
  auto plainbl = \
    authenticated_decrypt_update(std::move(ciphertext_and_tag), alignment);

  // I expect that for AES-GCM 0 bytes will be appended
  int final_len = 0;
  if (0 >= EVP_DecryptFinal_ex(ectx.get(),
	nullptr,
	&final_len)) {
    ldout(cct, 15) << __func__
		   << " plainbl.length()=" << plainbl.length()
		   << " final_len=" << final_len
		   << dendl;
    throw std::runtime_error("EVP_DecryptFinal_ex failed");
  } else {
    ceph_assert_always(final_len == 0);
    ceph_assert_always(plainbl.length() + final_len + AESGCM_TAG_LEN == cnt_len);
#if 0
    ceph_assert_always((update_len + final_len) % AESGCM_BLOCK_LEN == 0);

    // BE CAREFUL: we cannot expose any single bit of information about
    // the cause of failure. Otherwise we'll face padding oracle attack.
    // See: https://en.wikipedia.org/wiki/Padding_oracle_attack.
    const auto pad_len = \
      std::min<std::uint8_t>((*out_tmp)[update_len + final_len - 1], AESGCM_BLOCK_LEN);

    // TODO: move to a new interface after dropping AES-CBC-HMAC-SHA256
    length = update_len + final_len - pad_len;
    memcpy(payload, out_tmp->c_str(), length);
#endif
  }

  return plainbl;
}

ceph::crypto::onwire::rxtx_t ceph::crypto::onwire::rxtx_t::create_handler_pair(
  CephContext* cct,
  const AuthConnectionMeta& auth_meta)
{
  if (auth_meta.is_mode_secure()) {
    // CLEANME, CLEANME CLEANME
    ceph_assert_always(
      auth_meta.connection_secret.length() >= 16 + 2*sizeof(ceph::uint128_t));

    ceph::uint128_t rx_nonce;
    ::memcpy(&rx_nonce, auth_meta.connection_secret.c_str() + 16, sizeof(rx_nonce));

    ceph::uint128_t tx_nonce;
    ::memcpy(&tx_nonce, auth_meta.connection_secret.c_str() + 16 + sizeof(rx_nonce),
      sizeof(tx_nonce));
    return {
      //std::make_unique<AES128GCM_OnWireRxHandler>(cct, auth_meta, rx_nonce),
      //std::make_unique<AES128GCM_OnWireTxHandler>(cct, auth_meta, tx_nonce)
      std::make_unique<AES128GCM_OnWireRxHandler>(cct, auth_meta, rx_nonce),
      std::make_unique<AES128GCM_OnWireTxHandler>(cct, auth_meta, tx_nonce)
    };
  } else {
    return { nullptr, nullptr };
  }
}

#endif
