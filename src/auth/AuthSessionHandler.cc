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
