// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <openssl/evp.h>

#include "crypto_onwire.h"

#include "common/debug.h"
#include "include/types.h"

#define dout_subsys ceph_subsys_ms

// http://www.mindspring.com/~dmcgrew/gcm-nist-6.pdf
// https://www.openssl.org/docs/man1.0.2/crypto/EVP_aes_128_gcm.html#GCM-mode
// https://wiki.openssl.org/index.php/EVP_Authenticated_Encryption_and_Decryption
// https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf
class AES128GCM_OnWireTxHandler : public ceph::crypto::onwire::TxHandler {
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

  void authenticated_encrypt_update(const ceph::bufferlist& plaintext) override;
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

  // transplantate the nonce update management from OpenVPN's AES-GCM
  nonce += 1ULL << 32;
}

void AES128GCM_OnWireTxHandler::authenticated_encrypt_update(
  const ceph::bufferlist& plaintext)
{
  auto filler = buffer.append_hole(plaintext.length());

  for (const auto& plainbuf : plaintext.buffers()) {
    int update_len = 0;

    if(1 != EVP_EncryptUpdate(ectx.get(),
	reinterpret_cast<unsigned char*>(filler.c_str()),
	&update_len,
	reinterpret_cast<const unsigned char*>(plainbuf.c_str()),
	plainbuf.length())) {
      throw std::runtime_error("EVP_EncryptUpdate failed");
    }
    ceph_assert_always(update_len >= 0);
    ceph_assert(static_cast<unsigned>(update_len) == plainbuf.length());
    filler.advance(update_len);
  }

  ldout(cct, 15) << __func__
		 << " plaintext.length()=" << plaintext.length()
		 << " buffer.length()=" << buffer.length()
		 << dendl;
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
      nonce(nonce)
  {
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

  std::uint32_t get_extra_size_at_final() override {
    return AESGCM_TAG_LEN;
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
  if(1 != EVP_DecryptInit_ex(ectx.get(), nullptr, nullptr, nullptr,
	reinterpret_cast<const unsigned char*>(&nonce))) {
    throw std::runtime_error("EVP_DecryptInit_ex failed");
  }
  nonce += 1ULL << 32;
}

ceph::bufferlist AES128GCM_OnWireRxHandler::authenticated_decrypt_update(
  ceph::bufferlist&& ciphertext,
  std::uint32_t alignment)
{
  ceph_assert(ciphertext.length() > 0);
  //ceph_assert(ciphertext.length() % AESGCM_BLOCK_LEN == 0);

  // NOTE: we might consider in-place transformations in the future. AFAIK
  // OpenSSL's might sustain that but lack of clear confirmation postpones.
  auto plainnode = ceph::buffer::ptr_node::create(buffer::create_aligned(
    ciphertext.length(), alignment));
  auto* plainbuf = reinterpret_cast<unsigned char*>(plainnode->c_str());

  for (const auto& cipherbuf : ciphertext.buffers()) {
    // XXX: Why int?
    int update_len = 0;

    if (1 != EVP_DecryptUpdate(ectx.get(),
	plainbuf,
	&update_len,
	reinterpret_cast<const unsigned char*>(cipherbuf.c_str()),
	cipherbuf.length())) {
      throw std::runtime_error("EVP_DecryptUpdate failed");
    }
    ceph_assert_always(update_len >= 0);
    ceph_assert(cipherbuf.length() == static_cast<unsigned>(update_len));

    plainbuf += update_len;
  }

  ceph::bufferlist outbl;
  outbl.push_back(std::move(plainnode));
  return std::move(outbl);
}


ceph::bufferlist AES128GCM_OnWireRxHandler::authenticated_decrypt_update_final(
  ceph::bufferlist&& ciphertext_and_tag,
  std::uint32_t alignment)
{
  const auto cnt_len = ciphertext_and_tag.length();
  ceph_assert(cnt_len >= AESGCM_TAG_LEN);

  // decrypt optional data. Caller is obliged to provide only signature but it
  // may supply ciphertext as well. Combining the update + final is reflected
  // combined together.
  ceph::bufferlist plainbl;
  ceph::bufferlist auth_tag;
  {
    const auto tag_off = cnt_len - AESGCM_TAG_LEN;
    ceph::bufferlist ciphertext;
    ciphertext_and_tag.splice(0, tag_off, &ciphertext);

    // the rest is the signature (a.k.a auth tag)
    auth_tag = std::move(ciphertext_and_tag);

    if (ciphertext.length()) {
      plainbl = authenticated_decrypt_update(std::move(ciphertext), alignment);
    }
  }

  // we need to ensure the tag is stored in continuous memory.
  if (1 != EVP_CIPHER_CTX_ctrl(ectx.get(), EVP_CTRL_GCM_SET_TAG,
	AESGCM_TAG_LEN, auth_tag.c_str())) {
    throw std::runtime_error("EVP_CIPHER_CTX_ctrl failed");
  }

  // I expect that 0 bytes will be appended. The call is supposed solely to
  // authenticate the message.
  {
    int final_len = 0;
    if (0 >= EVP_DecryptFinal_ex(ectx.get(), nullptr, &final_len)) {
      ldout(cct, 15) << __func__
		     << " plainbl.length()=" << plainbl.length()
		     << " final_len=" << final_len
		     << dendl;
      throw std::runtime_error("EVP_DecryptFinal_ex failed");
    } else {
      ceph_assert_always(final_len == 0);
      ceph_assert_always(plainbl.length() + final_len + AESGCM_TAG_LEN == cnt_len);
    }
  }

  return plainbl;
}

ceph::crypto::onwire::rxtx_t ceph::crypto::onwire::rxtx_t::create_handler_pair(
  CephContext* cct,
  const AuthConnectionMeta& auth_meta,
  bool crossed)
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
      std::make_unique<AES128GCM_OnWireRxHandler>(
	cct, auth_meta, crossed ? tx_nonce : rx_nonce),
      std::make_unique<AES128GCM_OnWireTxHandler>(
	cct, auth_meta, crossed ? rx_nonce : tx_nonce)
    };
  } else {
    return { nullptr, nullptr };
  }
}
