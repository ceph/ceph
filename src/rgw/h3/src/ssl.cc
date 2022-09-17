// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <openssl/err.h>
#include "ssl.h"

namespace rgw::h3::ssl {

boost::system::error_category& get_error_category()
{
  static struct category : boost::system::error_category {
    const char* name() const noexcept override { return "ssl"; }
    std::string message(int value) const override {
      return ::ERR_reason_error_string(value);
    }
  } instance;
  return instance;
}

error_code get_error()
{
  return error_code(::ERR_get_error(), get_error_category());
}

// ssl factory functions start with a single ref; don't add an extra ref when
// constructing an intrusive_ptr
constexpr bool no_add_ref = false;

boost::intrusive_ptr<BIO> make_BIO(std::span<const char> buffer)
{
  return {::BIO_new_mem_buf(buffer.data(), buffer.size()), no_add_ref};
}

boost::intrusive_ptr<SSL_CTX> make_SSL_CTX(const SSL_METHOD* method)
{
  return {::SSL_CTX_new(method), no_add_ref};
}


void use_certificate(SSL_CTX* ctx, X509* a, error_code& ec)
{
  if (!::SSL_CTX_use_certificate(ctx, a)) {
    ec = get_error();
  } else {
    ec.clear();
  }
}

void use_certificate_chain_pem(SSL_CTX* ctx, std::span<const char> buffer,
                               error_code& ec)
{
  auto bio = make_BIO(buffer);
  if (!bio) {
    ec = get_error();
    return;
  }
  auto x = boost::intrusive_ptr{::PEM_read_bio_X509_AUX(
          bio.get(), nullptr, nullptr, nullptr), no_add_ref};
  if (!x) {
    ec = get_error();
    return;
  }
  use_certificate(ctx, x.get(), ec);
  if (ec) {
    return;
  }

  // read any extra certs until PEM fails to find the next start line
  while (X509* cacert = ::PEM_read_bio_X509(
          bio.get(), nullptr, nullptr, nullptr)) {
    if (!::SSL_CTX_add_extra_chain_cert(ctx, cacert)) {
      ec = get_error();
      return;
    }
  }

  const unsigned long e = ::ERR_get_error();
  if (ERR_GET_LIB(e) != ERR_LIB_PEM ||
      ERR_GET_REASON(e) != PEM_R_NO_START_LINE) {
    ec.assign(e, get_error_category());
  } else {
    ec.clear();
  }
}

void use_certificate_chain_file(SSL_CTX* ctx, const char* path, error_code& ec)
{
  if (!::SSL_CTX_use_certificate_chain_file(ctx, path)) {
    ec = get_error();
  } else {
    ec.clear();
  }
}


void use_private_key(SSL_CTX* ctx, EVP_PKEY* key, error_code& ec)
{
  if (!::SSL_CTX_use_PrivateKey(ctx, key)) {
    ec = get_error();
  } else {
    ec.clear();
  }
}

void use_private_key_pem(SSL_CTX* ctx, std::span<const char> buffer,
                         error_code& ec)
{
  auto bio = make_BIO(buffer);
  if (!bio) {
    ec = get_error();
    return;
  }
  auto key = boost::intrusive_ptr{::PEM_read_bio_PrivateKey(
          bio.get(), nullptr, nullptr, nullptr), no_add_ref};
  if (!key) {
    ec = get_error();
    return;
  }
  use_private_key(ctx, key.get(), ec);
}

void use_private_key_file(SSL_CTX* ctx, const char* path, error_code& ec)
{
  if (!::SSL_CTX_use_PrivateKey_file(ctx, path, SSL_FILETYPE_PEM)) {
    ec = get_error();
  } else {
    ec.clear();
  }
}


struct EVP_PKEY_CTX_deleter {
  void operator()(EVP_PKEY_CTX* p) { ::EVP_PKEY_CTX_free(p); };
};
using EVP_PKEY_CTX_ptr = std::unique_ptr<EVP_PKEY_CTX, EVP_PKEY_CTX_deleter>;


auto generate_rsa_key(int bits, error_code& ec)
    -> boost::intrusive_ptr<EVP_PKEY>
{
  auto ctx = EVP_PKEY_CTX_ptr{::EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr)};
  if (!ctx) {
    ec = get_error();
    return nullptr;
  }
  if (::EVP_PKEY_keygen_init(ctx.get()) != 1) {
    ec = get_error();
    return nullptr;
  }
  if (::EVP_PKEY_CTX_set_rsa_keygen_bits(ctx.get(), bits) != 1) {
    ec = get_error();
    return nullptr;
  }
  EVP_PKEY *pkey = nullptr;
  if (::EVP_PKEY_keygen(ctx.get(), &pkey) != 1) {
    ec = get_error();
    return nullptr;
  }
  return {pkey, no_add_ref};
}

static int add_name_entry(X509_NAME* name, const char* field,
                          std::string_view value)
{
  auto data = reinterpret_cast<const unsigned char*>(value.data());
  return ::X509_NAME_add_entry_by_txt(name, field, MBSTRING_ASC,
                                      data, value.size(), -1, 0);
}

auto self_sign_certificate(EVP_PKEY* key,
                           std::string_view country,
                           std::string_view organization,
                           std::string_view common_name,
                           std::chrono::seconds duration,
                           error_code& ec)
    -> boost::intrusive_ptr<X509>
{
  auto cert = boost::intrusive_ptr{::X509_new(), no_add_ref};
  if (!cert) {
    ec = get_error();
    return nullptr;
  }
  if (::X509_set_version(cert.get(), 2) != 1) {
    ec = get_error();
    return nullptr;
  }
  if (::ASN1_INTEGER_set(::X509_get_serialNumber(cert.get()), 1) != 1) {
    ec = get_error();
    return nullptr;
  }
  if (::X509_set_pubkey(cert.get(), key) != 1) {
    ec = get_error();
    return nullptr;
  }
  if (!::X509_gmtime_adj(::X509_get_notBefore(cert.get()), 0)) {
    ec = get_error();
    return nullptr;
  }
  if (!::X509_gmtime_adj(::X509_get_notAfter(cert.get()), duration.count())) {
    ec = get_error();
    return nullptr;
  }
  auto name = ::X509_get_subject_name(cert.get());
  if (add_name_entry(name, "C", country) != 1) {
    ec = get_error();
    return nullptr;
  }
  if (add_name_entry(name, "O", organization) != 1) {
    ec = get_error();
    return nullptr;
  }
  if (add_name_entry(name, "CN", common_name) != 1) {
    ec = get_error();
    return nullptr;
  }
  if (::X509_set_issuer_name(cert.get(), name) != 1) {
    ec = get_error();
    return nullptr;
  }
  if (::X509_sign(cert.get(), key, ::EVP_sha256()) == 0) {
    ec = get_error();
    return nullptr;
  }
  return cert;
}

} // namespace rgw::h3::ssl
