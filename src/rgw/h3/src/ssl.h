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

#pragma once

#include <chrono>
#include <memory>
#include <span>
#include <string_view>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <boost/intrusive_ptr.hpp>
#include <h3/types.h>

inline void intrusive_ptr_add_ref(BIO* a) { BIO_up_ref(a); }
inline void intrusive_ptr_release(BIO* a) { BIO_free(a); }

inline void intrusive_ptr_add_ref(EVP_PKEY* key) { EVP_PKEY_up_ref(key); }
inline void intrusive_ptr_release(EVP_PKEY* key) { EVP_PKEY_free(key); }

inline void intrusive_ptr_add_ref(SSL_CTX* ctx) { SSL_CTX_up_ref(ctx); }
inline void intrusive_ptr_release(SSL_CTX* ctx) { SSL_CTX_free(ctx); }

inline void intrusive_ptr_add_ref(X509* a) { X509_up_ref(a); }
inline void intrusive_ptr_release(X509* a) { X509_free(a); }

namespace rgw::h3::ssl {

/// Return an error category for ssl error codes.
boost::system::error_category& get_error_category();

/// Return the first unread ssl error code.
error_code get_error();

boost::intrusive_ptr<SSL_CTX> make_SSL_CTX(const SSL_METHOD* method);


void use_certificate(SSL_CTX* ctx, X509* a, error_code& ec);
void use_certificate_chain_pem(SSL_CTX* ctx, std::span<const char> buffer, error_code& ec);
void use_certificate_chain_file(SSL_CTX* ctx, const char* path, error_code& ec);

void use_private_key(SSL_CTX* ctx, EVP_PKEY* key, error_code& ec);
void use_private_key_pem(SSL_CTX* ctx, std::span<const char> buffer, error_code& ec);
void use_private_key_file(SSL_CTX* ctx, const char* path, error_code& ec);


/// Generate an RSA key with the requested number of bits.
auto generate_rsa_key(int bits, error_code& ec)
    -> boost::intrusive_ptr<EVP_PKEY>;

/// Generate a self-signed X509 certificate using the given private key.
auto self_sign_certificate(EVP_PKEY* key,
                           std::string_view country,
                           std::string_view organization,
                           std::string_view common_name,
                           std::chrono::seconds duration,
                           error_code& ec)
    -> boost::intrusive_ptr<X509>;

} // namespace rgw::h3::ssl
