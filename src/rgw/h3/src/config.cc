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

#include <quiche.h>

#include <h3/h3.h>
#include "config.h"
#include "message.h"
#include "ssl.h"

namespace rgw::h3 {

void config_deleter::operator()(quiche_config* config)
{
  ::quiche_config_free(config);
}

void h3_config_deleter::operator()(quiche_h3_config* h3)
{
  ::quiche_h3_config_free(h3);
}

struct certificate_visitor {
  SSL_CTX* ctx;
  void operator()(std::monostate) {} // noop
  void operator()(pem_data buffer) {
    error_code ec;
    ssl::use_certificate_chain_pem(ctx, buffer, ec);
    if (ec) {
      throw boost::system::system_error(ec);
    }
  }
  void operator()(pem_path path) {
    error_code ec;
    ssl::use_certificate_chain_file(ctx, path.c_str(), ec);
    if (ec) {
      throw boost::system::system_error(ec);
    }
  }
};

struct private_key_visitor {
  SSL_CTX* ctx;
  void operator()(std::monostate) {} // noop
  void operator()(pem_data buffer) {
    error_code ec;
    ssl::use_private_key_pem(ctx, buffer, ec);
    if (ec) {
      throw boost::system::system_error(ec);
    }
  }
  void operator()(pem_path path) {
    error_code ec;
    ssl::use_private_key_file(ctx, path.c_str(), ec);
    if (ec) {
      throw boost::system::system_error(ec);
    }
  }
};

int alpn_select_cb(SSL* ssl, const unsigned char** out, unsigned char* outlen,
                   const unsigned char* in, unsigned int inlen, void* arg)
{
  auto alpn = static_cast<const char*>(arg);
  int r = ::SSL_select_next_proto(const_cast<unsigned char**>(out), outlen,
                                  const_cast<unsigned char*>(in), inlen,
                                  reinterpret_cast<const unsigned char*>(alpn),
                                  strlen(alpn));
  if (r == OPENSSL_NPN_NEGOTIATED) {
    return SSL_TLSEXT_ERR_OK;
  } else {
    return SSL_TLSEXT_ERR_ALERT_FATAL;
  }
}

auto init_ssl(const Options& o)
    -> boost::intrusive_ptr<SSL_CTX>
{
  auto ctx = ssl::make_SSL_CTX(::TLS_server_method());

  // QUIC requires TLSv1.3, disable everything earlier
  if (!::SSL_CTX_set_min_proto_version(ctx.get(), TLS1_3_VERSION)) {
    throw boost::system::system_error(ssl::get_error());
  }
  if (!::SSL_CTX_set_max_proto_version(ctx.get(), TLS1_3_VERSION)) {
    throw boost::system::system_error(ssl::get_error());
  }

  // accept the h3 protocol only (no earlier draft versions)
  static constexpr std::string_view alpn = "\x02h3";
  ::SSL_CTX_set_alpn_select_cb(ctx.get(), alpn_select_cb,
                               const_cast<char*>(alpn.data()));

  // load the requested certificate and private key
  std::visit(certificate_visitor{ctx.get()}, o.ssl_certificate);
  std::visit(private_key_visitor{ctx.get()}, o.ssl_private_key);

  if (o.ssl_ciphers) {
    // returns 1 if any cipher could be selected
    if (!::SSL_CTX_set_strict_cipher_list(ctx.get(), o.ssl_ciphers)) {
      throw boost::system::system_error(ssl::get_error());
    }
  }

  // SSL_CTX_set_keylog_callback if SSLKEYLOGFILE is in environment?
  // SSL_CTX_set_early_data_enabled for 0RTT? consider security implications

  return ctx;
}

void configure(const Options& o,
               quiche_config* config,
               quiche_h3_config* h3config)
{
  if (o.log_callback) {
    ::quiche_enable_debug_logging(o.log_callback, o.log_arg);
  }

  ::quiche_config_set_max_idle_timeout(config, o.conn_idle_timeout.count());
  ::quiche_config_set_max_recv_udp_payload_size(config, max_datagram_size);
  ::quiche_config_set_max_send_udp_payload_size(config, max_datagram_size);
  ::quiche_config_set_initial_max_data(config, o.conn_max_data);
  ::quiche_config_set_initial_max_stream_data_bidi_local(
      config, o.stream_max_data_bidi_local);
  ::quiche_config_set_initial_max_stream_data_bidi_remote(
      config, o.stream_max_data_bidi_remote);
  ::quiche_config_set_initial_max_stream_data_uni(
      config, o.stream_max_data_uni);
  ::quiche_config_set_initial_max_streams_bidi(config, o.conn_max_streams_bidi);
  ::quiche_config_set_initial_max_streams_uni(config, o.conn_max_streams_uni);
  ::quiche_config_set_ack_delay_exponent(config, o.ack_delay_exponent);
  ::quiche_config_set_max_ack_delay(config, o.max_ack_delay.count());
  ::quiche_config_set_disable_active_migration(config, true);

  if (o.cc_algorithm == "cubic") {
    ::quiche_config_set_cc_algorithm(config, QUICHE_CC_CUBIC);
  } else if (o.cc_algorithm == "reno") {
    ::quiche_config_set_cc_algorithm(config, QUICHE_CC_RENO);
  } else if (o.cc_algorithm == "bbr") {
    ::quiche_config_set_cc_algorithm(config, QUICHE_CC_BBR);
  } else if (o.cc_algorithm == "bbr2") {
    ::quiche_config_set_cc_algorithm(config, QUICHE_CC_BBR2);
  } else if (!o.cc_algorithm.empty()) {
    throw std::invalid_argument("unknown cc_algorithm");
  }
}

} // namespace rgw::h3

extern "C" {

auto create_h3_config(const rgw::h3::Options& options)
    -> std::unique_ptr<rgw::h3::Config>
{
  auto ssl_context = rgw::h3::init_ssl(options);

  rgw::h3::config_ptr config{::quiche_config_new(QUICHE_PROTOCOL_VERSION)};
  rgw::h3::h3_config_ptr h3config{::quiche_h3_config_new()};
  rgw::h3::configure(options, config.get(), h3config.get());

  return std::make_unique<rgw::h3::ConfigImpl>(
      std::move(ssl_context), std::move(config), std::move(h3config));
}

} // extern "C"
