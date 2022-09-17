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

#include <boost/system/system_error.hpp>
#include <quiche.h>

namespace rgw::h3 {

/// Error conditions for quiche_error codes
enum class quic_errc {
  done = QUICHE_ERR_DONE,
  buffer_too_short = QUICHE_ERR_BUFFER_TOO_SHORT,
  unknown_version = QUICHE_ERR_UNKNOWN_VERSION,
  invalid_frame = QUICHE_ERR_INVALID_FRAME,
  invalid_packet = QUICHE_ERR_INVALID_PACKET,
  invalid_connection_state = QUICHE_ERR_INVALID_STATE,
  invalid_stream_state = QUICHE_ERR_INVALID_STREAM_STATE,
  invalid_transport_param = QUICHE_ERR_INVALID_TRANSPORT_PARAM,
  crypto_error = QUICHE_ERR_CRYPTO_FAIL,
  handshake_error = QUICHE_ERR_TLS_FAIL,
  flow_control_violation = QUICHE_ERR_FLOW_CONTROL,
  stream_limit_violation = QUICHE_ERR_STREAM_LIMIT,
  stream_stopped = QUICHE_ERR_STREAM_STOPPED,
  stream_reset = QUICHE_ERR_STREAM_RESET,
  final_size_exceeded = QUICHE_ERR_FINAL_SIZE,
  congestion_error = QUICHE_ERR_CONGESTION_CONTROL,
  id_limit = QUICHE_ERR_ID_LIMIT,
  out_of_identifiers = QUICHE_ERR_OUT_OF_IDENTIFIERS,
  key_update = QUICHE_ERR_KEY_UPDATE,
  crypto_buffer_exceeded = QUICHE_ERR_CRYPTO_BUFFER_EXCEEDED,
};

/// Error category for quiche_error
const boost::system::error_category& quic_category();

inline boost::system::error_code make_error_code(quic_errc e)
{
  return {static_cast<int>(e), quic_category()};
}
inline boost::system::error_condition make_error_condition(quic_errc e)
{
  return {static_cast<int>(e), quic_category()};
}

/// Error conditions for quiche_h3_error codes
enum class h3_errc {
  done = QUICHE_H3_ERR_DONE,
  buffer_too_short = QUICHE_H3_ERR_BUFFER_TOO_SHORT,
  internal_error = QUICHE_H3_ERR_INTERNAL_ERROR,
  excessive_load = QUICHE_H3_ERR_EXCESSIVE_LOAD,
  stream_id_error = QUICHE_H3_ERR_ID_ERROR,
  peer_stream_rejected = QUICHE_H3_ERR_STREAM_CREATION_ERROR,
  critical_stream_closed = QUICHE_H3_ERR_CLOSED_CRITICAL_STREAM,
  missing_settings = QUICHE_H3_ERR_MISSING_SETTINGS,
  unexpected_frame = QUICHE_H3_ERR_FRAME_UNEXPECTED,
  frame_error = QUICHE_H3_ERR_FRAME_ERROR,
  qpack_decompression_error = QUICHE_H3_ERR_QPACK_DECOMPRESSION_FAILED,
  stream_blocked = QUICHE_H3_ERR_STREAM_BLOCKED,
  settings_error = QUICHE_H3_ERR_SETTINGS_ERROR,
  server_rejected = QUICHE_H3_ERR_REQUEST_REJECTED,
  request_cancelled = QUICHE_H3_ERR_REQUEST_CANCELLED,
  request_incomplete = QUICHE_H3_ERR_REQUEST_INCOMPLETE,
  message_error = QUICHE_H3_ERR_MESSAGE_ERROR,
  connect_error = QUICHE_H3_ERR_CONNECT_ERROR,
  version_fallback = QUICHE_H3_ERR_VERSION_FALLBACK,
};

/// Error category for quiche_h3_error
const boost::system::error_category& h3_category();

inline boost::system::error_code make_error_code(h3_errc e)
{
  return {static_cast<int>(e), h3_category()};
}
inline boost::system::error_condition make_error_condition(h3_errc e)
{
  return {static_cast<int>(e), h3_category()};
}

} // namespace rgw::h3

namespace boost::system {

// enable implicit conversions to boost::system::error_condition
template<>
struct is_error_condition_enum<rgw::h3::quic_errc> : public std::true_type {};
template<>
struct is_error_condition_enum<rgw::h3::h3_errc> : public std::true_type {};

} // namespace boost::system
