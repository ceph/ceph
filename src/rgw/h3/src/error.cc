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
#include "error.h"

namespace rgw::h3 {

const boost::system::error_category& quic_category()
{
  struct category : boost::system::error_category {
    const char* name() const noexcept override {
      return "quic";
    }
    std::string message(int ev) const override {
      switch (ev) {
        case QUICHE_ERR_DONE:
          return "There is no more work to do.";
        case QUICHE_ERR_BUFFER_TOO_SHORT:
          return "The provided buffer is too short.";
        case QUICHE_ERR_UNKNOWN_VERSION:
          return "The provided packet cannot be parsed because its version "
              "is unknown.";
        case QUICHE_ERR_INVALID_FRAME:
          return "The provided packet cannot be parsed because it contains "
              "an invalid frame.";
        case QUICHE_ERR_INVALID_PACKET:
          return "The provided packet cannot be parsed.";
        case QUICHE_ERR_INVALID_STATE:
          return "The operation cannot be completed because the connection "
              "is in an invalid state.";
        case QUICHE_ERR_INVALID_STREAM_STATE:
          return "The operation cannot be completed because the stream is "
              "in an invalid state.";
        case QUICHE_ERR_INVALID_TRANSPORT_PARAM:
          return "The peer's transport params cannot be parsed.";
        case QUICHE_ERR_CRYPTO_FAIL:
          return "A cryptographic operation failed.";
        case QUICHE_ERR_TLS_FAIL:
          return "The TLS handshake failed.";
        case QUICHE_ERR_FLOW_CONTROL:
          return "The peer violated the local flow control limits.";
        case QUICHE_ERR_STREAM_LIMIT:
          return "The peer violated the local stream limits.";
        case QUICHE_ERR_STREAM_STOPPED:
          return "The specified stream was stopped by the peer.";
        case QUICHE_ERR_STREAM_RESET:
          return "The specified stream was reset by the peer.";
        case QUICHE_ERR_FINAL_SIZE:
          return "The received data exceeds the stream's final size.";
        case QUICHE_ERR_CONGESTION_CONTROL:
          return "Error in congestion control.";
        case QUICHE_ERR_ID_LIMIT:
          return "Too many identifiers were provided.";
        case QUICHE_ERR_OUT_OF_IDENTIFIERS:
          return "Not enough available identifiers.";
        case QUICHE_ERR_KEY_UPDATE:
          return "Error in key update.";
        case QUICHE_ERR_CRYPTO_BUFFER_EXCEEDED:
          return "The peer sent more data in CRYPTO frames than we can buffer.";
        default:
          return "unknown";
      }
    }
  };
  static category instance;
  return instance;
}

const boost::system::error_category& h3_category()
{
  struct category : boost::system::error_category {
    const char* name() const noexcept override {
      return "h3";
    }
    std::string message(int ev) const override {
      switch (ev) {
        case QUICHE_H3_ERR_DONE:
          return "There is no error or no work to do";
        case QUICHE_H3_ERR_BUFFER_TOO_SHORT:
          return "The provided buffer is too short.";
        case QUICHE_H3_ERR_INTERNAL_ERROR:
          return "Internal error in the HTTP/3 stack.";
        case QUICHE_H3_ERR_EXCESSIVE_LOAD:
          return "Endpoint detected that the peer is exhibiting behavior "
              "that causes excessive load.";
        case QUICHE_H3_ERR_ID_ERROR:
          return "Stream ID or Push ID greater that current maximum was used "
              "incorrectly, such as exceeding a limit, reducing a limit, or "
              "being reused.";
        case QUICHE_H3_ERR_STREAM_CREATION_ERROR:
          return "The endpoint detected that its peer created a stream that "
              "it will not accept.";
        case QUICHE_H3_ERR_CLOSED_CRITICAL_STREAM:
          return "A required critical stream was closed.";
        case QUICHE_H3_ERR_MISSING_SETTINGS:
          return "No SETTINGS frame at beginning of control stream.";
        case QUICHE_H3_ERR_FRAME_UNEXPECTED:
          return "A frame was received which is not permitted in the current "
              "state.";
        case QUICHE_H3_ERR_FRAME_ERROR:
          return "Frame violated layout or size rules.";
        case QUICHE_H3_ERR_QPACK_DECOMPRESSION_FAILED:
          return "QPACK Header block decompression failure.";
        case QUICHE_H3_ERR_STREAM_BLOCKED:
          return "The underlying QUIC stream (or connection) doesn't have "
              "enough capacity for the operation to complete. The application "
              "should retry later on.";
        case QUICHE_H3_ERR_SETTINGS_ERROR:
          return "Error in the payload of a SETTINGS frame.";
        case QUICHE_H3_ERR_REQUEST_REJECTED:
          return "Server rejected request.";
        case QUICHE_H3_ERR_REQUEST_CANCELLED:
          return "Request or its response cancelled.";
        case QUICHE_H3_ERR_REQUEST_INCOMPLETE:
          return "Client's request stream terminated without containing a "
              "full-formed request.";
        case QUICHE_H3_ERR_MESSAGE_ERROR:
          return "An HTTP message was malformed and cannot be processed.";
        case QUICHE_H3_ERR_CONNECT_ERROR:
          return "The TCP connection established in response to a CONNECT "
              "request was reset or abnormally closed.";
        case QUICHE_H3_ERR_VERSION_FALLBACK:
          return "The requested operation cannot be served over HTTP/3. Peer "
              "should retry over HTTP/1.1.";
        default:
          return "unknown";
      }
    }
    boost::system::error_condition default_error_condition(int code) const noexcept override {
      if (code < -1000) {
        return {code + 1000, quic_category()};
      }
      return {code, category()};
    }
  };
  static category instance;
  return instance;
}

} // namespace rgw::h3
