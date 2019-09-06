// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <variant>
#include <seastar/core/sharded.hh>

#include "Fwd.h"
#include "msg/async/frames_v2.h"

namespace ceph::net {

enum class custom_bp_t {
  BANNER_WRITE = 0,
  BANNER_READ,
  BANNER_PAYLOAD_READ,
  SOCKET_CONNECTING,
  SOCKET_ACCEPTED
};
inline const char* get_bp_name(custom_bp_t bp) {
  static const char *const bp_names[] = {"BANNER_WRITE",
                                         "BANNER_READ",
                                         "BANNER_PAYLOAD_READ",
                                         "SOCKET_CONNECTING",
                                         "SOCKET_ACCEPTED"};
  assert(static_cast<int>(bp) < std::size(bp_names));
  return bp_names[static_cast<int>(bp)];
}

enum class bp_type_t {
  READ = 0,
  WRITE
};

struct tag_bp_t {
  ceph::msgr::v2::Tag tag;
  bp_type_t type;
  bool operator==(const tag_bp_t& x) const {
    return tag == x.tag && type == x.type;
  }
  bool operator!=(const tag_bp_t& x) const { return !operator==(x); }
  bool operator<(const tag_bp_t& x) const {
    return std::tie(tag, type) < std::tie(x.tag, x.type);
  }
};

struct Breakpoint {
  using var_t = std::variant<custom_bp_t, tag_bp_t>;
  var_t bp;
  Breakpoint(custom_bp_t bp) : bp(bp) { }
  Breakpoint(ceph::msgr::v2::Tag tag, bp_type_t type)
    : bp(tag_bp_t{tag, type}) { }
  bool operator==(const Breakpoint& x) const { return bp == x.bp; }
  bool operator!=(const Breakpoint& x) const { return !operator==(x); }
  bool operator==(const custom_bp_t& x) const { return bp == var_t(x); }
  bool operator!=(const custom_bp_t& x) const { return !operator==(x); }
  bool operator==(const tag_bp_t& x) const { return bp == var_t(x); }
  bool operator!=(const tag_bp_t& x) const { return !operator==(x); }
  bool operator<(const Breakpoint& x) const { return bp < x.bp; }
};

inline std::ostream& operator<<(std::ostream& out, const Breakpoint& bp) {
  if (auto custom_bp = std::get_if<custom_bp_t>(&bp.bp)) {
    return out << get_bp_name(*custom_bp);
  } else {
    auto tag_bp = std::get<tag_bp_t>(bp.bp);
    static const char *const tag_names[] = {"NONE",
                                            "HELLO",
                                            "AUTH_REQUEST",
                                            "AUTH_BAD_METHOD",
                                            "AUTH_REPLY_MORE",
                                            "AUTH_REQUEST_MORE",
                                            "AUTH_DONE",
                                            "AUTH_SIGNATURE",
                                            "CLIENT_IDENT",
                                            "SERVER_IDENT",
                                            "IDENT_MISSING_FEATURES",
                                            "SESSION_RECONNECT",
                                            "SESSION_RESET",
                                            "SESSION_RETRY",
                                            "SESSION_RETRY_GLOBAL",
                                            "SESSION_RECONNECT_OK",
                                            "WAIT",
                                            "MESSAGE",
                                            "KEEPALIVE2",
                                            "KEEPALIVE2_ACK",
                                            "ACK"};
    assert(static_cast<size_t>(tag_bp.tag) < std::size(tag_names));
    return out << tag_names[static_cast<size_t>(tag_bp.tag)]
               << (tag_bp.type == bp_type_t::WRITE ? "_WRITE" : "_READ");
  }
}

struct Interceptor {
  virtual ~Interceptor() {}
  virtual void register_conn(Connection& conn) = 0;
  virtual void register_conn_ready(Connection& conn) = 0;
  virtual void register_conn_closed(Connection& conn) = 0;
  virtual void register_conn_replaced(Connection& conn) = 0;
  virtual bool intercept(Connection& conn, Breakpoint bp) = 0;
};

} // namespace ceph::net
