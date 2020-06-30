// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <variant>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>

#include "Fwd.h"
#include "msg/async/frames_v2.h"

namespace crimson::net {

enum class custom_bp_t : uint8_t {
  BANNER_WRITE = 0,
  BANNER_READ,
  BANNER_PAYLOAD_READ,
  SOCKET_CONNECTING,
  SOCKET_ACCEPTED
};
inline const char* get_bp_name(custom_bp_t bp) {
  uint8_t index = static_cast<uint8_t>(bp);
  static const char *const bp_names[] = {"BANNER_WRITE",
                                         "BANNER_READ",
                                         "BANNER_PAYLOAD_READ",
                                         "SOCKET_CONNECTING",
                                         "SOCKET_ACCEPTED"};
  assert(index < std::size(bp_names));
  return bp_names[index];
}

enum class bp_type_t {
  READ = 0,
  WRITE
};

enum class bp_action_t {
  CONTINUE = 0,
  FAULT,
  BLOCK,
  STALL
};

inline std::ostream& operator<<(std::ostream& out, const bp_action_t& action) {
  static const char *const action_names[] = {"CONTINUE",
                                             "FAULT",
                                             "BLOCK",
                                             "STALL"};
  assert(static_cast<size_t>(action) < std::size(action_names));
  return out << action_names[static_cast<size_t>(action)];
}

class socket_blocker {
  std::optional<seastar::abort_source> p_blocked;
  std::optional<seastar::abort_source> p_unblocked;

 public:
  seastar::future<> wait_blocked() {
    ceph_assert(!p_blocked);
    if (p_unblocked) {
      return seastar::now();
    } else {
      p_blocked = seastar::abort_source();
      return seastar::sleep_abortable(10s, *p_blocked).then([] {
        throw std::runtime_error(
            "Timeout (10s) in socket_blocker::wait_blocked()");
      }).handle_exception_type([] (const seastar::sleep_aborted& e) {
        // wait done!
      });
    }
  }

  seastar::future<> block() {
    if (p_blocked) {
      p_blocked->request_abort();
      p_blocked = std::nullopt;
    }
    ceph_assert(!p_unblocked);
    p_unblocked = seastar::abort_source();
    return seastar::sleep_abortable(10s, *p_unblocked).then([] {
      ceph_abort("Timeout (10s) in socket_blocker::block()");
    }).handle_exception_type([] (const seastar::sleep_aborted& e) {
      // wait done!
    });
  }

  void unblock() {
    ceph_assert(!p_blocked);
    ceph_assert(p_unblocked);
    p_unblocked->request_abort();
    p_unblocked = std::nullopt;
  }
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
  socket_blocker blocker;
  virtual ~Interceptor() {}
  virtual void register_conn(Connection& conn) = 0;
  virtual void register_conn_ready(Connection& conn) = 0;
  virtual void register_conn_closed(Connection& conn) = 0;
  virtual void register_conn_replaced(Connection& conn) = 0;
  virtual bp_action_t intercept(Connection& conn, Breakpoint bp) = 0;
};

} // namespace crimson::net
