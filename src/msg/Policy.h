// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/ceph_features.h"

namespace ceph::net {

using peer_type_t = int;

/**
 * A Policy describes the rules of a Connection. Is there a limit on how
 * much data this Connection can have locally? When the underlying connection
 * experiences an error, does the Connection disappear? Can this Messenger
 * re-establish the underlying connection?
 */
template<class ThrottleType>
struct Policy {
  /// If true, the Connection is tossed out on errors.
  bool lossy;
  /// If true, the underlying connection can't be re-established from this end.
  bool server;
  /// If true, we will standby when idle
  bool standby;
  /// If true, we will try to detect session resets
  bool resetcheck;

  /// Server: register lossy client connections.
  bool register_lossy_clients = true;
  // The net result of this is that a given client can only have one
  // open connection with the server.  If a new connection is made,
  // the old (registered) one is closed by the messenger during the accept
  // process.
  
  /**
   *  The throttler is used to limit how much data is held by Messages from
   *  the associated Connection(s). When reading in a new Message, the Messenger
   *  will call throttler->throttle() for the size of the new Message.
   */
  ThrottleType* throttler_bytes;
  ThrottleType* throttler_messages;
  
  /// Specify features supported locally by the endpoint.
  uint64_t features_supported;
  /// Specify features any remotes must have to talk to this endpoint.
  uint64_t features_required;
  
  Policy()
    : lossy(false), server(false), standby(false), resetcheck(true),
      throttler_bytes(NULL),
      throttler_messages(NULL),
      features_supported(CEPH_FEATURES_SUPPORTED_DEFAULT),
      features_required(0) {}
private:
  Policy(bool l, bool s, bool st, bool r, bool rlc, uint64_t req)
    : lossy(l), server(s), standby(st), resetcheck(r),
      register_lossy_clients(rlc),
      throttler_bytes(NULL),
      throttler_messages(NULL),
      features_supported(CEPH_FEATURES_SUPPORTED_DEFAULT),
      features_required(req) {}
  
public:
  static Policy stateful_server(uint64_t req) {
    return Policy(false, true, true, true, true, req);
  }
  static Policy stateless_registered_server(uint64_t req) {
    return Policy(true, true, false, false, true, req);
  }
  static Policy stateless_server(uint64_t req) {
    return Policy(true, true, false, false, false, req);
  }
  static Policy lossless_peer(uint64_t req) {
    return Policy(false, false, true, false, true, req);
  }
  static Policy lossless_peer_reuse(uint64_t req) {
    return Policy(false, false, true, true, true, req);
  }
  static Policy lossy_client(uint64_t req) {
    return Policy(true, false, false, false, true, req);
  }
  static Policy lossless_client(uint64_t req) {
    return Policy(false, false, false, true, true, req);
  }
};

template<class ThrottleType>
class PolicySet {
  using policy_t = Policy<ThrottleType> ;
  /// the default Policy we use for Pipes
  policy_t default_policy;
  /// map specifying different Policies for specific peer types
  std::map<int, policy_t> policy_map; // entity_name_t::type -> Policy

public:
  const policy_t& get(peer_type_t peer_type) const {
    if (auto found = policy_map.find(peer_type); found != policy_map.end()) {
      return found->second;
    } else {
      return default_policy;
    }
  }
  policy_t& get(peer_type_t peer_type) {
    if (auto found = policy_map.find(peer_type); found != policy_map.end()) {
      return found->second;
    } else {
      return default_policy;
    }
  }
  void set(peer_type_t peer_type, const policy_t& p) {
    policy_map[peer_type] = p;
  }
  const policy_t& get_default() const {
    return default_policy;
  }
  void set_default(const policy_t& p) {
    default_policy = p;
  }
  void set_throttlers(peer_type_t peer_type,
                      ThrottleType* byte_throttle,
                      ThrottleType* msg_throttle) {
    auto& policy = get(peer_type);
    policy.throttler_bytes = byte_throttle;
    policy.throttler_messages = msg_throttle;
  }
};

}
