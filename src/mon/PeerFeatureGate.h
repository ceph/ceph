// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <string>

#include "common/ceph_mutex.h"
#include "common/ceph_time.h"

/// Which clients the mons turn away at authentication for lacking
/// required features (Monitor::rejects_peer()), with what the policy alone
/// cannot hold: the second strikes of older msgr2 clients, and the rate
/// limit of the cluster log line.
class PeerFeatureGate {
public:
  /// a client refused at identification reconnects within a second
  static constexpr auto strike_window = std::chrono::seconds(60);
  /// remembered at most, so that a flood of refused clients cannot grow it
  static constexpr size_t max_strikes = 10000;
  static constexpr auto log_interval = std::chrono::seconds(60);

  /// what a client has told us when it asks to authenticate
  struct peer_t {
    enum class kind_t {
      MSGR1,    ///< features from the connection
      UNKNOWN,  ///< msgr2: features come only after authentication
    } kind = kind_t::UNKNOWN;
    uint64_t features = 0;
    uint64_t global_id = 0;  ///< as claimed, not yet verified
  };

  struct refusal_t {
    uint64_t missing = 0;
    std::string reason;
  };

  /// whether @p peer lacks @p required
  std::optional<refusal_t> check(uint64_t required, const peer_t& peer,
                                 ceph::mono_time now);

  /// Identification refused client @p global_id, whose global_id we know
  /// to be its own, for lacking @p missing: refuse its next authentication.
  void strike(uint64_t global_id, uint64_t missing, ceph::mono_time now);

  /// count a refusal of the client at @p addr
  void count_refusal(const std::string& addr, const std::string& reason);
  /// the cluster log line due at @p now, if any: at most one an interval
  std::optional<std::string> take_log_line(ceph::mono_time now);

private:
  struct strike_t {
    ceph::mono_time until;
    uint64_t missing;
  };

  ceph::mutex lock = ceph::make_mutex("PeerFeatureGate::lock");
  std::map<uint64_t, strike_t> strikes;
  /// the same entries in the order they expire; stale ones are skipped
  std::deque<std::pair<ceph::mono_time, uint64_t>> strikes_by_time;
  uint64_t refused = 0;
  std::string last_refused_addr, last_reason;
  std::optional<ceph::mono_time> last_logged;
};
