// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "mon/PeerFeatureGate.h"

#include <fmt/format.h>

#include "include/ceph_features_ext.h"

std::optional<PeerFeatureGate::refusal_t> PeerFeatureGate::check(
  uint64_t required,
  const ceph::feature_set_t& required_ext,
  const std::string& why,
  const peer_t& peer,
  ceph::mono_time now)
{
  using kind_t = peer_t::kind_t;
  refusal_t r;
  if (peer.kind == kind_t::BANNER) {
    r.missing = required & ~peer.features;
    r.missing_ext = required_ext - peer.features_ext;
  } else {
    r.missing_ext = required_ext;
  }
  if (peer.kind == kind_t::MSGR1) {
    r.missing = required & ~peer.features;
  }
  if (peer.kind == kind_t::UNKNOWN && r.missing_ext.empty() && peer.global_id) {
    std::lock_guard l(lock);
    // Kept until it expires: a peer presenting someone else's global_id
    // only gets itself turned away, and cannot clear the entry.
    if (auto p = strikes.find(peer.global_id);
        p != strikes.end() && now < p->second.until) {
      r.missing = p->second.missing & required;
    }
  }
  if (!r.missing && r.missing_ext.empty()) {
    return std::nullopt;
  }
  r.reason = "this cluster requires " +
    ceph_describe_missing_features(r.missing, r.missing_ext) +
    " that this client lacks";
  if (!r.missing_ext.empty()) {
    if (!why.empty()) {
      r.reason += ": " + why;
    }
    if (peer.kind != kind_t::BANNER) {
      r.reason += "; use a vampire or later client with msgr2"
        " (kernel: mount with ms_mode=prefer-crc)";
    }
  }
  return r;
}

void PeerFeatureGate::strike(uint64_t global_id, uint64_t missing,
                             ceph::mono_time now)
{
  if (!global_id || !missing) {
    return;
  }
  auto until = now + strike_window;
  std::lock_guard l(lock);
  while (!strikes_by_time.empty() &&
         (strikes_by_time.front().first <= now ||
          strikes_by_time.size() >= max_strikes)) {
    auto [t, gid] = strikes_by_time.front();
    strikes_by_time.pop_front();
    if (auto p = strikes.find(gid); p != strikes.end() && p->second.until == t) {
      strikes.erase(p);
    }
  }
  strikes[global_id] = {until, missing};
  strikes_by_time.emplace_back(until, global_id);
}

void PeerFeatureGate::count_refusal(const std::string& addr,
                                    const std::string& reason)
{
  std::lock_guard l(lock);
  ++refused;
  last_refused_addr = addr;
  last_reason = reason;
}

std::optional<std::string> PeerFeatureGate::take_log_line(ceph::mono_time now)
{
  std::lock_guard l(lock);
  if (!refused || (last_logged && now - *last_logged < log_interval)) {
    return std::nullopt;
  }
  auto line = fmt::format(
    "turned away {} connection attempt(s) from clients lacking required "
    "features (latest at {}): {}", refused, last_refused_addr, last_reason);
  refused = 0;
  last_logged = now;
  return line;
}
