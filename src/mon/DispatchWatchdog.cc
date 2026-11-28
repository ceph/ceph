// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "DispatchWatchdog.h"

#include <mutex>
#include <sstream>
#include <utility>

#include "include/ceph_assert.h"

DispatchWatchdog::DispatchWatchdog(ceph::HeartbeatMap& heartbeat_map,
                                 std::string name)
  : heartbeat_map(heartbeat_map), name(std::move(name))
{
}

DispatchWatchdog::~DispatchWatchdog()
{
  shutdown();
}

DispatchWatchdog::Token DispatchWatchdog::start(double timeout)
{
  std::lock_guard l(lock);
  if (stopping) {
    return {};
  }

  const auto thread = std::this_thread::get_id();
  auto it = threads.find(thread);
  if (it != threads.end() && it->second->depth > 0) {
    ++it->second->depth;
    return it->second;
  }

  if (timeout <= 0) {
    if (it != threads.end()) {
      heartbeat_map.remove_worker(it->second->heartbeat);
      it->second->heartbeat = nullptr;
      threads.erase(it);
    }
    return {};
  }

  // Thread identifiers may be reused after a dispatch thread exits. Keep the
  // heartbeat's signal target immutable while it is registered: replacement
  // goes through HeartbeatMap's lock, including concurrent health checks.
  if (it != threads.end() &&
      it->second->heartbeat->thread_id != pthread_self()) {
    heartbeat_map.remove_worker(it->second->heartbeat);
    it->second->heartbeat = nullptr;
    threads.erase(it);
    it = threads.end();
  }

  if (it == threads.end()) {
    auto state = std::make_shared<State>();
    std::stringstream ss;
    ss << name << " thread " << thread;
    state->heartbeat = heartbeat_map.add_worker(ss.str(), pthread_self());
    it = threads.emplace(thread, std::move(state)).first;
  }

  auto& state = it->second;
  state->depth = 1;
  auto grace = ceph::make_timespan(timeout);
  heartbeat_map.reset_timeout(state->heartbeat, grace, grace);
  return state;
}

void DispatchWatchdog::finish(const Token& token)
{
  std::lock_guard l(lock);
  // shutdown() invalidates workers, but guards keep their State alive.
  if (!token || !token->heartbeat) {
    return;
  }
  ceph_assert(token->depth > 0);
  if (--token->depth == 0) {
    // Preserve the check for a call that completed after its deadline.
    heartbeat_map.clear_timeout(token->heartbeat);
  }
}

void DispatchWatchdog::shutdown()
{
  std::lock_guard l(lock);
  stopping = true;
  for (auto& [thread, state] : threads) {
    heartbeat_map.remove_worker(state->heartbeat);
    state->heartbeat = nullptr;
    state->depth = 0;
  }
  threads.clear();
}
