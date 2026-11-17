// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <map>
#include <memory>
#include <string>
#include <thread>

#include "common/HeartbeatMap.h"

// Each dispatch thread owns its deadline, including time waiting for
// Monitor::lock. Progress on another thread must not reset that deadline.
class DispatchWatchdog {
  struct State {
    ceph::heartbeat_handle_d* heartbeat = nullptr;
    unsigned depth = 0;
  };

public:
  using Token = std::shared_ptr<State>;

  DispatchWatchdog(ceph::HeartbeatMap& heartbeat_map, std::string name);
  ~DispatchWatchdog();

  // Configuration changes take effect at the next outermost entry on a thread.
  Token start(double timeout);
  void finish(const Token& token);
  // Disarm active workers and permanently prevent new registrations.
  void shutdown();

private:
  friend class DispatchWatchdogTest;
  ceph::HeartbeatMap& heartbeat_map;
  const std::string name;
  ceph::mutex lock = ceph::make_mutex("DispatchWatchdog::lock");
  bool stopping = false;
  // Cache idle workers rather than registering one for every message.
  std::map<std::thread::id, Token> threads;
};
