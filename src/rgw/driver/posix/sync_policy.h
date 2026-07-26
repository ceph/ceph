// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <unistd.h>

namespace rgw::posix {

enum class SyncPolicy { ALWAYS, COMPLETE, RELAXED, NONE };

inline SyncPolicy parse_sync_policy(const std::string& s) {
  if (s == "always") return SyncPolicy::ALWAYS;
  if (s == "relaxed") return SyncPolicy::RELAXED;
  if (s == "none") return SyncPolicy::NONE;
  return SyncPolicy::COMPLETE;
}

class SyncFsThread {
  int root_fd;
  std::chrono::milliseconds interval;
  std::atomic<bool> stop{false};
  std::thread thread;

public:
  SyncFsThread(int _root_fd, uint64_t interval_ms)
    : root_fd(_root_fd),
      interval(interval_ms) {}

  void start() {
    thread = std::thread([this] {
      while (!stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(interval);
        if (!stop.load(std::memory_order_relaxed)) {
          ::syncfs(root_fd);
        }
      }
    });
  }

  void shutdown() {
    stop.store(true, std::memory_order_relaxed);
    if (thread.joinable()) {
      thread.join();
    }
  }

  ~SyncFsThread() { shutdown(); }
};

} // namespace rgw::posix
