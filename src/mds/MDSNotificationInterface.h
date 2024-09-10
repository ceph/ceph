#pragma once

#include "LogEvent.h"
#include "common/ceph_context.h"
#include "include/buffer.h"
#include <bits/stdc++.h>

class MDSNotificationInterface {
public:
  virtual uint64_t publish_internal(std::shared_ptr<bufferlist> &message) = 0;
  virtual bool need_to_collect_ack() = 0;
  virtual uint64_t poll() = 0;
  virtual ~MDSNotificationInterface() = default;
};

class MDSNotificationManager {
public:
  MDSNotificationManager(const MDSNotificationManager &) = delete;
  MDSNotificationManager &operator=(const MDSNotificationManager &) = delete;
  static void create();
  static bool interface_exist(uint64_t hash_key);
  static bool is_full();
  static void
  add_interface(uint64_t hash_key,
                std::shared_ptr<MDSNotificationInterface> interface);
  static int send(std::shared_ptr<bufferlist> &message);

private:
  MDSNotificationManager();
  void run();
  uint64_t publish(std::shared_ptr<bufferlist> &message);
  uint64_t polling();
  static const size_t MAX_CONNECTIONS_DEFAULT = 256;
  static const size_t MAX_QUEUE_DEFAULT = 8192;
  static const unsigned IDLE_TIME_MS = 50;
  static MDSNotificationManager *manager;
  std::shared_mutex interface_mutex;
  std::mutex queue_mutex;
  std::unordered_map<uint64_t, std::shared_ptr<MDSNotificationInterface>>
      interfaces;
  std::queue<std::shared_ptr<bufferlist>> message_queue;
  std::thread runner;
};