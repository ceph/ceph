#pragma once

#include <memory>
#include <thread>
#include <atomic>
#include <chrono>
#include "common/ceph_context.h"
#include "rgw_rados.h"
#include "rgw_user.h"
#include "rgw_bucket.h"

class RGWUsageUpdater {
  CephContext* cct;
  RGWRados* store;
  std::thread updater_thread;
  std::atomic<bool> stop_flag;

  void run(); // background loop
  void update_all_metrics();
  void update_user_metrics(const rgw_user& user);
  void update_bucket_metrics(const rgw_bucket& bucket);

public:
  RGWUsageUpdater(RGWRados* store, CephContext* cct);
  ~RGWUsageUpdater();

  void start();
  void stop();
};
