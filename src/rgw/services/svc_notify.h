// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"

#include "svc_rados.h"

class RGWSI_Zone;
class RGWSI_Finisher;

class RGWWatcher;

class RGWSI_Notify : public RGWServiceInstance
{
  friend class RGWWatcher;
  friend class RGWServices_Def;

public:
  class CB;

private:
  RGWSI_Zone *zone_svc{nullptr};
  RGWSI_RADOS *rados_svc{nullptr};
  RGWSI_Finisher *finisher_svc{nullptr};

  ceph::shared_mutex watchers_lock = ceph::make_shared_mutex("watchers_lock");
  rgw_pool control_pool;

  int num_watchers{0};
  RGWWatcher **watchers{nullptr};
  std::set<int> watchers_set;
  std::vector<RGWSI_RADOS::Obj> notify_objs;

  bool enabled{false};

  double inject_notify_timeout_probability{0};
  unsigned max_notify_retries{0};

  string get_control_oid(int i);
  RGWSI_RADOS::Obj pick_control_obj(const string& key);

  CB *cb{nullptr};

  std::optional<int> finisher_handle;

  bool finalized{false};

  boost::system::error_code init_watch();
  void finalize_watch();

  void init(RGWSI_Zone *_zone_svc,
            RGWSI_RADOS *_rados_svc,
            RGWSI_Finisher *_finisher_svc) {
    zone_svc = _zone_svc;
    rados_svc = _rados_svc;
    finisher_svc = _finisher_svc;
  }
  boost::system::error_code do_start() override;
  void shutdown() override;

  boost::system::error_code unwatch(RGWSI_RADOS::Obj& obj,
                                    uint64_t watch_handle);
  void add_watcher(int i);
  void remove_watcher(int i);

  int watch_cb(uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               bufferlist& bl);
  void _set_enabled(bool status);
  void set_enabled(bool status);

  boost::system::error_code robust_notify(RGWSI_RADOS::Obj& notify_obj,
                                          bufferlist& bl,
                                          optional_yield y);

public:
  RGWSI_Notify(CephContext *cct, boost::asio::io_context& ioc)
    : RGWServiceInstance(cct, ioc) {}
  ~RGWSI_Notify();

  class CB {
    public:
      virtual ~CB() {}
      virtual int watch_cb(uint64_t notify_id,
                           uint64_t cookie,
                           uint64_t notifier_id,
                           bufferlist& bl) = 0;
      virtual void set_enabled(bool status) = 0;
  };

  boost::system::error_code distribute(const string& key, bufferlist& bl,
                                       optional_yield y);

  void register_watch_cb(CB *cb);
};
