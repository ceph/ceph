// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <optional>
#include <string>
#include <vector>

#include <boost/container/flat_set.hpp>
#include <boost/system/error_code.hpp>

#include "include/buffer.h"

#include "common/async/yield_context.h"
#include "common/ceph_mutex.h"

#include "rgw/rgw_service.h"

#include "rgw/rgw_rados.h"

namespace bc = boost::container;
namespace bs = boost::system;

namespace cb = ceph::buffer;

class RGWSI_Zone;
class RGWSI_Finisher;

class RGWWatcher;

class RGWSI_Notify : public RGWServiceInstance
{
  friend class RGWWatcher;
  friend struct RGWServices_Def;

public:
  class CB;

private:
  RGWSI_Zone *zone_svc{nullptr};
  RGWRados* rados{nullptr};
  RGWSI_Finisher *finisher_svc{nullptr};

  ceph::shared_mutex watchers_lock = ceph::make_shared_mutex("watchers_lock");
  rgw_pool control_pool;

  int num_watchers{0};
  RGWWatcher **watchers{nullptr};
  bc::flat_set<int> watchers_set;
  std::vector<neo_obj_ref> notify_objs;

  bool enabled{false};

  double inject_notify_timeout_probability{0};
  unsigned max_notify_retries{0};

  std::string get_control_oid(int i);
  neo_obj_ref& pick_control_obj(const string& key);

  CB *cb{nullptr};

  std::optional<int> finisher_handle;

  bool finalized{false};

  bs::error_code init_watch(optional_yield y);
  void finalize_watch();

  void init(RGWSI_Zone *_zone_svc,
            RGWRados *_rados,
            RGWSI_Finisher *_finisher_svc) {
    zone_svc = _zone_svc;
    rados = _rados;
    finisher_svc = _finisher_svc;
  }
  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;
  void shutdown() override;

  bs::error_code unwatch(neo_obj_ref& obj, uint64_t watch_handle);
  void add_watcher(int i);
  void remove_watcher(int i);

  int watch_cb(uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               cb::list& bl);
  void _set_enabled(bool status);
  void set_enabled(bool status);

  bs::error_code robust_notify(neo_obj_ref& notify_obj, cb::list& bl,
			       optional_yield y);

public:
  RGWSI_Notify(CephContext *cct): RGWServiceInstance(cct) {}
  ~RGWSI_Notify();

  class CB {
    public:
      virtual ~CB() {}
      virtual int watch_cb(uint64_t notify_id,
                           uint64_t cookie,
                           uint64_t notifier_id,
                           cb::list& bl) = 0;
      virtual void set_enabled(bool status) = 0;
  };

  bs::error_code distribute(const std::string& key, cb::list& bl, optional_yield y);

  void register_watch_cb(CB *cb);
};
