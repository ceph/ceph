#ifndef CEPH_RGWMOTR_H
#define CEPH_RGWMOTR_H
#include <functional>
#include <boost/container/flat_map.hpp>

#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "include/random.h"
#include "common/RefCountedObj.h"
#include "common/ceph_time.h"
#include "common/Timer.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/version/cls_version_types.h"
#include "cls/log/cls_log_types.h"
#include "cls/timeindex/cls_timeindex_types.h"
#include "cls/otp/cls_otp_types.h"
#include "rgw_log.h"
#include "rgw_metadata.h"
#include "rgw_meta_sync_status.h"
#include "rgw_period_puller.h"
#include "rgw_obj_manifest.h"
#include "rgw_sync_module.h"
#include "rgw_trim_bilog.h"
#include "rgw_service.h"
#include "rgw_sal.h"
#include "rgw_aio.h"
#include "rgw_d3n_cacherequest.h"

#include "services/svc_rados.h"
#include "services/svc_bi_rados.h"
#include "common/Throttle.h"
#include "common/ceph_mutex.h"
#include "rgw_cache.h"


class RGWMotr
{
  bool use_gc_thread;
  bool use_lc_thread;
  bool quota_threads;
  bool run_sync_thread;
  bool run_reshard_thread;
protected:
  CephContext *cct;
    bool use_cache{false};
  bool use_gc{true};
  bool use_datacache{false};
 public:
  RGWMotr():cct(NULL),
               pctl(&ctl)
                {} 
   std::string host_id ="";
  int initialize(CephContext *_cct, const DoutPrefixProvider *dpp) {
    set_context(_cct);
    return initialize(dpp);
  }
    void set_context(CephContext *_cct) {
    cct = _cct;
  }
  int initialize(const DoutPrefixProvider *dpp);
    RGWServices svc;
  RGWCtl ctl;

  RGWCtl *pctl{nullptr};
    int init_svc(bool raw, const DoutPrefixProvider *dpp);
  int init_ctl(const DoutPrefixProvider *dpp);

    RGWMotr& set_use_cache(bool status) {
    use_cache = status;
    return *this;
  }

  RGWMotr& set_use_gc(bool status) {
    use_gc = status;
    return *this;
  }

  RGWMotr& set_use_datacache(bool status) {
    use_datacache = status;
    return *this;
  }

  bool get_use_datacache() {
    return use_datacache;
  }

  RGWMotr& set_run_gc_thread(bool _use_gc_thread) {
    use_gc_thread = _use_gc_thread;
    return *this;
  }

  RGWMotr& set_run_lc_thread(bool _use_lc_thread) {
    use_lc_thread = _use_lc_thread;
    return *this;
  }

  RGWMotr& set_run_quota_threads(bool _run_quota_threads) {
    quota_threads = _run_quota_threads;
    return *this;
  }

  RGWMotr& set_run_sync_thread(bool _run_sync_thread) {
    run_sync_thread = _run_sync_thread;
    return *this;
  }

  RGWMotr& set_run_reshard_thread(bool _run_reshard_thread) {
    run_reshard_thread = _run_reshard_thread;
    return *this;
  }

  uint64_t get_new_req_id() {
    return ceph::util::generate_random_number<uint64_t>();
  }
  std::string zone_unique_id(uint64_t unique_num);
  std::string zone_unique_trans_id(const uint64_t unique_num);
  std::string get_host_id();

};

#endif
