// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <fmt/chrono.h>
#include <string.h>
#include <iostream>
#include <map>
#include <algorithm>
#include <tuple>
#include <functional>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/variant.hpp>

#include "include/scope_guard.h"
#include "include/function2.hpp"
#include "common/Formatter.h"
#include "common/containers.h"
#include "common/split.h"
#include <common/errno.h>
#include "include/random.h"
#include "cls/lock/cls_lock_client.h"
#include "rgw_perf_counters.h"
#include "rgw_common.h"
#include "rgw_bucket.h"
#include "rgw_restore.h"
#include "rgw_zone.h"
#include "rgw_string.h"
#include "rgw_multi.h"
#include "rgw_sal.h"
#include "rgw_lc_tier.h"
#include "rgw_notify.h"
#include "common/dout.h"

#include "fmt/format.h"

#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw_restore


constexpr int32_t hours_in_a_day = 24;
constexpr int32_t secs_in_a_day = hours_in_a_day * 60 * 60;

using namespace std;

//using namespace librados;

void *RGWRestore::RestoreWorker::entry() {
  do {
    ldpp_dout(dpp, -1) << "XXXXXXXXXXXX RGWRestore Reached RestoreWorker " << dendl;
  } while (!restore->going_down());

  return NULL;
}

void RGWRestore::initialize(CephContext *_cct, rgw::sal::Driver* _driver) {
  cct = _cct;
  driver = _driver;
  sal_restore = driver->get_restore();
  max_objs = cct->_conf->rgw_restore_max_objs;
  if (max_objs > HASH_PRIME)
    max_objs = HASH_PRIME;
 
  obj_names = new string[max_objs];

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = restore_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf);
  }
}

void RGWRestore::finalize()
{
  delete[] obj_names;
}

//static inline std::ostream& operator<<(std::ostream &os, rgw::sal::RestoreEntry& ent) {
/*  os << "<ent: bucket=";
  os << ent.bucket;
  os << "; start_time=";
  os << rgw_to_asctime(utime_t(ent.start_time, 0));
  os << "; status=";
  os << LC_STATUS[ent.status];
  os << ">";*/
 // return os;
//}

void RGWRestore::RestoreWorker::stop()
{
  std::lock_guard l{lock};
  cond.notify_all();
}

bool RGWRestore::going_down()
{
  return down_flag;
}

void RGWRestore::start_processor()
{
  worker = std::make_unique<RGWRestore::RestoreWorker>(this, cct, this);
  worker->create("rgw_restore");
}

void RGWRestore::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  worker.reset(nullptr);
}

unsigned RGWRestore::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWRestore::gen_prefix(std::ostream& out) const
{
  return out << "restore: ";
}

int RGWRestore::process(RestoreWorker* worker,
		   bool once = false, //is it needed for CR?
		   bool retry = false) // to retry in_progress request after restart
{
  int ret = 0;
//  int max_secs = cct->_conf->rgw_cr_lock_max_time;

  return 0;
}

/* XXX: check how to use this routine */
time_t RGWRestore::thread_stop_at()
{
  uint64_t interval = (cct->_conf->rgw_restore_debug_interval > 0)
    ? cct->_conf->rgw_restore_debug_interval : secs_in_a_day;

  return time(nullptr) + interval;
}

