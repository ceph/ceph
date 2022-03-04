#include "include/compat.h"
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include <string_view>

#include <boost/container/flat_set.hpp>
#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>

#include "common/ceph_json.h"

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/Throttle.h"

#include "rgw_sal.h"
#include "rgw_zone.h"
#include "rgw_cache.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h" /* for dumping s3policy in debug log */
#include "rgw_aio_throttle.h"
#include "rgw_bucket.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_datalog.h"
#include "rgw_putobj_processor.h"
#include "rgw_motr.h"

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw/cls_rgw_const.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "osd/osd_types.h"

#include "rgw_tools.h"
#include "rgw_coroutine.h"
#include "rgw_compression.h"
#include "rgw_etag_verifier.h"
#include "rgw_worker.h"
#include "rgw_notify.h"

#undef fork // fails to compile RGWPeriod::fork() below

#include "common/Clock.h"
#include <string>
#include <iostream>
#include <vector>
#include <atomic>
#include <list>
#include <map>
#include "include/random.h"

#include "rgw_gc.h"
#include "rgw_lc.h"

#include "rgw_object_expirer_core.h"
#include "rgw_sync.h"
#include "rgw_sync_counters.h"
#include "rgw_sync_trace.h"
#include "rgw_trim_datalog.h"
#include "rgw_trim_mdlog.h"
#include "rgw_data_sync.h"
#include "rgw_realm_watcher.h"
#include "rgw_reshard.h"

#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"
#include "services/svc_quota.h"
#include "services/svc_sync_modules.h"
#include "services/svc_sys_obj.h"
#include "services/svc_sys_obj_cache.h"
#include "services/svc_bucket.h"
#include "services/svc_mdlog.h"

#include "compressor/Compressor.h"

#include "rgw_d3n_datacache.h"

int RGWMotr::initialize(const DoutPrefixProvider *dpp)
{
  int ret;
  ret = init_svc(true, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
    return ret;
  }
  ret = init_ctl(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to init ctls (ret=" << cpp_strerror(-ret) << ")" << dendl;
    return ret;
  }

  return ret;

  //return init_complete(dpp);
}

std::string RGWMotr::get_host_id()
{
  return svc.zone_utils->gen_host_id();
}

std::string RGWMotr::zone_unique_id(uint64_t unique_num)
{
  return svc.zone_utils->unique_id(unique_num);
}

std::string RGWMotr::zone_unique_trans_id(const uint64_t unique_num)
{
  return svc.zone_utils->unique_trans_id(unique_num);
}

int RGWMotr::init_svc(bool raw, const DoutPrefixProvider *dpp)
{
  if (raw) {
    return svc.init_raw(cct, false, null_yield, dpp);
  }

  return svc.init(cct, false, run_sync_thread, null_yield, dpp);
}

int RGWMotr::init_ctl(const DoutPrefixProvider *dpp)
{
  return ctl.init(&svc, dpp);
}

