// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>

#include "common/errno.h"
#include "common/safe_io.h"
#include "librados/librados_asio.h"
#include "common/async/yield_context.h"

#include "include/types.h"
#include "include/stringify.h"

#include "librados/AioCompletionImpl.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_acl_s3.h"
#include "rgw_op.h"
#include "rgw_putobj_processor.h"
#include "rgw_aio_throttle.h"
#include "rgw_compression.h"
#include "rgw_zone.h"
#include "rgw_sal_rados.h"
#include "osd/osd_types.h"

#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

using namespace std;

int rgw_init_ioctx(const DoutPrefixProvider *dpp,
                   librados::Rados *rados, const rgw_pool& pool,
                   librados::IoCtx& ioctx, bool create,
		   bool mostly_omap)
{
  int r = rados->ioctx_create(pool.name.c_str(), ioctx);
  if (r == -ENOENT && create) {
    r = rados->pool_create(pool.name.c_str());
    if (r == -ERANGE) {
      ldpp_dout(dpp, 0)
        << __func__
        << " ERROR: librados::Rados::pool_create returned " << cpp_strerror(-r)
        << " (this can be due to a pool or placement group misconfiguration, e.g."
        << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
        << dendl;
    }
    if (r < 0 && r != -EEXIST) {
      return r;
    }

    r = rados->ioctx_create(pool.name.c_str(), ioctx);
    if (r < 0) {
      return r;
    }

    r = ioctx.application_enable(pg_pool_t::APPLICATION_NAME_RGW, false);
    if (r < 0 && r != -EOPNOTSUPP) {
      return r;
    }

    if (mostly_omap) {
      // set pg_autoscale_bias
      bufferlist inbl;
      float bias = g_conf().get_val<double>("rgw_rados_pool_autoscale_bias");
      int r = rados->mon_command(
	"{\"prefix\": \"osd pool set\", \"pool\": \"" +
	pool.name + "\", \"var\": \"pg_autoscale_bias\", \"val\": \"" +
	stringify(bias) + "\"}",
	inbl, NULL, NULL);
      if (r < 0) {
	ldpp_dout(dpp, 10) << __func__ << " warning: failed to set pg_autoscale_bias on "
		 << pool.name << dendl;
      }
      // set recovery_priority
      int p = g_conf().get_val<uint64_t>("rgw_rados_pool_recovery_priority");
      r = rados->mon_command(
	"{\"prefix\": \"osd pool set\", \"pool\": \"" +
	pool.name + "\", \"var\": \"recovery_priority\": \"" +
	stringify(p) + "\"}",
	inbl, NULL, NULL);
      if (r < 0) {
	ldpp_dout(dpp, 10) << __func__ << " warning: failed to set recovery_priority on "
		 << pool.name << dendl;
      }
    }
  } else if (r < 0) {
    return r;
  }
  if (!pool.ns.empty()) {
    ioctx.set_namespace(pool.ns);
  }
  return 0;
}

