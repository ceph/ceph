// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/optional.hpp>

#include "common/RefCountedObj.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"
#include "common/admin_socket.h"
#include "common/errno.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"

#include "rgw_common.h"
#include "rgw_zone.h"
#include "rgw_sync.h"
#include "rgw_metadata.h"
#include "rgw_mdlog_types.h"
#include "rgw_rest_conn.h"
#include "rgw_tools.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_http_client.h"
#include "rgw_sync_trace.h"

#include "cls/lock/cls_lock_client.h"

#include "services/svc_zone.h"
#include "services/svc_mdlog.h"
#include "services/svc_meta.h"
#include "services/svc_cls.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "meta sync: ")

using namespace std;

std::ostream&  RGWMetaSyncStatusManager::gen_prefix(std::ostream& out) const
{
  return out << "meta sync: ";
}

unsigned RGWMetaSyncStatusManager::get_subsys() const
{
  return dout_subsys;
}

void RGWRemoteMetaLog::finish()
{
  going_down = true;
  stop();
}
