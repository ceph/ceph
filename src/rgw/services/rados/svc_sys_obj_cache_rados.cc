
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/admin_socket.h"

#include "svc_sys_obj_cache_rados.h"
#include "svc_zone.h"
#include "svc_notify.h"

#include "rgw/rgw_zone.h"
#include "rgw/rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

class RGWSI_SysObj_Cache_CB : public RGWSI_Notify::CB
{
  RGWSI_SysObj_Cache_RADOS *svc;
public:
  RGWSI_SysObj_Cache_CB(RGWSI_SysObj_Cache_RADOS *_svc) : svc(_svc) {}
  int watch_cb(const DoutPrefixProvider *dpp,
               uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               bufferlist& bl) {
    return svc->watch_cb(dpp, notify_id, cookie, notifier_id, bl);
  }

  void set_enabled(bool status) {
    svc->set_enabled(status);
  }
};

int RGWSI_SysObj_Cache_RADOS::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{
  int r = RGWSI_SysObj_Cache::do_start(y, dpp);
  if (r < 0) {
    return r;
  }

  r = notify_svc->start(y, dpp);
  if (r < 0) {
    return r;
  }

  assert(notify_svc->is_started());

  cb.reset(new RGWSI_SysObj_Cache_CB(this));

  notify_svc->register_watch_cb(cb.get());

  return 0;
}

int RGWSI_SysObj_Cache_RADOS::distribute_cache(const DoutPrefixProvider *dpp, 
                                               const string& normal_name,
                                               const rgw_raw_obj& obj,
                                               ObjectCacheInfo& obj_info, int op,
                                               optional_yield y)
{
  RGWCacheNotifyInfo info;
  info.op = op;
  info.obj_info = obj_info;
  info.obj = obj;
  return notify_svc->distribute(dpp, normal_name, info, y);
}

int RGWSI_SysObj_Cache_RADOS::watch_cb(const DoutPrefixProvider *dpp,
                                       uint64_t notify_id,
                                       uint64_t cookie,
                                       uint64_t notifier_id,
                                       bufferlist& bl)
{
  RGWCacheNotifyInfo info;

  try {
    auto iter = bl.cbegin();
    decode(info, iter);
  } catch (buffer::end_of_buffer& err) {
    ldpp_dout(dpp, 0) << "ERROR: got bad notification" << dendl;
    return -EIO;
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: buffer::error" << dendl;
    return -EIO;
  }

  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(info.obj.pool, info.obj.oid, pool, oid);
  string name = normal_name(pool, oid);
  
  switch (info.op) {
  case UPDATE_OBJ:
    cache.put(dpp, name, info.obj_info, NULL);
    break;
  case INVALIDATE_OBJ:
    cache.invalidate_remove(dpp, name);
    break;
  default:
    ldpp_dout(dpp, 0) << "WARNING: got unknown notification op: " << info.op << dendl;
    return -EINVAL;
  }

  return 0;
}

