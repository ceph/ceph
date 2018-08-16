// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include <string>
#include <map>
#include <boost/algorithm/string.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "rgw_rados.h"
#include "rgw_zone.h"

#include "include/types.h"

#include "rgw_common.h"
#include "rgw_tools.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


static RGWMetadataHandler *otp_meta_handler = NULL;


class RGWOTPMetadataObject : public RGWMetadataObject {
  list<rados::cls::otp::otp_info_t> result;
public:
  RGWOTPMetadataObject(list<rados::cls::otp::otp_info_t>& _result, obj_version& v, real_time m) {
    result.swap(_result);
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    encode_json("devices", result, f);
  }
};

class RGWOTPMetadataHandler : public RGWMetadataHandler {
public:
  string get_type() override { return "otp"; }

  int get(RGWRados *store, string& entry, RGWMetadataObject **obj) override {
    RGWObjVersionTracker objv_tracker;
    real_time mtime;

    list<rados::cls::otp::otp_info_t> result;
    int r = store->list_mfa(entry, &result, &objv_tracker, &mtime);
    if (r < 0) {
      return r;
    }
    RGWOTPMetadataObject *mdo = new RGWOTPMetadataObject(result, objv_tracker.read_version, mtime);
    *obj = mdo;
    return 0;
  }

  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
          real_time mtime, JSONObj *obj, sync_type_t sync_mode) override {

    list<rados::cls::otp::otp_info_t> devices;
    try {
      JSONDecoder::decode_json("devices", devices, obj);
    } catch (JSONDecoder::err& e) {
      return -EINVAL;
    }

    int ret = store->meta_mgr->mutate(this, entry, mtime, &objv_tracker,
                                      MDLOG_STATUS_WRITE, sync_mode,
                                      [&] {
         return store->set_mfa(entry, devices, true, &objv_tracker, mtime);
    });
    if (ret < 0) {
      return ret;
    }

    return STATUS_APPLIED;
  }

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override {
    return store->meta_mgr->remove_entry(this, entry, &objv_tracker);
  }

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_pool& pool, string& oid) override {
    oid = key;
    pool = store->svc.zone->get_zone_params().otp_pool;
  }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int list_keys_init(RGWRados *store, const string& marker, void **phandle) override
  {
    auto info = std::make_unique<list_keys_info>();

    info->store = store;

    int ret = store->list_raw_objects_init(store->svc.zone->get_zone_params().otp_pool, marker,
                                           &info->ctx);
    if (ret < 0) {
      return ret;
    }

    *phandle = (void *)info.release();

    return 0;
  }

  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);

    string no_filter;

    keys.clear();

    RGWRados *store = info->store;

    int ret = store->list_raw_objects_next(no_filter, max, info->ctx,
                                           keys, truncated);
    if (ret < 0 && ret != -ENOENT)
      return ret;
    if (ret == -ENOENT) {
      if (truncated)
        *truncated = false;
      return 0;
    }

    return 0;
  }

  void list_keys_complete(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    delete info;
  }

  string get_marker(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    return info->store->list_raw_objs_get_cursor(info->ctx);
  }
};

RGWMetadataHandler *rgw_otp_get_handler()
{
  return otp_meta_handler;
}

void rgw_otp_init(RGWRados *store)
{
  otp_meta_handler = new RGWOTPMetadataHandler;
  store->meta_mgr->register_handler(otp_meta_handler);
}
