// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include <string>
#include <map>
#include <boost/algorithm/string.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/backport14.h"
#include "rgw_rados.h"

#include "include/types.h"

#include "rgw_common.h"

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
    int r = store->list_mfa(entry, &result);
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

    int r = store->set_mfa(entry, devices, true);
    if (r < 0) {
      return r;
    }

    return STATUS_APPLIED;

#if 0
    RGWUserCompleteInfo uci;

    try {
      decode_json_obj(uci, obj);
    } catch (JSONDecoder::err& e) {
      return -EINVAL;
    }

    map<string, bufferlist> *pattrs = NULL;
    if (uci.has_attrs) {
      pattrs = &uci.attrs;
    }

    rgw_user uid(entry);

    RGWUserInfo old_info;
    real_time orig_mtime;
    int ret = rgw_get_user_info_by_uid(store, uid, old_info, &objv_tracker, &orig_mtime);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    // are we actually going to perform this put, or is it too old?
    if (ret != -ENOENT &&
        !check_versions(objv_tracker.read_version, orig_mtime,
			objv_tracker.write_version, mtime, sync_mode)) {
      return STATUS_NO_APPLY;
    }

    ret = rgw_store_user_info(store, uci.info, &old_info, &objv_tracker, mtime, false, pattrs);
    if (ret < 0) {
      return ret;
    }

    return STATUS_APPLIED;
#endif
  }

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override {
    return store->meta_mgr->remove_entry(this, entry, &objv_tracker);
  }

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_pool& pool, string& oid) override {
    oid = key;
    pool = store->get_zone_params().otp_pool;
  }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int list_keys_init(RGWRados *store, const string& marker, void **phandle) override
  {
    auto info = ceph::make_unique<list_keys_info>();

    info->store = store;

    int ret = store->list_raw_objects_init(store->get_zone_params().otp_pool, marker,
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

    list<string> unfiltered_keys;

    int ret = store->list_raw_objects_next(no_filter, max, info->ctx,
                                           keys, truncated);
    if (ret < 0 && ret != -ENOENT)
      return ret;		        
    if (ret == -ENOENT) {
      if (truncated)
        *truncated = false;
      return -ENOENT;
    }

    return 0;
  }

  void list_keys_complete(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    delete info;
  }

  string get_marker(void *handle) {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    return info->store->list_raw_objs_get_cursor(info->ctx);
  }
};

void rgw_otp_init(RGWRados *store)
{
  otp_meta_handler = new RGWOTPMetadataHandler;
  store->meta_mgr->register_handler(otp_meta_handler);
}
