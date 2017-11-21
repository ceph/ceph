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


struct RGWUserCompleteInfo {
  RGWUserInfo info;
  map<string, bufferlist> attrs;
  bool has_attrs;

  RGWUserCompleteInfo()
    : has_attrs(false)
  {}

  void dump(Formatter * const f) const {
    info.dump(f);
    encode_json("attrs", attrs, f);
  }

  void decode_json(JSONObj *obj) {
    decode_json_obj(info, obj);
    has_attrs = JSONDecoder::decode_json("attrs", attrs, obj);
  }
};

class RGWUserMetadataObject : public RGWMetadataObject {
  RGWUserCompleteInfo uci;
public:
  RGWUserMetadataObject(const RGWUserCompleteInfo& _uci, obj_version& v, real_time m)
      : uci(_uci) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    uci.dump(f);
  }
};

class RGWOTPMetadataHandler : public RGWMetadataHandler {
public:
  string get_type() override { return "otp"; }

  int get(RGWRados *store, string& entry, RGWMetadataObject **obj) override {
#if 0
    RGWUserCompleteInfo uci;
    RGWObjVersionTracker objv_tracker;
    real_time mtime;

    rgw_user uid(entry);

    int ret = rgw_get_user_info_by_uid(store, uid, uci.info, &objv_tracker,
                                       &mtime, NULL, &uci.attrs);
    if (ret < 0) {
      return ret;
    }

    RGWUserMetadataObject *mdo = new RGWUserMetadataObject(uci, objv_tracker.read_version, mtime);
    *obj = mdo;

#endif
    return -ENOTSUP;
  }

  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
          real_time mtime, JSONObj *obj, sync_type_t sync_mode) override {
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
    return -ENOTSUP;
  }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override {
    RGWUserInfo info;

#warning FIXME
#if 0
    rgw_user uid(entry);

    int ret = rgw_get_user_info_by_uid(store, uid, info, &objv_tracker);
    if (ret < 0)
      return ret;

    return rgw_delete_user(store, info, objv_tracker);
#endif
    return -ENOTSUP;
  }

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_pool& pool, string& oid) override {
    oid = key;
    pool = store->get_zone_params().user_uid_pool;
  }

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
