// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>
#include <boost/algorithm/string.hpp>
#include "include/ceph_assert.h"

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"

#include "rgw_string.h"

#include "common/Formatter.h"
#include "rgw_sal_fwd.h"
#include "rgw_user.h"

#define RGW_USER_ANON_ID "anonymous"

class RGWUserCtl;
class RGWBucketCtl;
class RGWUserBuckets;
class RGWMetadataHandler;
class RGWSI_User;

/**
 * A string wrapper that includes encode/decode functions for easily accessing
 * a UID in all forms. In some objects, this may refer to an account id instead
 * of a user.
 */
struct RGWUID
{
  std::string id;
  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(id, bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    using ceph::decode;
    decode(id, bl);
  }
  void dump(Formatter *f) const {
    f->dump_string("user_id", id);
  }
  static std::list<RGWUID> generate_test_instances() {
    std::list<RGWUID> o;
    o.emplace_back();
    o.emplace_back();
    o.back().id = "test:tester";
    return o;
  }
};
WRITE_CLASS_ENCODER(RGWUID)

class RGWUserCtl
{
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_User *user{nullptr};
  } svc;

  struct Ctl {
    RGWBucketCtl *bucket{nullptr};
  } ctl;

public:
  RGWUserCtl(RGWSI_Zone *zone_svc, RGWSI_User *user_svc);

  void init(RGWBucketCtl *bucket_ctl) {
    ctl.bucket = bucket_ctl;
  }

  RGWBucketCtl *get_bucket_ctl() {
    return ctl.bucket;
  }

  struct GetParams {
    RGWObjVersionTracker *objv_tracker{nullptr};
    ceph::real_time *mtime{nullptr};
    rgw_cache_entry_info *cache_info{nullptr};
    std::map<std::string, bufferlist> *attrs{nullptr};

    GetParams() {}

    GetParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
      objv_tracker = _objv_tracker;
      return *this;
    }

    GetParams& set_mtime(ceph::real_time *_mtime) {
      mtime = _mtime;
      return *this;
    }

    GetParams& set_cache_info(rgw_cache_entry_info *_cache_info) {
      cache_info = _cache_info;
      return *this;
    }

    GetParams& set_attrs(std::map<std::string, bufferlist> *_attrs) {
      attrs = _attrs;
      return *this;
    }
  };

  struct PutParams {
    RGWUserInfo *old_info{nullptr};
    RGWObjVersionTracker *objv_tracker{nullptr};
    ceph::real_time mtime;
    bool exclusive{false};
    std::map<std::string, bufferlist> *attrs{nullptr};

    PutParams() {}

    PutParams& set_old_info(RGWUserInfo *_info) {
      old_info = _info;
      return *this;
    }

    PutParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
      objv_tracker = _objv_tracker;
      return *this;
    }

    PutParams& set_mtime(const ceph::real_time& _mtime) {
      mtime = _mtime;
      return *this;
    }

    PutParams& set_exclusive(bool _exclusive) {
      exclusive = _exclusive;
      return *this;
    }

    PutParams& set_attrs(std::map<std::string, bufferlist> *_attrs) {
      attrs = _attrs;
      return *this;
    }
  };

  struct RemoveParams {
    RGWObjVersionTracker *objv_tracker{nullptr};

    RemoveParams() {}

    RemoveParams& set_objv_tracker(RGWObjVersionTracker *_objv_tracker) {
      objv_tracker = _objv_tracker;
      return *this;
    }
  };

  int get_info_by_uid(const DoutPrefixProvider *dpp, 
                      const rgw_user& uid, RGWUserInfo *info,
                      optional_yield y, const GetParams& params = {});
  int get_info_by_email(const DoutPrefixProvider *dpp, 
                        const std::string& email, RGWUserInfo *info,
                        optional_yield y, const GetParams& params = {});
  int get_info_by_swift(const DoutPrefixProvider *dpp, 
                        const std::string& swift_name, RGWUserInfo *info,
                        optional_yield y, const GetParams& params = {});
  int get_info_by_access_key(const DoutPrefixProvider *dpp, 
                             const std::string& access_key, RGWUserInfo *info,
                             optional_yield y, const GetParams& params = {});

  int get_attrs_by_uid(const DoutPrefixProvider *dpp, 
                       const rgw_user& user_id,
                       std::map<std::string, bufferlist> *attrs,
                       optional_yield y,
                       RGWObjVersionTracker *objv_tracker = nullptr);

  int store_info(const DoutPrefixProvider *dpp, 
                 const RGWUserInfo& info, optional_yield y,
                 const PutParams& params = {});
  int remove_info(const DoutPrefixProvider *dpp, 
                  const RGWUserInfo& info, optional_yield y,
                  const RemoveParams& params = {});
};

// user metadata handler factory
auto create_user_metadata_handler(RGWSI_User *user_svc)
    -> std::unique_ptr<RGWMetadataHandler>;
