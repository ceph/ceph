// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_user_ctl.h"


#include "rgw_account.h"
#include "rgw_bucket.h"
#include "rgw_metadata.h"
#ifdef WITH_RADOSGW_RADOS
#include "rgw_metadata_lister.h"
#endif

#include "services/svc_user.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

struct RGWUserCompleteInfo {
  RGWUserInfo info;
  std::map<std::string, bufferlist> attrs;
  bool has_attrs{false};

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
  RGWUserMetadataObject(const RGWUserCompleteInfo& uci,
                        const obj_version& v, ceph::real_time m)
    : RGWMetadataObject(v, m), uci(uci) {}

  void dump(Formatter *f) const override {
    uci.dump(f);
  }

  RGWUserCompleteInfo& get_uci() {
    return uci;
  }
};

#ifdef WITH_RADOSGW_RADOS
class RGWUserMetadataHandler : public RGWMetadataHandler {
  RGWSI_User *svc_user{nullptr};
 public:
  explicit RGWUserMetadataHandler(RGWSI_User* svc_user)
    : svc_user(svc_user) {}

  string get_type() override { return "user"; }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv,
                                  const ceph::real_time& mtime) override {
    RGWUserCompleteInfo uci;

    try {
      decode_json_obj(uci, jo);
    } catch (JSONDecoder::err& e) {
      return nullptr;
    }

    return new RGWUserMetadataObject(uci, objv, mtime);
  }

  int get(std::string& entry, RGWMetadataObject** obj, optional_yield y,
          const DoutPrefixProvider *dpp) override;
  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv_tracker,
          optional_yield y, const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override;
  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp) override;

  int mutate(const std::string& entry, const ceph::real_time& mtime,
             RGWObjVersionTracker* objv_tracker, optional_yield y,
             const DoutPrefixProvider* dpp, RGWMDLogStatus op_type,
             std::function<int()> f) override;

  int list_keys_init(const DoutPrefixProvider* dpp, const std::string& marker,
                     void** phandle) override;
  int list_keys_next(const DoutPrefixProvider* dpp, void* handle, int max,
                     std::list<std::string>& keys, bool* truncated) override;
  void list_keys_complete(void *handle) override;
  std::string get_marker(void *handle) override;
};

int RGWUserMetadataHandler::get(std::string& entry, RGWMetadataObject **obj,
                                optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWUserCompleteInfo uci;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;

  rgw_user user = RGWSI_User::user_from_meta_key(entry);

  int ret = svc_user->read_user_info(user, &uci.info, &objv_tracker,
                                     &mtime, nullptr, &uci.attrs,
                                     y, dpp);
  if (ret < 0) {
    return ret;
  }

  *obj = new RGWUserMetadataObject(uci, objv_tracker.read_version, mtime);
  return 0;
}

int RGWUserMetadataHandler::put(std::string& entry, RGWMetadataObject *obj,
                                RGWObjVersionTracker& objv_tracker,
                                optional_yield y, const DoutPrefixProvider *dpp,
                                RGWMDLogSyncType type, bool from_remote_zone)
{
  const rgw_user user = RGWSI_User::user_from_meta_key(entry);

  // read existing user info
  std::optional old = RGWUserCompleteInfo{};
  int ret = svc_user->read_user_info(user, &old->info, &objv_tracker,
                                     nullptr, nullptr, &old->attrs, y, dpp);
  if (ret == -ENOENT) {
    old = std::nullopt;
  } else if (ret < 0) {
    return ret;
  }
  RGWUserInfo* pold_info = (old ? &old->info : nullptr);

  // store the updated user info
  auto newobj = static_cast<RGWUserMetadataObject*>(obj);
  RGWUserCompleteInfo& uci = newobj->get_uci();
  auto pattrs = (uci.has_attrs ? &uci.attrs : nullptr);
  auto mtime = obj->get_mtime();

  ret = svc_user->store_user_info(uci.info, pold_info, &objv_tracker,
                                  mtime, false, pattrs, y, dpp);
  if (ret < 0) {
    return ret;
  }

  return STATUS_APPLIED;
}

int RGWUserMetadataHandler::remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
                                   optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWUserInfo info;

  rgw_user user = RGWSI_User::user_from_meta_key(entry);

  int ret = svc_user->read_user_info(user, &info, nullptr,
                                     nullptr, nullptr, nullptr,
                                     y, dpp);
  if (ret < 0) {
    return ret;
  }

  return svc_user->remove_user_info(info, &objv_tracker, y, dpp);
};

int RGWUserMetadataHandler::mutate(const std::string& entry, const ceph::real_time& mtime,
                                   RGWObjVersionTracker* objv_tracker, optional_yield y,
                                   const DoutPrefixProvider* dpp, RGWMDLogStatus op_type,
                                   std::function<int()> f)
{
  return -ENOTSUP; // unused
}

int RGWUserMetadataHandler::list_keys_init(const DoutPrefixProvider* dpp,
                                           const std::string& marker,
                                           void** phandle)
{
  std::unique_ptr<RGWMetadataLister> lister;
  int ret = svc_user->create_lister(dpp, marker, lister);
  if (ret < 0) {
    return ret;
  }
  *phandle = lister.release(); // release ownership
  return 0;
}

int RGWUserMetadataHandler::list_keys_next(const DoutPrefixProvider* dpp,
                                           void* handle, int max,
                                           std::list<std::string>& keys,
                                           bool* truncated)
{
  auto lister = static_cast<RGWMetadataLister*>(handle);
  return lister->get_next(dpp, max, keys, truncated);
}

void RGWUserMetadataHandler::list_keys_complete(void *handle)
{
  delete static_cast<RGWMetadataLister*>(handle);
}

std::string RGWUserMetadataHandler::get_marker(void *handle)
{
  auto lister = static_cast<RGWMetadataLister*>(handle);
  return lister->get_marker();
}
#endif

RGWUserCtl::RGWUserCtl(RGWSI_Zone *zone_svc, RGWSI_User *user_svc)
{
  svc.zone = zone_svc;
  svc.user = user_svc;
}

template <class T>
class optional_default
{
  const std::optional<T>& opt;
  std::optional<T> def;
  const T *p;
public:
  optional_default(const std::optional<T>& _o) : opt(_o) {
    if (opt) {
      p = &(*opt);
    } else {
      def = T();
      p = &(*def);
    }
  }

  const T *operator->() {
    return p;
  }

  const T& operator*() {
    return *p;
  }
};

int RGWUserCtl::get_info_by_uid(const DoutPrefixProvider *dpp, 
                                const rgw_user& uid,
                                RGWUserInfo *info,
                                optional_yield y,
                                const GetParams& params)

{
  return svc.user->read_user_info(uid,
                                  info,
                                  params.objv_tracker,
                                  params.mtime,
                                  params.cache_info,
                                  params.attrs,
                                  y,
                                  dpp);
}

int RGWUserCtl::get_info_by_email(const DoutPrefixProvider *dpp, 
                                  const string& email,
                                  RGWUserInfo *info,
                                  optional_yield y,
                                  const GetParams& params)
{
  return svc.user->get_user_info_by_email(email,
                                          info,
                                          params.objv_tracker,
                                          params.attrs,
                                          params.mtime,
                                          y,
                                          dpp);
}

int RGWUserCtl::get_info_by_swift(const DoutPrefixProvider *dpp, 
                                  const string& swift_name,
                                  RGWUserInfo *info,
                                  optional_yield y,
                                  const GetParams& params)
{
  return svc.user->get_user_info_by_swift(swift_name,
                                          info,
                                          params.objv_tracker,
                                          params.attrs,
                                          params.mtime,
                                          y,
                                          dpp);
}

int RGWUserCtl::get_info_by_access_key(const DoutPrefixProvider *dpp, 
                                       const string& access_key,
                                       RGWUserInfo *info,
                                       optional_yield y,
                                       const GetParams& params)
{
  return svc.user->get_user_info_by_access_key(access_key,
                                               info,
                                               params.objv_tracker,
                                               params.attrs,
                                               params.mtime,
                                               y,
                                               dpp);
}

int RGWUserCtl::get_attrs_by_uid(const DoutPrefixProvider *dpp, 
                                 const rgw_user& user_id,
                                 map<string, bufferlist> *pattrs,
                                 optional_yield y,
                                 RGWObjVersionTracker *objv_tracker)
{
  RGWUserInfo user_info;

  return get_info_by_uid(dpp, user_id, &user_info, y, RGWUserCtl::GetParams()
                         .set_attrs(pattrs)
                         .set_objv_tracker(objv_tracker));
}

int RGWUserCtl::store_info(const DoutPrefixProvider *dpp, 
                           const RGWUserInfo& info, optional_yield y,
                           const PutParams& params)
{
  return svc.user->store_user_info(info,
                                   params.old_info,
                                   params.objv_tracker,
                                   params.mtime,
                                   params.exclusive,
                                   params.attrs,
                                   y,
                                   dpp);
}

int RGWUserCtl::remove_info(const DoutPrefixProvider *dpp, 
                            const RGWUserInfo& info, optional_yield y,
                            const RemoveParams& params)

{
  return svc.user->remove_user_info(info, params.objv_tracker, y, dpp);
}

#ifdef WITH_RADOSGW_RADOS
auto create_user_metadata_handler(RGWSI_User *user_svc)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<RGWUserMetadataHandler>(user_svc);
}
#endif

void rgw_user::dump(Formatter *f) const
{
  ::encode_json("user", *this, f);
}

