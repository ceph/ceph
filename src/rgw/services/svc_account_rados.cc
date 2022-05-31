#include "svc_account_rados.h"
#include "svc_meta_be_sobj.h"
#include "svc_meta.h"
#include "rgw/rgw_account.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_zone.h"
#include "svc_zone.h"
#include "cls/user/cls_user_client.h"

#define dout_subsys ceph_subsys_rgw

constexpr auto RGW_ACCOUNT_USER_OBJ_SUFFIX = ".users";

class RGWSI_Account_Module : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Zone* zone_svc;

  const std::string prefix;
public:
  RGWSI_Account_Module(RGWSI_Zone* zone_svc)
      : RGWSI_MBSObj_Handler_Module("account"), zone_svc(zone_svc)
  {}

  void get_pool_and_oid(const std::string& key, rgw_pool *pool, std::string *oid) override {
    if (pool) {
      *pool = zone_svc->get_zone_params().account_pool;
    }
    if (oid) {
      *oid = key;
    }
  }

  const std::string& get_oid_prefix() override {
    return prefix;
  }

  bool is_valid_oid(const std::string& oid) override {
    // filter out the user.buckets objects
    return !boost::algorithm::ends_with(oid, RGW_ACCOUNT_USER_OBJ_SUFFIX);
  }

  std::string key_to_oid(const std::string& key) override {
    return key;
  }

  std::string oid_to_key(const std::string& oid) override {
    return oid;
  }
};

RGWSI_Account_RADOS::RGWSI_Account_RADOS(CephContext *cct) :
  RGWSI_Account(cct) {
}

void RGWSI_Account_RADOS::init(RGWSI_Zone *_zone_svc,
                               RGWSI_Meta *_meta_svc,
                               RGWSI_MetaBackend *_meta_be_svc,
                               RGWSI_RADOS *_rados_svc)
{
  svc.zone = _zone_svc;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
  svc.rados = _rados_svc;
}

int RGWSI_Account_RADOS::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{
  int r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ,
                                     &be_handler);
  if (r < 0) {
    ldout(ctx(), 0) << "ERROR: failed to create be_handler for accounts: r=" << r << dendl;
    return r;
  }

  auto mod = std::make_unique<RGWSI_Account_Module>(svc.zone);

  RGWSI_MetaBackend_Handler_SObj *bh = static_cast<RGWSI_MetaBackend_Handler_SObj *>(be_handler);
  bh->set_module(mod.get());

  be_module = std::move(mod);
  return 0;
}

rgw_raw_obj RGWSI_Account_RADOS::get_account_user_obj(const std::string& account_id) const
{
  std::string oid = account_id + RGW_ACCOUNT_USER_OBJ_SUFFIX;
  return rgw_raw_obj(svc.zone->get_zone_params().account_pool, oid);
}

int RGWSI_Account_RADOS::store_account_info(const DoutPrefixProvider *dpp,
                                            RGWSI_MetaBackend::Context *_ctx,
                                            const RGWAccountInfo& info,
                                            RGWObjVersionTracker *objv_tracker,
                                            const real_time& mtime,
                                            bool exclusive,
                                            std::map<std::string, bufferlist> *pattrs,
                                            optional_yield y)
{
  bufferlist data_bl;
  encode(info, data_bl);

  RGWSI_MBSObj_PutParams params(data_bl, pattrs, mtime, exclusive);
  return svc.meta_be->put(_ctx, get_meta_key(info), params, objv_tracker, y, dpp);
}

int RGWSI_Account_RADOS::read_account_info(const DoutPrefixProvider* dpp,
                                           RGWSI_MetaBackend::Context *ctx,
                                           const std::string& account_id,
                                           RGWAccountInfo *info,
                                           RGWObjVersionTracker * const objv_tracker,
                                           real_time * const pmtime,
                                           std::map<std::string, bufferlist> * const pattrs,
                                           optional_yield y)
{
  bufferlist bl;
  RGWSI_MBSObj_GetParams params(&bl, pattrs, pmtime);
  int r = svc.meta_be->get_entry(ctx, account_id, params, objv_tracker, y, dpp);
  if (r < 0) {
    return r;
  }

  auto bl_iter = bl.cbegin();
  try {
    decode(*info, bl_iter);
    if (info->get_id() != account_id) {
      lderr(svc.meta_be->ctx()) << "ERROR: read_account_info account id mismatch" << info->get_id() << "!= " << account_id << dendl;
      return -EIO;
    }
  } catch (buffer::error& err) {
    ldout(svc.meta_be->ctx(), 0) << "ERROR: failed to decode account info, caught buffer::error" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSI_Account_RADOS::remove_account_info(const DoutPrefixProvider* dpp,
                                             RGWSI_MetaBackend::Context *ctx,
                                             const std::string& account_id,
                                             RGWObjVersionTracker *objv_tracker,
                                             optional_yield y)
{
  RGWSI_MBSObj_RemoveParams params;
  int ret = svc.meta_be->remove(ctx, account_id, params, objv_tracker, y, dpp);
  if (ret <0 && ret != -ENOENT && ret != -ECANCELED) {
    ldpp_dout(dpp, 0) << "ERROR: could not remove account: " << account_id << dendl;
    return ret;
  }
  return 0;
}

int RGWSI_Account_RADOS::add_user(const DoutPrefixProvider* dpp,
                                  const RGWAccountInfo& info,
                                  const rgw_user& user,
                                  optional_yield y)
{
  auto obj = get_account_user_obj(info.id);
  auto handle = svc.rados->obj(obj);
  int ret = handle.open(dpp);
  if (ret < 0) {
    return ret;
  }

  librados::ObjectWriteOperation op;
  cls_account_users_add(op, user.to_str(), info.max_users);
  return handle.operate(dpp, &op, y);
}

int RGWSI_Account_RADOS::remove_user(const DoutPrefixProvider *dpp,
                                     const RGWAccountInfo& info,
                                     const rgw_user& user,
                                     optional_yield y)
{
  auto obj = get_account_user_obj(info.id);
  auto handle = svc.rados->obj(obj);
  int ret = handle.open(dpp);
  if (ret < 0) {
    return ret;
  }

  librados::ObjectWriteOperation op;
  cls_account_users_rm(op, user.to_str());
  return handle.operate(dpp, &op, y);
}


int RGWSI_Account_RADOS::list_users(const DoutPrefixProvider *dpp,
                                    const RGWAccountInfo& info,
                                    const std::string& marker,
                                    bool *more,
                                    std::vector<rgw_user>& users,
                                    optional_yield y)
{
  auto obj = get_account_user_obj(info.id);
  auto handle = svc.rados->obj(obj);
  int ret = handle.open(dpp);
  if (ret < 0) {
    return ret;
  }

  static constexpr uint32_t max_entries = 1000u;
  std::vector<std::string> entries;
  int ret2 = 0;

  librados::ObjectReadOperation op;
  cls_account_users_list(op, marker, max_entries, entries, more, &ret2);

  ret = handle.operate(dpp, &op, nullptr, y);
  if (ret < 0) {
    return ret;
  }
  if (ret2 < 0) {
    return ret2;
  }

  // note that copy converts std::string -> rgw_user via from_str()
  std::copy(entries.begin(), entries.end(), users.end());
  return 0;
}
