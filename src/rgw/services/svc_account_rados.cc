#include "svc_account_rados.h"
#include "svc_meta_be_sobj.h"
#include "svc_meta.h"
#include "rgw/rgw_account.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_zone.h"
#include "svc_zone.h"
#include "cls/user/cls_user_client.h"
#include "common/errno.h"

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

void RGWSI_Account_RADOS::init(RGWSI_Zone* zone_svc,
                               RGWSI_Meta* meta_svc,
                               RGWSI_MetaBackend* meta_be_svc,
                               RGWSI_SysObj* sysobj_svc,
                               RGWSI_RADOS* rados_svc)
{
  svc.zone = zone_svc;
  svc.meta = meta_svc;
  svc.meta_be = meta_be_svc;
  svc.sysobj = sysobj_svc;
  svc.rados = rados_svc;
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

static rgw_raw_obj get_account_name_obj(RGWSI_Zone* zone_svc,
                                        std::string_view tenant,
                                        std::string_view name)
{
  const auto& pool = zone_svc->get_zone_params().account_name_pool;
  return {pool, RGWSI_Account::get_name_meta_key(tenant, name)};
}

struct AccountNameObj {
  RGWAccountNameToId data;
  rgw_raw_obj obj;
  RGWObjVersionTracker objv;
};

static int read_account_name(const DoutPrefixProvider* dpp,
                             RGWSI_Zone* zone_svc, RGWSysObjectCtx& obj_ctx,
                             std::string_view tenant, std::string_view name,
                             AccountNameObj& obj, optional_yield y)
{
  obj.obj = get_account_name_obj(zone_svc, tenant, name);
  bufferlist bl;

  int ret = rgw_get_system_obj(obj_ctx, obj.obj.pool, obj.obj.oid,
                               bl, &obj.objv, nullptr, y, dpp);
  if (ret < 0) {
    return ret;
  }
  try {
    auto p = bl.cbegin();
    decode(obj.data, p);
  } catch (const buffer::error&) {
    return -EIO;
  }
  return 0;
}

static int write_account_name(const DoutPrefixProvider *dpp,
                              RGWSysObjectCtx& obj_ctx,
                              AccountNameObj& name, optional_yield y)
{
  bufferlist bl;
  encode(name.data, bl);

  constexpr bool exclusive = true;
  return rgw_put_system_obj(dpp, obj_ctx, name.obj.pool, name.obj.oid, bl,
                            exclusive, &name.objv, ceph::real_time{}, y);
}

static int remove_account_name(const DoutPrefixProvider *dpp,
                               RGWSI_Zone* zone_svc, RGWSI_SysObj* sysobj_svc,
                               std::string_view tenant, std::string_view name,
                               RGWObjVersionTracker* objv, optional_yield y)
{
  const auto obj = get_account_name_obj(zone_svc, tenant, name);
  return rgw_delete_system_obj(dpp, sysobj_svc, obj.pool, obj.oid, objv, y);
}

static int remove_account_name(const DoutPrefixProvider *dpp,
                               RGWSI_SysObj* sysobj_svc,
                               AccountNameObj& name, optional_yield y)
{
  return rgw_delete_system_obj(dpp, sysobj_svc, name.obj.pool, name.obj.oid,
                               &name.objv, y);
}

int RGWSI_Account_RADOS::store_account_info(const DoutPrefixProvider *dpp,
                                            RGWSI_MetaBackend::Context* _ctx,
                                            const RGWAccountInfo& info,
                                            const RGWAccountInfo* old_info,
                                            RGWObjVersionTracker& objv,
                                            const real_time& mtime,
                                            bool exclusive,
                                            std::map<std::string, bufferlist> *pattrs,
                                            optional_yield y)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);
  RGWSysObjectCtx& obj_ctx = *ctx->obj_ctx;

  const bool same_name = old_info
      && old_info->tenant == info.tenant
      && old_info->name == info.name;

  std::optional<AccountNameObj> remove_name;
  if (old_info) {
    if (old_info->id != info.id) {
      return -EINVAL; // can't modify account id
    }
    if (!same_name) {
      // read old account name object
      AccountNameObj name;
      int ret = read_account_name(dpp, svc.zone, obj_ctx, old_info->tenant,
                                  old_info->name, name, y);
      if (ret == -ENOENT) {
        // leave remove_name empty
      } else if (ret < 0) {
        return ret;
      } else if (name.data.id == info.id) {
        remove_name = std::move(name);
      }
    }
  }

  if (!same_name) {
    // read new account name object
    AccountNameObj name;
    int ret = read_account_name(dpp, svc.zone, obj_ctx, info.tenant,
                                info.name, name, y);
    if (ret == -ENOENT) {
      // write the new name object below
    } else if (ret == 0) {
      return -EEXIST; // account name is already taken by another account
    } else if (ret < 0) {
      return ret;
    }
  }

  // encode/write the account info
  bufferlist data_bl;
  encode(info, data_bl);

  RGWSI_MBSObj_PutParams params(data_bl, pattrs, mtime, exclusive);
  int ret = svc.meta_be->put(_ctx, get_meta_key(info), params, &objv, y, dpp);
  if (ret < 0) {
    return ret;
  }

  if (remove_name) {
    // remove the old name object, ignoring errors
    auto& name = *remove_name;
    int r2 = remove_account_name(dpp, svc.sysobj, name, y);
    if (r2 < 0) {
      ldpp_dout(dpp, 20) << "WARNING: remove_account_name obj=" << name.obj
          << " failed: " << cpp_strerror(r2) << dendl;
    } // not fatal
  }
  if (!same_name) {
    // write the new name object
    AccountNameObj name;
    name.data.id = info.id;
    name.obj = get_account_name_obj(svc.zone, info.tenant, info.name);
    name.objv.generate_new_write_ver(dpp->get_cct());
    int r2 = write_account_name(dpp, obj_ctx, name, y);
    if (r2 < 0) {
      ldpp_dout(dpp, 20) << "WARNING: write_account_name obj=" << name.obj
          << " failed: " << cpp_strerror(r2) << dendl;
    } // not fatal
  }
  return 0;
}

int RGWSI_Account_RADOS::read_account_by_name(const DoutPrefixProvider* dpp,
                                              RGWSI_MetaBackend::Context* _ctx,
                                              std::string_view tenant,
                                              std::string_view name,
                                              RGWAccountInfo& info,
                                              RGWObjVersionTracker& objv,
                                              real_time* pmtime,
                                              std::map<std::string, bufferlist>* pattrs,
                                              optional_yield y)
{
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  // read RGWAccountNameToId from account_name_pool
  AccountNameObj obj;
  int ret = read_account_name(dpp, svc.zone, *ctx->obj_ctx, tenant, name, obj, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "account lookup with tenant=" << tenant
       << " name=" << name << " failed: " << cpp_strerror(ret) << dendl;
    return ret;
  }
  const auto& account_id = obj.data.id;
  return read_account_info(dpp, _ctx, account_id, info, objv, pmtime, pattrs, y);
}

int RGWSI_Account_RADOS::read_account_info(const DoutPrefixProvider* dpp,
                                           RGWSI_MetaBackend::Context *ctx,
                                           std::string_view account_id,
                                           RGWAccountInfo& info,
                                           RGWObjVersionTracker& objv,
                                           real_time* pmtime,
                                           std::map<std::string, bufferlist>* pattrs,
                                           optional_yield y)
{
  bufferlist bl;
  RGWSI_MBSObj_GetParams params(&bl, pattrs, pmtime);
  int r = svc.meta_be->get_entry(ctx, std::string{account_id},
                                 params, &objv, y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 20) << "account lookup with id=" << account_id
       << " failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  auto bl_iter = bl.cbegin();
  try {
    decode(info, bl_iter);
  } catch (buffer::error& err) {
    ldout(svc.meta_be->ctx(), 0) << "ERROR: failed to decode account info, caught buffer::error" << dendl;
    return -EIO;
  }
  if (info.id != account_id) {
    lderr(svc.meta_be->ctx()) << "ERROR: read_account_info account id mismatch" << info.id << " != " << account_id << dendl;
    return -EIO;
  }
  return 0;
}

int RGWSI_Account_RADOS::remove_account_info(const DoutPrefixProvider* dpp,
                                             RGWSI_MetaBackend::Context *ctx,
                                             const RGWAccountInfo& info,
                                             RGWObjVersionTracker& objv,
                                             optional_yield y)
{
  RGWSI_MBSObj_RemoveParams params;
  int ret = svc.meta_be->remove(ctx, get_meta_key(info), params, &objv, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: remove_account_info id=" << info.id
       << " failed: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  ret = remove_account_name(dpp, svc.zone, svc.sysobj,
                            info.tenant, info.name, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "WARNING: remove_account_name tenant=" << info.tenant
       << " name=" << info.name << " failed: " << cpp_strerror(ret) << dendl;
    // not fatal
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
