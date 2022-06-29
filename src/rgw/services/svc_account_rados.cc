#include "svc_account_rados.h"
#include "svc_mdlog.h"
#include "rgw/rgw_account.h"
#include "rgw/rgw_mdlog.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_zone.h"
#include "svc_zone.h"
#include "cls/user/cls_user_client.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw

constexpr auto RGW_ACCOUNT_USER_OBJ_SUFFIX = ".users";

RGWSI_Account_RADOS::RGWSI_Account_RADOS(RGWSI_Zone* zone_svc,
                                         RGWSI_MDLog* mdlog_svc,
                                         RGWSI_SysObj* sysobj_svc,
                                         RGWSI_RADOS* rados_svc)
  : RGWServiceInstance(zone_svc->ctx())
{
  svc.zone = zone_svc;
  svc.mdlog = mdlog_svc;
  svc.sysobj = sysobj_svc;
  svc.rados = rados_svc;
}

rgw_raw_obj RGWSI_Account_RADOS::get_account_user_obj(const std::string& account_id) const
{
  std::string oid = account_id + RGW_ACCOUNT_USER_OBJ_SUFFIX;
  return rgw_raw_obj(account_pool(), oid);
}


// metadata key for RGWAccountInfo
static std::string get_meta_key(const RGWAccountInfo& info)
{
  return info.id;
}

// metadata key for RGWAccountNameToId
static std::string get_name_meta_key(std::string_view tenant,
                                     std::string_view name)
{
  if (tenant.empty()) {
    return std::string{name};
  }
  return fmt::format("{}${}", tenant, name);
}

const rgw_pool& RGWSI_Account_RADOS::account_pool() const
{
  return svc.zone->get_zone_params().account_pool;
}

const rgw_pool& RGWSI_Account_RADOS::account_name_pool() const
{
  return svc.zone->get_zone_params().account_name_pool;
}

struct AccountNameObj {
  rgw_pool pool;
  std::string oid;
  RGWAccountNameToId data;
  RGWObjVersionTracker objv;

  AccountNameObj(const rgw_pool& pool,
                 std::string_view tenant,
                 std::string_view name)
      : pool(pool), oid(get_name_meta_key(tenant, name))
  {}
};

static int read_account_name(const DoutPrefixProvider* dpp,
                             RGWSysObjectCtx& obj_ctx,
                             AccountNameObj& obj, optional_yield y)
{
  bufferlist bl;
  int ret = rgw_get_system_obj(obj_ctx, obj.pool, obj.oid, bl,
                               &obj.objv, nullptr, y, dpp);
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
  return rgw_put_system_obj(dpp, obj_ctx, name.pool, name.oid, bl,
                            exclusive, &name.objv, ceph::real_time{}, y);
}

static int remove_account_name(const DoutPrefixProvider *dpp,
                               RGWSI_SysObj* sysobj_svc,
                               AccountNameObj& name, optional_yield y)
{
  return rgw_delete_system_obj(dpp, sysobj_svc, name.pool,
                               name.oid, &name.objv, y);
}

static int write_mdlog_entry(const DoutPrefixProvider* dpp,
                             RGWSI_MDLog* mdlog_svc, const std::string& key,
                             const RGWObjVersionTracker& objv)
{
  RGWMetadataLogData entry;
  entry.read_version = objv.read_version;
  entry.write_version = objv.write_version;
  entry.status = MDLOG_STATUS_COMPLETE;

  bufferlist bl;
  encode(entry, bl);

  const std::string section = "account";
  auto hash_key = fmt::format("{}:{}", section, key);
  return mdlog_svc->add_entry(dpp, hash_key, section, key, bl);
}

int RGWSI_Account_RADOS::store_account_info(const DoutPrefixProvider *dpp,
                                            RGWSysObjectCtx& obj_ctx,
                                            const RGWAccountInfo& info,
                                            const RGWAccountInfo* old_info,
                                            RGWObjVersionTracker& objv,
                                            const real_time& mtime,
                                            bool exclusive,
                                            std::map<std::string, bufferlist> *pattrs,
                                            optional_yield y)
{
  const std::string key = get_meta_key(info);

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
      AccountNameObj name{account_name_pool(), old_info->tenant, old_info->name};
      int ret = read_account_name(dpp, obj_ctx, name, y);
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
    AccountNameObj name{account_name_pool(), info.tenant, info.name};
    int ret = read_account_name(dpp, obj_ctx, name, y);
    if (ret == -ENOENT) {
      // write the new name object below
    } else if (ret == 0) {
      return -EEXIST; // account name is already taken by another account
    } else if (ret < 0) {
      return ret;
    }
  }

  // encode/write the account info
  {
    bufferlist bl;
    encode(info, bl);

    int ret = rgw_put_system_obj(dpp, obj_ctx, account_pool(), key, bl,
                                 exclusive, &objv, mtime, y, pattrs);
    if (ret < 0) {
      return ret;
    }
  }

  if (remove_name) {
    // remove the old name object, ignoring errors
    auto& name = *remove_name;
    int ret = remove_account_name(dpp, svc.sysobj, name, y);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "WARNING: remove_account_name obj=" << name.oid
          << " failed: " << cpp_strerror(ret) << dendl;
    } // not fatal
  }
  if (!same_name) {
    // write the new name object
    AccountNameObj name{account_name_pool(), info.tenant, info.name};
    name.data.id = info.id;
    name.objv.generate_new_write_ver(dpp->get_cct());
    int ret = write_account_name(dpp, obj_ctx, name, y);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << "WARNING: write_account_name obj=" << name.oid
          << " failed: " << cpp_strerror(ret) << dendl;
    } // not fatal
  }

  return write_mdlog_entry(dpp, svc.mdlog, key, objv);
}

int RGWSI_Account_RADOS::read_account_by_name(const DoutPrefixProvider* dpp,
                                              RGWSysObjectCtx& obj_ctx,
                                              std::string_view tenant,
                                              std::string_view name,
                                              RGWAccountInfo& info,
                                              RGWObjVersionTracker& objv,
                                              real_time* pmtime,
                                              std::map<std::string, bufferlist>* pattrs,
                                              optional_yield y)
{
  // read RGWAccountNameToId from account_name_pool
  AccountNameObj nameobj{account_name_pool(), tenant, name};
  int ret = read_account_name(dpp, obj_ctx, nameobj, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "account lookup with tenant=" << tenant
       << " name=" << name << " failed: " << cpp_strerror(ret) << dendl;
    return ret;
  }
  const auto& account_id = nameobj.data.id;
  return read_account_info(dpp, obj_ctx, account_id, info, objv, pmtime, pattrs, y);
}

int RGWSI_Account_RADOS::read_account_info(const DoutPrefixProvider* dpp,
                                           RGWSysObjectCtx& obj_ctx,
                                           std::string_view account_id,
                                           RGWAccountInfo& info,
                                           RGWObjVersionTracker& objv,
                                           real_time* pmtime,
                                           std::map<std::string, bufferlist>* pattrs,
                                           optional_yield y)
{
  bufferlist bl;
  int ret = rgw_get_system_obj(obj_ctx, account_pool(), std::string{account_id},
                               bl, &objv, pmtime, y, dpp, pattrs);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "account lookup with id=" << account_id
       << " failed: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  auto bl_iter = bl.cbegin();
  try {
    decode(info, bl_iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode account info, caught buffer::error" << dendl;
    return -EIO;
  }
  if (info.id != account_id) {
    ldpp_dout(dpp, 0) << "ERROR: read_account_info account id mismatch" << info.id << " != " << account_id << dendl;
    return -EIO;
  }
  return 0;
}

int RGWSI_Account_RADOS::remove_account_info(const DoutPrefixProvider* dpp,
                                             RGWSysObjectCtx& obj_ctx,
                                             const RGWAccountInfo& info,
                                             RGWObjVersionTracker& objv,
                                             optional_yield y)
{
  const std::string key = get_meta_key(info);

  // delete the account object
  int ret = rgw_delete_system_obj(dpp, svc.sysobj, account_pool(), key, &objv, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: remove_account_info id=" << info.id
       << " failed: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  // delete the account name object
  AccountNameObj name{account_name_pool(), info.tenant, info.name};
  ret = rgw_delete_system_obj(dpp, svc.sysobj, name.pool, name.oid, &name.objv, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "WARNING: remove_account_name tenant=" << info.tenant
       << " name=" << info.name << " failed: " << cpp_strerror(ret) << dendl;
    // not fatal
  }

  // record the change in the mdlog
  return write_mdlog_entry(dpp, svc.mdlog, key, objv);
}

int RGWSI_Account_RADOS::add_user(const DoutPrefixProvider* dpp,
                                  std::string_view account_id,
                                  const rgw_user& user,
                                  optional_yield y)
{
  RGWAccountInfo info;
  RGWSysObjectCtx obj_ctx{svc.sysobj};
  RGWObjVersionTracker objv;
  int ret = read_account_info(dpp, obj_ctx, account_id, info,
                              objv, nullptr, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  auto obj = get_account_user_obj(info.id);
  auto handle = svc.rados->obj(obj);
  ret = handle.open(dpp);
  if (ret < 0) {
    return ret;
  }

  librados::ObjectWriteOperation op;
  cls_account_users_add(op, user.to_str(), info.max_users);
  return handle.operate(dpp, &op, y);
}

int RGWSI_Account_RADOS::remove_user(const DoutPrefixProvider *dpp,
                                     std::string_view account_id,
                                     const rgw_user& user,
                                     optional_yield y)
{
  auto obj = get_account_user_obj(std::string{account_id});
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
                                    std::string_view account_id,
                                    const std::string& marker,
                                    int max_entries, bool *more,
                                    std::vector<rgw_user>& users,
                                    optional_yield y)
{
  auto obj = get_account_user_obj(std::string{account_id});
  auto handle = svc.rados->obj(obj);
  int ret = handle.open(dpp);
  if (ret < 0) {
    return ret;
  }

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
