#include "rgw_account.h"
#include "rgw_metadata.h"
#include "rgw_sal.h"
#include "common/random_string.h"
#include "common/utf8.h"

#include "services/svc_account_rados.h"
#include "services/svc_sys_obj.h"

#define dout_subsys ceph_subsys_rgw

// account ids start with 'RGW' followed by 17 numeric digits
static constexpr std::string_view account_id_prefix = "RGW";
static constexpr std::size_t account_id_len = 20;

std::string rgw_generate_account_id(CephContext* cct)
{
  // fill with random numeric digits
  std::string id = gen_rand_numeric(cct, account_id_len);
  // overwrite the prefix bytes
  std::copy(account_id_prefix.begin(), account_id_prefix.end(), id.begin());
  return id;
}

bool rgw_validate_account_id(std::string_view id, std::string& err_msg)
{
  if (id.size() != account_id_len) {
    err_msg = fmt::format("account id must be {} bytes long", account_id_len);
    return false;
  }
  if (id.compare(0, account_id_prefix.size(), account_id_prefix) != 0) {
    err_msg = fmt::format("account id must start with {}", account_id_prefix);
    return false;
  }
  auto suffix = id.substr(account_id_prefix.size());
  // all remaining bytes must be digits
  constexpr auto digit = [] (int c) { return std::isdigit(c); };
  if (!std::all_of(suffix.begin(), suffix.end(), digit)) {
    err_msg = "account id must end with numeric digits";
    return false;
  }
  return true;
}

bool rgw_validate_account_name(std::string_view name, std::string& err_msg)
{
  if (name.empty()) {
    err_msg = "account name must not be empty";
    return false;
  }
  // must not contain the tenant delimiter $
  if (name.find('$') != name.npos) {
    err_msg = "account name must not contain $";
    return false;
  }
  // must not contain the metadata section delimeter :
  if (name.find(':') != name.npos) {
    err_msg = "account name must not contain :";
    return false;
  }
  // must be valid utf8
  if (check_utf8(name.data(), name.size()) != 0) {
    err_msg = "account name must be valid utf8";
    return false;
  }
  return true;
}

void RGWAccountInfo::dump(Formatter * const f) const
{
  encode_json("id", id, f);
  encode_json("tenant", tenant, f);
  encode_json("name", name, f);
  encode_json("max_users", max_users, f);
}

void RGWAccountInfo::decode_json(JSONObj* obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("tenant", tenant, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("max_users", max_users, obj);
}

void RGWAccountInfo::generate_test_instances(std::list<RGWAccountInfo*>& o)
{
  o.push_back(new RGWAccountInfo);
  auto p = new RGWAccountInfo;
  p->id = "account1";
  p->tenant = "tenant1";
  p->name = "name1";
  o.push_back(p);
}


RGWAccountMetadataHandler::RGWAccountMetadataHandler(RGWSI_Account_RADOS* svc_account,
                                                     RGWSI_SysObj* svc_sysobj)
  : svc_account(svc_account), svc_sysobj(svc_sysobj)
{
  base_init(svc_account->ctx());
}

RGWMetadataObject* RGWAccountMetadataHandler::get_meta_obj(JSONObj *jo,
                                                           const obj_version& objv,
                                                           const ceph::real_time& mtime)
{
  RGWAccountCompleteInfo aci;
  try {
    decode_json_obj(aci, jo);
  } catch (const JSONDecoder::err&) {
    return nullptr;
  }
  return new RGWAccountMetadataObject(aci, objv, mtime);
}

int RGWAccountMetadataHandler::get(std::string& entry,
                                   RGWMetadataObject** obj,
                                   optional_yield y,
                                   const DoutPrefixProvider* dpp)
{
  RGWAccountCompleteInfo aci;
  RGWObjVersionTracker objv;
  real_time mtime;

  RGWSysObjectCtx ctx{svc_sysobj};
  int ret = svc_account->read_account_info(dpp, ctx, entry, aci.info,
                                           objv, &mtime, &aci.attrs, y);
  if (ret < 0) {
    return ret;
  }

  *obj = new RGWAccountMetadataObject(aci, objv.read_version, mtime);
  return 0;
}

int RGWAccountMetadataHandler::put(std::string& entry, RGWMetadataObject* obj,
                                   RGWObjVersionTracker& objv, optional_yield y,
                                   const DoutPrefixProvider* dpp,
                                   RGWMDLogSyncType type, bool from_remote_zone)
{
  auto account_obj = static_cast<RGWAccountMetadataObject*>(obj);
  const auto& new_info = account_obj->get_aci().info;

  // account id must match metadata key
  if (new_info.id != entry) {
    return -EINVAL;
  }

  // read existing metadata
  RGWSysObjectCtx ctx{svc_sysobj};
  RGWAccountInfo old_info;
  int ret = svc_account->read_account_info(dpp, ctx, entry, old_info,
                                           objv, nullptr, nullptr, y);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  const RGWAccountInfo* pold_info = (ret == -ENOENT ? nullptr : &old_info);

  // write/overwrite metadata
  constexpr bool exclusive = false;
  return svc_account->store_account_info(dpp, ctx, new_info, pold_info,
                                         objv, obj->get_mtime(), exclusive,
                                         obj->get_pattrs(), y);
}

int RGWAccountMetadataHandler::remove(std::string& entry,
                                      RGWObjVersionTracker& objv,
                                      optional_yield y,
                                      const DoutPrefixProvider* dpp)
{
  // read existing metadata
  RGWSysObjectCtx ctx{svc_sysobj};
  RGWAccountInfo old_info;
  int ret = svc_account->read_account_info(dpp, ctx, entry, old_info,
                                           objv, nullptr, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  return svc_account->remove_account_info(dpp, ctx, old_info, objv, y);
}

int RGWAccountMetadataHandler::mutate(const std::string& entry,
                                      const ceph::real_time& mtime,
                                      RGWObjVersionTracker *objv_tracker,
                                      optional_yield y,
                                      const DoutPrefixProvider *dpp,
                                      RGWMDLogStatus op_type,
                                      std::function<int()> f)
{
  return -ENOTSUP; // unused
}

struct account_list_handle {
  using Pool = RGWSI_SysObj::Pool;
  Pool pool;
  Pool::Op op;

  account_list_handle(RGWSI_SysObj* sysobj, const rgw_pool& p)
      : pool(sysobj->get_pool(p)), op(pool.op()) {}
};

int RGWAccountMetadataHandler::list_keys_init(const DoutPrefixProvider* dpp,
                                              const std::string& marker,
                                              void** phandle)
{
  const auto& pool = svc_account->account_pool();
  auto handle = std::make_unique<account_list_handle>(svc_sysobj, pool);
  int ret = handle->op.init(dpp, marker, "");
  if (ret < 0) {
    return ret;
  }
  *phandle = handle.release();
  return 0;
}

int RGWAccountMetadataHandler::list_keys_next(const DoutPrefixProvider* dpp,
                                              void* handle, int max,
                                              std::list<std::string>& keys,
                                              bool* truncated)
{
  auto h = static_cast<account_list_handle*>(handle);
  std::vector<std::string> oids;
  int ret = h->op.get_next(dpp, max, &oids, truncated);
  if (ret < 0) {
    if (ret == -ENOENT) {
      *truncated = false;
    }
    return ret;
  }
  std::move(oids.begin(), oids.end(), std::back_inserter(keys));
  return 0;
}

void RGWAccountMetadataHandler::list_keys_complete(void* handle)
{
  delete static_cast<account_list_handle*>(handle);
}

std::string RGWAccountMetadataHandler::get_marker(void* handle)
{
  std::string marker;
  auto h = static_cast<account_list_handle*>(handle);
  h->op.get_marker(&marker);
  return marker;
}

int RGWAdminOp_Account::create(const DoutPrefixProvider *dpp,
                               rgw::sal::Store *store,
                               RGWAccountAdminOpState& op_state,
                               std::string& err_msg,
                               RGWFormatterFlusher& flusher,
                               optional_yield y)
{
  // account name is required
  if (!op_state.has_account_name()) {
    err_msg = "requires an account name";
    return -EINVAL;
  }
  if (!rgw_validate_account_name(op_state.account_name, err_msg)) {
    return -EINVAL;
  }

  RGWAccountInfo info;
  info.tenant = op_state.tenant;
  info.name = op_state.account_name;

  // account id is optional, but must be valid
  if (!op_state.has_account_id()) {
    info.id = rgw_generate_account_id(dpp->get_cct());
  } else if (!rgw_validate_account_id(op_state.account_id, err_msg)) {
    return -EINVAL;
  } else {
    info.id = op_state.account_id;
  }

  constexpr RGWAccountInfo* old_info = nullptr;
  constexpr bool exclusive = true;
  auto& objv = op_state.objv_tracker;

  int ret = store->store_account(dpp, info, old_info, objv, exclusive, y);
  // TODO: on -EEXIST, retry with new random id unless has_account_id()
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  encode_json("AccountInfo", info, flusher.get_formatter());
  flusher.flush();

  return 0;
}

int RGWAdminOp_Account::modify(const DoutPrefixProvider *dpp,
                               rgw::sal::Store *store,
                               RGWAccountAdminOpState& op_state,
                               std::string& err_msg,
                               RGWFormatterFlusher& flusher,
                               optional_yield y)
{
  int ret = 0;
  RGWAccountInfo info;
  auto& objv = op_state.objv_tracker;
  if (op_state.has_account_id()) {
    ret = store->load_account_by_id(dpp, op_state.account_id, info, objv, y);
  } else if (op_state.has_account_name()) {
    ret = store->load_account_by_name(dpp, op_state.tenant,
                                      op_state.account_name,
                                      info, objv, y);
  } else {
    err_msg = "requires account id or name";
    return -EINVAL;
  }
  if (ret < 0) {
    return ret;
  }
  const RGWAccountInfo old_info = info;

  if (!op_state.tenant.empty() && op_state.tenant != info.tenant) {
    err_msg = "cannot modify account tenant";
    return -EINVAL;
  }

  if (op_state.has_account_name()) {
    // name must be valid
    if (!rgw_validate_account_name(op_state.account_name, err_msg)) {
      return -EINVAL;
    }
    info.name = op_state.account_name;
  }

  constexpr bool exclusive = false;

  ret = store->store_account(dpp, info, &old_info, objv, exclusive, y);
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  encode_json("AccountInfo", info, flusher.get_formatter());
  flusher.flush();

  return 0;
}

int RGWAdminOp_Account::remove(const DoutPrefixProvider *dpp,
                               rgw::sal::Store *store,
                               RGWAccountAdminOpState& op_state,
                               std::string& err_msg,
                               RGWFormatterFlusher& flusher,
                               optional_yield y)
{
  int ret = 0;
  RGWAccountInfo info;

  auto& objv = op_state.objv_tracker;
  if (op_state.has_account_id()) {
    ret = store->load_account_by_id(dpp, op_state.account_id, info, objv, y);
  } else if (op_state.has_account_name()) {
    ret = store->load_account_by_name(dpp, op_state.tenant,
                                      op_state.account_name,
                                      info, objv, y);
  } else {
    err_msg = "requires account id or name";
    return -EINVAL;
  }
  if (ret < 0) {
    return ret;
  }

  return store->delete_account(dpp, info, objv, y);
}

int RGWAdminOp_Account::info(const DoutPrefixProvider *dpp,
                             rgw::sal::Store *store,
                             RGWAccountAdminOpState& op_state,
                             std::string& err_msg,
                             RGWFormatterFlusher& flusher,
                             optional_yield y)
{
  int ret = 0;
  RGWAccountInfo info;

  auto& objv = op_state.objv_tracker;
  if (op_state.has_account_id()) {
    ret = store->load_account_by_id(dpp, op_state.account_id, info, objv, y);
  } else if (op_state.has_account_name()) {
    ret = store->load_account_by_name(dpp, op_state.tenant,
                                      op_state.account_name,
                                      info, objv, y);
  } else {
    err_msg = "requires account id or name";
    return -EINVAL;
  }
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  encode_json("AccountInfo", info, flusher.get_formatter());
  flusher.flush();

  return 0;
}

int RGWAdminOp_Account::list_users(const DoutPrefixProvider *dpp,
                                   rgw::sal::Store* store,
                                   RGWAccountAdminOpState& op_state,
                                   std::string& err_msg,
                                   RGWFormatterFlusher& flusher,
                                   optional_yield y)
{
  int ret = 0;
  RGWAccountInfo info;

  auto& objv = op_state.objv_tracker;
  if (op_state.has_account_id()) {
    ret = store->load_account_by_id(dpp, op_state.account_id, info, objv, y);
  } else if (op_state.has_account_name()) {
    ret = store->load_account_by_name(dpp, op_state.tenant,
                                      op_state.account_name,
                                      info, objv, y);
  } else {
    err_msg = "requires account id or name";
    return -EINVAL;
  }
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  Formatter* formatter = flusher.get_formatter();

  std::string marker = op_state.marker;
  int left = std::numeric_limits<int>::max();
  bool truncated = true;

  // if max-entries is given, wrap the keys in another 'result' section to
  // include the 'truncated' flag. otherwise, just list all keys
  if (op_state.max_entries) {
    left = *op_state.max_entries;
    formatter->open_object_section("result");
  }
  formatter->open_array_section("keys");

  do {
    std::vector<rgw_user> users;
    int max_entries = std::min(left, 1000);
    ret = store->list_account_users(dpp, info.id, marker, max_entries,
                                    &truncated, users, y);
    if (ret < 0) {
      return ret;
    }
    for (const auto& u : users) {
      encode_json("user", u, formatter);
    }
    if (!users.empty()) {
      marker = users.back().to_str();
    }
    left -= users.size();
  } while (truncated && left > 0);

  formatter->close_section(); // keys
  if (op_state.max_entries) {
    encode_json("truncated", truncated, formatter);
    formatter->close_section(); // result
  }
  flusher.flush();
  return 0;
}
