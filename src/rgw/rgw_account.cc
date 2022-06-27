#include "rgw_account.h"
#include "rgw_metadata.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h" // TODO
#include "common/random_string.h"
#include "common/utf8.h"

#include "services/svc_account.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"

#define dout_subsys ceph_subsys_rgw

RGWAccountCtl::RGWAccountCtl(RGWSI_Zone *zone_svc,
                             RGWSI_Account *account_svc,
                             RGWAccountMetadataHandler *_am_handler) : am_handler(_am_handler)
{
  svc.zone = zone_svc;
  svc.account = account_svc;
  be_handler = am_handler->get_be_handler();
}

RGWAccountMetadataHandler::RGWAccountMetadataHandler(RGWSI_Account *account_svc) {
  base_init(account_svc->ctx(), account_svc->get_be_handler());
  svc.account = account_svc;
}

// account ids start with 'RGW' followed by 17 numeric digits
static constexpr std::string_view account_id_prefix = "RGW";
static constexpr std::size_t account_id_len = 20;

std::string RGWAccountCtl::generate_account_id(CephContext* cct)
{
  // fill with random numeric digits
  std::string id = gen_rand_numeric(cct, account_id_len);
  // overwrite the prefix bytes
  std::copy(account_id_prefix.begin(), account_id_prefix.end(), id.begin());
  return id;
}

bool RGWAccountCtl::validate_account_id(std::string_view id,
                                        std::string& err_msg)
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

bool RGWAccountCtl::validate_account_name(std::string_view name,
                                          std::string& err_msg)
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

int RGWAccountCtl::store_info(const DoutPrefixProvider* dpp,
                              const RGWAccountInfo& info,
                              const RGWAccountInfo* old_info,
                              RGWObjVersionTracker& objv,
                              const real_time& mtime, bool exclusive,
                              std::map<std::string, bufferlist> *pattrs,
                              optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
      return svc.account->store_account_info(
          dpp, op->ctx(), info, old_info, objv, mtime, exclusive, pattrs, y);
    });
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

int RGWAccountCtl::read_by_name(const DoutPrefixProvider* dpp,
                                std::string_view tenant,
                                std::string_view name,
                                RGWAccountInfo& info,
                                RGWObjVersionTracker& objv,
                                real_time* pmtime,
                                std::map<std::string, bufferlist>* pattrs,
                                optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
      return svc.account->read_account_by_name(
          dpp, op->ctx(), tenant, name, info, objv, pmtime, pattrs, y);
    });
}

int RGWAccountCtl::read_info(const DoutPrefixProvider* dpp,
                             const std::string& account_id,
                             RGWAccountInfo& info,
                             RGWObjVersionTracker& objv,
                             real_time* pmtime,
                             std::map<std::string, bufferlist>* pattrs,
                             optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
      return svc.account->read_account_info(
          dpp, op->ctx(), account_id, info, objv, pmtime, pattrs, y);
    });
}

int RGWAccountCtl::remove_info(const DoutPrefixProvider* dpp,
                               const RGWAccountInfo& info,
                               RGWObjVersionTracker& objv,
                               optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
      return svc.account->remove_account_info(
          dpp, op->ctx(), info, objv, y);
    });
}


int RGWAccountCtl::add_user(const DoutPrefixProvider* dpp,
                            const std::string& account_id,
                            const rgw_user& user,
                            optional_yield y)
{
  RGWAccountInfo info;
  RGWObjVersionTracker objv;
  real_time mtime;

  int ret = read_info(dpp, account_id, info, objv, &mtime, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  return svc.account->add_user(dpp, info, user, y);
}

int RGWAccountCtl::remove_user(const DoutPrefixProvider* dpp,
                               const std::string& account_id,
                               const rgw_user& user_id,
                               optional_yield y)
{
  RGWAccountInfo info;
  RGWObjVersionTracker objv;

  int ret = read_info(dpp, account_id, info, objv, nullptr, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  return svc.account->remove_user(dpp, info, user_id, y);
}

int RGWAccountCtl::list_users(const DoutPrefixProvider* dpp,
                              const std::string& account_id,
                              const std::string& marker,
                              bool *more,
                              std::vector<rgw_user>& results,
                              optional_yield y)
{
  RGWAccountInfo info;
  RGWObjVersionTracker objv;

  int ret = read_info(dpp, account_id, info, objv, nullptr, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  return svc.account->list_users(dpp, info, marker, more, results, y);
}


RGWMetadataObject* RGWAccountMetadataHandler::get_meta_obj(JSONObj *jo,
                                                            const obj_version& objv,
                                                            const ceph::real_time& mtime)
{
  RGWAccountCompleteInfo aci;
  try {
    decode_json_obj(aci, jo);
  } catch (JSONDecoder::err& err) {
    return nullptr;
  }

  return new RGWAccountMetadataObject(aci, objv, mtime);
}


int RGWAccountMetadataHandler::do_get(RGWSI_MetaBackend_Handler::Op *op,
                                      std::string& entry,
                                      RGWMetadataObject **obj,
                                      optional_yield y,
                                      const DoutPrefixProvider* dpp)
{
  RGWAccountCompleteInfo aci;
  RGWObjVersionTracker objv;
  real_time mtime;

  int ret = svc.account->read_account_info(dpp, op->ctx(),
                                           entry,
                                           aci.info,
                                           objv,
                                           &mtime,
                                           &aci.attrs,
                                           y);
  if (ret < 0) {
    return ret;
  }

  *obj = new RGWAccountMetadataObject(aci, objv.read_version, mtime);
  return 0;
}

class RGWMetadataHandlerPut_Account : public RGWMetadataHandlerPut_SObj
{
  RGWAccountMetadataHandler* ahandler;
  RGWAccountMetadataObject* aobj;
public:
  RGWMetadataHandlerPut_Account(RGWAccountMetadataHandler *_handler,
                                RGWSI_MetaBackend_Handler::Op *op,
                                std::string& entry,
                                RGWMetadataObject *obj,
                                RGWObjVersionTracker& objv_tracker,
                                optional_yield y,
                                RGWMDLogSyncType type, bool from_remote_zone)
      : RGWMetadataHandlerPut_SObj(_handler, op, entry, obj, objv_tracker,
                                   y, type, from_remote_zone),
        ahandler(_handler),
        aobj(static_cast<RGWAccountMetadataObject *>(obj))
  {}

  int put_checked(const DoutPrefixProvider *dpp) override;
};

int RGWMetadataHandlerPut_Account::put_checked(const DoutPrefixProvider *dpp)
{
  auto orig_obj = static_cast<RGWAccountMetadataObject*>(old_obj);
  RGWAccountInfo *pold_info = (orig_obj ? &orig_obj->get_aci().info : nullptr);

  RGWAccountCompleteInfo& aci = aobj->get_aci();
  std::map<std::string, bufferlist> *pattrs {nullptr};
  if (aci.has_attrs) {
    pattrs = &aci.attrs;
  }

  auto mtime = obj->get_mtime();
  int r = ahandler->svc.account->store_account_info(dpp, op->ctx(),
                                                    aci.info,
                                                    pold_info,
                                                    objv_tracker,
                                                    mtime,
                                                    false,
                                                    pattrs,
                                                    y);
  if (r < 0) {
    return r;
  }

  return STATUS_APPLIED;
}

int RGWAccountMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op,
                                      std::string& entry,
                                      RGWMetadataObject *obj,
                                      RGWObjVersionTracker& objv_tracker,
                                      optional_yield y,
                                      const DoutPrefixProvider* dpp,
                                      RGWMDLogSyncType type,
                                      bool from_remote_zone)
{
  RGWMetadataHandlerPut_Account put_op(this, op, entry, obj, objv_tracker, y, type, from_remote_zone);
  return do_put_operate(&put_op, dpp);
}

int RGWAccountMetadataHandler::do_remove(RGWSI_MetaBackend_Handler::Op *op,
                                         std::string& entry,
                                         RGWObjVersionTracker& objv,
                                         optional_yield y,
                                         const DoutPrefixProvider* dpp)
{
  const std::string account_id = entry;

  RGWAccountInfo info;
  int ret = svc.account->read_account_info(dpp, op->ctx(), account_id, info,
                                           objv, nullptr, nullptr, y);
  if (ret < 0) {
    return ret;
  }
  return svc.account->remove_account_info(dpp, op->ctx(), info, objv, y);
}

int RGWAdminOp_Account::create(const DoutPrefixProvider *dpp,
                               rgw::sal::Store *store,
                               RGWAccountAdminOpState& op_state,
                               std::string& err_msg,
                               RGWFormatterFlusher& flusher,
                               optional_yield y)
{
  auto account_ctl = static_cast<rgw::sal::RadosStore*>(store)->ctl()->account;

  // account name is required
  if (!op_state.has_account_name()) {
    err_msg = "requires an account name";
    return -EINVAL;
  }
  if (!account_ctl->validate_account_name(op_state.account_name, err_msg)) {
    return -EINVAL;
  }

  RGWAccountInfo info;
  info.tenant = op_state.tenant;
  info.name = op_state.account_name;

  // account id is optional, but must be valid
  if (!op_state.has_account_id()) {
    info.id = account_ctl->generate_account_id(dpp->get_cct());
  } else if (!account_ctl->validate_account_id(op_state.account_id, err_msg)) {
    return -EINVAL;
  } else {
    info.id = op_state.account_id;
  }

  constexpr RGWAccountInfo* old_info = nullptr;
  constexpr bool exclusive = true;

  int ret = account_ctl->store_info(dpp, info, old_info,
                                    op_state.objv_tracker, real_time(),
                                    exclusive, nullptr, y);
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
  auto account_ctl = static_cast<rgw::sal::RadosStore*>(store)->ctl()->account;

  int ret = 0;
  RGWAccountInfo info;
  auto& objv = op_state.objv_tracker;
  if (op_state.has_account_id()) {
    ret = account_ctl->read_info(dpp, op_state.account_id, info,
                                 objv, nullptr, nullptr, y);
  } else if (op_state.has_account_name()) {
    ret = account_ctl->read_by_name(dpp, op_state.tenant, op_state.account_name,
                                    info, objv, nullptr, nullptr, y);
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
    if (!account_ctl->validate_account_name(op_state.account_name, err_msg)) {
      return -EINVAL;
    }
    info.name = op_state.account_name;
  }

  constexpr bool exclusive = false;

  ret = account_ctl->store_info(dpp, info, &old_info,
                                op_state.objv_tracker, real_time(),
                                exclusive, nullptr, y);
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

  // TODO: use sal::Account
  auto account_svc = static_cast<rgw::sal::RadosStore*>(store)->ctl()->account;

  auto& objv = op_state.objv_tracker;
  if (op_state.has_account_id()) {
    ret = account_svc->read_info(dpp, op_state.account_id, info,
                                 objv, nullptr, nullptr, y);
  } else if (op_state.has_account_name()) {
    ret = account_svc->read_by_name(dpp, op_state.tenant, op_state.account_name,
                                    info, objv, nullptr, nullptr, y);
  } else {
    err_msg = "requires account id or name";
    return -EINVAL;
  }
  if (ret < 0) {
    return ret;
  }

  return account_svc->remove_info(dpp, info, objv, y);
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

  // TODO: use sal::Account
  auto account_svc = static_cast<rgw::sal::RadosStore*>(store)->ctl()->account;

  auto& objv = op_state.objv_tracker;
  if (op_state.has_account_id()) {
    ret = account_svc->read_info(dpp, op_state.account_id, info,
                                 objv, nullptr, nullptr, y);
  } else if (op_state.has_account_name()) {
    ret = account_svc->read_by_name(dpp, op_state.tenant, op_state.account_name,
                                    info, objv, nullptr, nullptr, y);
  } else {
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
