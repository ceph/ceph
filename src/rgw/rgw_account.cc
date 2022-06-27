#include "rgw_account.h"
#include "rgw_metadata.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h" // TODO

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
  encode_json("max_users", max_users, f);
}

void RGWAccountInfo::decode_json(JSONObj* obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("tenant", tenant, obj);
  JSONDecoder::decode_json("max_users", max_users, obj);
}

void RGWAccountInfo::generate_test_instances(std::list<RGWAccountInfo*>& o)
{
  o.push_back(new RGWAccountInfo);
  auto p = new RGWAccountInfo;
  p->id = "account1";
  p->tenant = "tenant1";
  o.push_back(p);
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

  RGWAccountMetadataObject *mdo = new RGWAccountMetadataObject(aci, objv.read_version, mtime);
  *obj = mdo;

  return 0;
}

class RGWMetadataHandlerPut_Account : public RGWMetadataHandlerPut_SObj
{
  RGWAccountMetadataHandler *ahandler;
  RGWAccountMetadataObject *aobj;
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

int RGWAdminOp_Account::add(const DoutPrefixProvider *dpp,
                            rgw::sal::Store *store,
                            RGWAccountAdminOpState& op_state,
                            RGWFormatterFlusher& flusher,
                            optional_yield y)
{

  if (!op_state.has_account_id()) {
    return -EINVAL;
  }

  RGWAccountInfo account_info;
  account_info.tenant = op_state.tenant;
  account_info.id = op_state.account_id;

  constexpr RGWAccountInfo* old_info = nullptr;
  constexpr bool exclusive = true;

  int ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->account->store_info(
      dpp, account_info, old_info, op_state.objv_tracker, real_time(), exclusive, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  encode_json("AccountInfo", account_info, flusher.get_formatter());
  flusher.flush();

  return 0;
}

int RGWAdminOp_Account::remove(const DoutPrefixProvider *dpp,
                               rgw::sal::Store *store,
                               RGWAccountAdminOpState& op_state,
                               RGWFormatterFlusher& flusher,
                               optional_yield y)
{
  if (!op_state.has_account_id()) {
    return -EINVAL;
  }
  RGWAccountInfo info;

  // TODO: use sal::Account
  auto account_svc = static_cast<rgw::sal::RadosStore*>(store)->ctl()->account;

  auto& objv = op_state.objv_tracker;
  int ret = account_svc->read_info(dpp, op_state.account_id, info,
                                   objv, nullptr, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  return account_svc->remove_info(dpp, info, objv, y);
}

int RGWAdminOp_Account::info(const DoutPrefixProvider *dpp,
                             rgw::sal::Store *store,
                             RGWAccountAdminOpState& op_state,
                             RGWFormatterFlusher& flusher,
                             optional_yield y)
{

  if (!op_state.has_account_id()) {
    return -EINVAL;
  }
  // TODO: use sal::Account
  auto account_svc = static_cast<rgw::sal::RadosStore*>(store)->ctl()->account;

  RGWAccountInfo info;
  auto& objv = op_state.objv_tracker;
  int ret = account_svc->read_info(dpp, op_state.account_id, info,
                                   objv, nullptr, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  flusher.start(0);
  encode_json("AccountInfo", info, flusher.get_formatter());
  flusher.flush();

  return 0;
}
