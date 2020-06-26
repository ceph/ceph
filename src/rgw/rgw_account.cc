#include "rgw_account.h"
#include "rgw_metadata.h"

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
                              RGWObjVersionTracker *objv_tracker,
                              const real_time& mtime,
                              bool exclusive,
                              std::map<std::string, bufferlist> *pattrs,
                              optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
   return svc.account->store_account_info(dpp, op->ctx(),
                                          info,
                                          objv_tracker,
                                          mtime,
                                          exclusive,
                                          pattrs,
                                          y);
                          });
}

void AccountQuota::dump(Formatter * const f) const
{
  f->dump_unsigned("max_users", max_users);
  f->dump_unsigned("max_roles", max_roles);
}

void AccountQuota::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("max_users", max_users, obj);
  JSONDecoder::decode_json("max_roles", max_roles, obj);
}

void RGWAccountInfo::dump(Formatter * const f) const
{
  encode_json("id", id, f);
  encode_json("tenant", tenant, f);
  encode_json("AccountQuota", account_quota, f);
}

void RGWAccountInfo::decode_json(JSONObj* obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("tenant", tenant, obj);
  JSONDecoder::decode_json("quota", account_quota, obj);
}

void RGWAccountInfo::generate_test_instances(std::list<RGWAccountInfo*>& o)
{
  o.push_back(new RGWAccountInfo("tenant1","account1"));
}

int RGWAccountCtl::read_info(const DoutPrefixProvider* dpp,
                             const std::string& account_id,
                             RGWAccountInfo* info,
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             std::map<std::string, bufferlist> * pattrs,
                             optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
        return svc.account->read_account_info(dpp, op->ctx(), account_id,
                                              info, objv_tracker, pmtime,
                                              pattrs, y);

      }
    );
}

int RGWAccountCtl::remove_info(const DoutPrefixProvider* dpp,
                               const std::string& account_id,
                               RGWObjVersionTracker *objv_tracker,
                               optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
      return svc.account->remove_account_info(dpp, op->ctx(), account_id,
                                              objv_tracker, y);
    });
}


int RGWAccountCtl::add_user(const DoutPrefixProvider* dpp,
                            const std::string& account_id,
                            const rgw_user& user,
                            optional_yield y)
{
  RGWAccountInfo info;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;

  int ret = read_info(dpp, account_id, &info, &objv_tracker, &mtime, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  return svc.account->add_user(dpp, info, user, y);
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
  RGWObjVersionTracker objv_tracker;
  real_time mtime;

  int ret = svc.account->read_account_info(dpp, op->ctx(),
                                           entry,
                                           &aci.info,
                                           &objv_tracker,
                                           &mtime,
                                           &aci.attrs,
                                           y);
  if (ret < 0) {
    return ret;
  }

  RGWAccountMetadataObject *mdo = new RGWAccountMetadataObject(aci, objv_tracker.read_version,mtime);
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
  RGWAccountCompleteInfo& aci = aobj->get_aci();
  std::map<std::string, bufferlist> *pattrs {nullptr};
  if (aci.has_attrs) {
    pattrs = &aci.attrs;
  }

  auto mtime = obj->get_mtime();
  int r = ahandler->svc.account->store_account_info(dpp, op->ctx(),
                                                    aci.info,
                                                    &objv_tracker,
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
                                         RGWObjVersionTracker& objv_tracker,
                                         optional_yield y,
                                         const DoutPrefixProvider* dpp)
{
  // TODO find out if we need to error if the key doesn't exist?
  return svc.account->remove_account_info(dpp, op->ctx(), entry, &objv_tracker, y);
}
