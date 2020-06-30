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

int RGWAccountCtl::store_info(const RGWAccountInfo& info,
                              RGWObjVersionTracker *objv_tracker,
                              const real_time& mtime,
                              bool exclusive,
                              map<string,bufferlist> *pattrs,
                              optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
   return svc.account->store_account_info(op->ctx(),
                                          info,
                                          objv_tracker,
                                          mtime,
                                          exclusive,
                                          pattrs,
                                          y);
                          });
}

int RGWAccountCtl::read_info(const std::string& account_id,
                             RGWAccountInfo* info,
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             map<std::string, bufferlist> * pattrs,
                             optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
                            return svc.account->read_account_info(op->ctx(),
                                                                  account_id,
                                                                  info,
                                                                  objv_tracker,
                                                                  pmtime,
                                                                  pattrs,
                                                                  y
                                                                  );

                          }
    );
}

int RGWAccountCtl::remove_info(const std::string& account_id,
                               RGWObjVersionTracker *objv_tracker,
                               optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
                            return svc.account->remove_account_info(op->ctx(),
                                                                    account_id,
                                                                    objv_tracker,
                                                                    y);
                          });
}


int RGWAccountCtl::add_user(const std::string& account_id,
                            const rgw_user& user,
                            optional_yield y)
{
  RGWAccountInfo info;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;

  int ret = read_info(account_id, &info, &objv_tracker, &mtime, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  return svc.account->add_user(info, user, y);
}

int RGWAccountCtl::remove_user(const std::string& account_id,
                               const rgw_user& user_id,
                               optional_yield y)
{
  RGWAccountInfo info;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;

  int ret = read_info(account_id, &info, &objv_tracker, &mtime, nullptr, y);
  if (ret < 0) {
    return ret;
  }

  return svc.account->remove_user(info.get_id(), user_id, y);
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
                                      optional_yield y)
{
  RGWAccountCompleteInfo aci;
  RGWObjVersionTracker objv_tracker;
  real_time mtime;

  int ret = svc.account->read_account_info(op->ctx(),
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
                                RGWMDLogSyncType type) : RGWMetadataHandlerPut_SObj(_handler,
                                                                                    op,
                                                                                    entry,
                                                                                    obj,
                                                                                    objv_tracker,
                                                                                    y,
                                                                                    type),
                                                         ahandler(_handler) {
    aobj = static_cast<RGWAccountMetadataObject *>(obj);
  }

  int put_checked() override;
};

int RGWMetadataHandlerPut_Account::put_checked()
{
  RGWAccountCompleteInfo& aci = aobj->get_aci();
  map<string, bufferlist> *pattrs {nullptr};
  if (aci.has_attrs) {
    pattrs = &aci.attrs;
  }

  auto mtime = obj->get_mtime();
  int r = ahandler->svc.account->store_account_info(op->ctx(),
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
                                      RGWMDLogSyncType type)
{
  RGWMetadataHandlerPut_Account put_op(this, op, entry, obj, objv_tracker, y, type);
  return do_put_operate(&put_op);
}

int RGWAccountMetadataHandler::do_remove(RGWSI_MetaBackend_Handler::Op *op,
                                         std::string& entry,
                                         RGWObjVersionTracker& objv_tracker,
                                         optional_yield y)
{
  // TODO find out if we need to error if the key doesn't exist?
  return svc.account->remove_account_info(op->ctx(), entry, &objv_tracker, y);
}
