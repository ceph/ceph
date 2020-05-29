#include "svc_account_rados.h"
#include "svc_sys_obj.h"
#include "svc_meta_be_sobj.h"
#include "svc_meta.h"
#include "rgw/rgw_account.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_zone.h"
#include "svc_zone.h"


RGWSI_Account_RADOS::RGWSI_Account_RADOS(CephContext *cct) :
  RGWSI_Account(cct) {
}

void RGWSI_Account_RADOS::init(RGWSI_Zone *_zone_svc,
                               RGWSI_Meta *_meta_svc,
                               RGWSI_MetaBackend *_meta_be_svc)
{
  svc.zone = _zone_svc;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
}

int RGWSI_Account_RADOS::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{
  return svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ,
                                     &be_handler);
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

  int r = svc.meta_be->put(_ctx, get_meta_key(info), params, objv_tracker, y, dpp);
  if (r < 0)
    return r;
  RGWSI_MetaBackend_SObj::Context_SObj *ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(_ctx);

  auto obj_ctx = ctx->obj_ctx;
  return rgw_put_system_obj(dpp, *obj_ctx, svc.zone->get_zone_params().user_swift_pool,
                            info.get_id(), data_bl, exclusive, nullptr, real_time(), y);
}
