// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <ctime>
#include <regex>
#include <boost/algorithm/string/replace.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"
#include "rgw_zone.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_role_metadata.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_meta.h"
#include "services/svc_role_rados.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw { namespace sal {

RGWRoleMetadataHandler::RGWRoleMetadataHandler(Driver* driver,
                                              RGWSI_Role_RADOS *role_svc)
{
  this->driver = driver;
  base_init(role_svc->ctx(), role_svc->get_be_handler());
}

RGWMetadataObject *RGWRoleMetadataHandler::get_meta_obj(JSONObj *jo,
							const obj_version& objv,
							const ceph::real_time& mtime)
{
  RGWRoleInfo info;

  try {
    info.decode_json(jo);
  } catch (JSONDecoder:: err& e) {
    return nullptr;
  }

  return new RGWRoleMetadataObject(info, objv, mtime, driver);
}

int RGWRoleMetadataHandler::do_get(RGWSI_MetaBackend_Handler::Op *op,
                                   std::string& entry,
                                   RGWMetadataObject **obj,
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(entry);
  int ret = role->read_info(dpp, y);
  if (ret < 0) {
    return ret;
  }

  RGWObjVersionTracker objv_tracker = role->get_objv_tracker();
  real_time mtime = role->get_mtime();

  RGWRoleInfo info = role->get_info();
  RGWRoleMetadataObject *rdo = new RGWRoleMetadataObject(info, objv_tracker.read_version,
                                                         mtime, driver);
  *obj = rdo;

  return 0;
}

int RGWRoleMetadataHandler::do_remove(RGWSI_MetaBackend_Handler::Op *op,
                                      std::string& entry,
                                      RGWObjVersionTracker& objv_tracker,
                                      optional_yield y,
                                      const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(entry);
  int ret = role->read_info(dpp, y);
  if (ret < 0) {
    return ret == -ENOENT? 0 : ret;
  }

  return role->delete_obj(dpp, y);
}

class RGWMetadataHandlerPut_Role : public RGWMetadataHandlerPut_SObj
{
  RGWRoleMetadataHandler *rhandler;
  RGWRoleMetadataObject *mdo;
public:
  RGWMetadataHandlerPut_Role(RGWRoleMetadataHandler *handler,
                             RGWSI_MetaBackend_Handler::Op *op,
                             std::string& entry,
                             RGWMetadataObject *obj,
                             RGWObjVersionTracker& objv_tracker,
                             optional_yield y,
                             RGWMDLogSyncType type,
                             bool from_remote_zone) :
    RGWMetadataHandlerPut_SObj(handler, op, entry, obj, objv_tracker, y, type, from_remote_zone),
    rhandler(handler) {
    mdo = static_cast<RGWRoleMetadataObject*>(obj);
  }

  int put_checked(const DoutPrefixProvider *dpp) override {
    auto& info = mdo->get_role_info();
    auto mtime = mdo->get_mtime();
    auto* driver = mdo->get_driver();
    info.mtime = mtime;
    std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(info);
    int ret = role->create(dpp, true, info.id, y);
    if (ret == -EEXIST) {
      ret = role->update(dpp, y);
    }

    return ret < 0 ? ret : STATUS_APPLIED;
  }
};

int RGWRoleMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op,
                                   std::string& entry,
                                   RGWMetadataObject *obj,
                                   RGWObjVersionTracker& objv_tracker,
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp,
                                   RGWMDLogSyncType type,
                                   bool from_remote_zone)
{
  RGWMetadataHandlerPut_Role put_op(this, op , entry, obj, objv_tracker, y, type, from_remote_zone);
  return do_put_operate(&put_op, dpp);
}

} } // namespace rgw::sal
