// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>

#include "common/async/yield_context.h"

#include "common/ceph_json.h"
#include "common/ceph_context.h"
#include "rgw_rados.h"
#include "rgw_metadata.h"
#include "rgw_role.h"

namespace rgw { namespace sal {

class RGWRoleMetadataObject: public RGWMetadataObject {
  RGWRoleInfo info;
  Driver* driver;
public:
  RGWRoleMetadataObject() = default;
  RGWRoleMetadataObject(RGWRoleInfo& info,
			const obj_version& v,
			real_time m,
      Driver* driver) : RGWMetadataObject(v,m), info(info), driver(driver) {}

  void dump(Formatter *f) const override {
    info.dump(f);
  }

  RGWRoleInfo& get_role_info() {
    return info;
  }

  Driver* get_driver() {
    return driver;
  }
};

class RGWRoleMetadataHandler: public RGWMetadataHandler_GenericMetaBE
{
public:
  RGWRoleMetadataHandler(Driver* driver, RGWSI_Role_RADOS *role_svc);

  std::string get_type() final { return "roles";  }

  RGWMetadataObject *get_meta_obj(JSONObj *jo,
				  const obj_version& objv,
				  const ceph::real_time& mtime);

  int do_get(RGWSI_MetaBackend_Handler::Op *op,
	     std::string& entry,
	     RGWMetadataObject **obj,
	     optional_yield y,
       const DoutPrefixProvider *dpp) final;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op,
		std::string& entry,
		RGWObjVersionTracker& objv_tracker,
		optional_yield y,
    const DoutPrefixProvider *dpp) final;

  int do_put(RGWSI_MetaBackend_Handler::Op *op,
	     std::string& entr,
	     RGWMetadataObject *obj,
	     RGWObjVersionTracker& objv_tracker,
	     optional_yield y,
       const DoutPrefixProvider *dpp,
	     RGWMDLogSyncType type,
       bool from_remote_zone) override;

private:
  Driver* driver;
};
} } // namespace rgw::sal
