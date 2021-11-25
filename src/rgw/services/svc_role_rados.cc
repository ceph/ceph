#include "svc_role_rados.h"
#include "svc_meta_be_sobj.h"
#include "svc_meta.h"
#include "rgw_role.h"
#include "rgw_zone.h"
#include "svc_zone.h"
#include "rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

class RGWSI_Role_Module : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Role_RADOS::Svc& svc;
  const std::string prefix;
public:
  RGWSI_Role_Module(RGWSI_Role_RADOS::Svc& _svc): RGWSI_MBSObj_Handler_Module("roles"),
                                                  svc(_svc),
                                                  prefix(role_oid_prefix) {}

  void get_pool_and_oid(const std::string& key,
                        rgw_pool *pool,
                        std::string *oid) override
  {
    if (pool) {
      *pool = svc.zone->get_zone_params().roles_pool;
    }

    if (oid) {
      *oid = key_to_oid(key);
    }
  }

  bool is_valid_oid(const std::string& oid) override {
    return boost::algorithm::starts_with(oid, prefix);
  }

  std::string key_to_oid(const std::string& key) override {
    return prefix + key;
  }

  // This is called after `is_valid_oid` and is assumed to be a valid oid
  std::string oid_to_key(const std::string& oid) override {
    return oid.substr(prefix.size());
  }

  const std::string& get_oid_prefix() {
    return prefix;
  }
};

RGWSI_MetaBackend_Handler* RGWSI_Role_RADOS::get_be_handler()
{
  return be_handler;
}

void RGWSI_Role_RADOS::init(RGWSI_Zone *_zone_svc,
                            RGWSI_Meta *_meta_svc,
                            RGWSI_MetaBackend *_meta_be_svc,
                            RGWSI_SysObj *_sysobj_svc)
{
  svc.zone = _zone_svc;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
  svc.sysobj = _sysobj_svc;
}

int RGWSI_Role_RADOS::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{

  int r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ,
                                      &be_handler);
  if (r < 0) {
    ldout(ctx(), 0) << "ERROR: failed to create be_handler for Roles: r="
                    << r <<dendl;
    return r;
  }

  auto module = new RGWSI_Role_Module(svc);
  RGWSI_MetaBackend_Handler_SObj* bh= static_cast<RGWSI_MetaBackend_Handler_SObj *>(be_handler);
  be_module.reset(module);
  bh->set_module(module);
  return 0;
}
