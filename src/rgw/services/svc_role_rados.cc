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

int RGWSI_Role_RADOS::do_start()
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

class PutRole
{
  RGWSI_Role_RADOS* svc_role;
  RGWSI_MetaBackend::Context *ctx;
  RGWRoleInfo& info;
  RGWObjVersionTracker *objv_tracker;
  const real_time& mtime;
  bool exclusive;
  map<std::string, bufferlist> *pattrs;
  optional_yield y;

public:
  PutRole(RGWSI_Role_RADOS* _svc,
          RGWSI_MetaBackend::Context *_ctx,
          RGWRoleInfo& _info,
          RGWObjVersionTracker *_ot,
          const real_time& _mtime,
          bool _exclusive,
          map<std::string, bufferlist> *_pattrs,
          optional_yield _y) :
    svc_role(_svc), ctx(_ctx), info(_info), objv_tracker(_ot),
    mtime(_mtime), exclusive(_exclusive), pattrs(_pattrs), y(_y)
  {}

  int prepare() {
    if (exclusive) {
      // TODO replace this with a stat call instead we don't really need to read
      // the values here
      ceph::real_time _mtime;
      int ret = svc_role->read_name(ctx, info.name, info.tenant, info.id,
                                    objv_tracker, &_mtime, y);
      if (ret == 0) {
        ldout(svc_role->ctx(), 0) << "ERROR: name " << info.name
                                  << " already in use for role id "
                                  << info.id << dendl;
        return -EEXIST;
      }
    }


    return 0;
  }

  int put() {
    return svc_role->store_info(ctx, info, objv_tracker,
                                mtime, exclusive, pattrs, y);
  }

  int complete() {
    int r = svc_role->store_name(ctx, info.id, info.name, info.tenant,
                                 objv_tracker, mtime, exclusive, y);

    if (r == 0) {
      r = svc_role->store_path(ctx, info.id, info.path, info.tenant,
                               objv_tracker, mtime, exclusive, y);
    }

    if (r < 0) {
      svc_role->delete_info(ctx, info, objv_tracker, y);
      svc_role->delete_name(ctx, info.name, info.tenant, objv_tracker, y);
    }

    return r;
  }

};


int RGWSI_Role_RADOS::create(RGWSI_MetaBackend::Context *ctx,
                             RGWRoleInfo& info,
                             RGWObjVersionTracker * const objv_tracker,
                             const real_time& mtime,
                             bool exclusive,
                             map<std::string, bufferlist> * pattrs,
                             optional_yield y)
{
  PutRole Op(this, ctx, info, objv_tracker, mtime, exclusive, pattrs, y);

  int r = Op.prepare();
  if (r < 0) {
    return r;
  }

  r = Op.put();
  if (r < 0) {
    return r;
  }

  return Op.complete();
}

int RGWSI_Role_RADOS::store_info(RGWSI_MetaBackend::Context *ctx,
                                 const RGWRoleInfo& info,
                                 RGWObjVersionTracker * const objv_tracker,
                                 const real_time& mtime,
                                 bool exclusive,
                                 map<std::string, bufferlist> * pattrs,
                                 optional_yield y)
{
  bufferlist data_bl;
  encode(info, data_bl);
  RGWSI_MBSObj_PutParams params(data_bl, pattrs, mtime, exclusive);
  return svc.meta_be->put(ctx, info.id,
                          params, objv_tracker, y);

}

int RGWSI_Role_RADOS::store_name(RGWSI_MetaBackend::Context *ctx,
                                 const std::string& role_id,
                                 const std::string& name,
                                 const std::string& tenant,
                                 RGWObjVersionTracker * const objv_tracker,
                                 const real_time& mtime,
                                 bool exclusive,
                                 optional_yield y)
{
  RGWNameToId nameToId;
  nameToId.obj_id = role_id;

  bufferlist data_bl;
  encode(nameToId, data_bl);

  RGWSI_MetaBackend_SObj::Context_SObj *sys_ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(ctx);
  return rgw_put_system_obj(*sys_ctx->obj_ctx,
                            svc.zone->get_zone_params().roles_pool,
                            get_role_name_meta_key(name, tenant),
                            data_bl,
                            exclusive,
                            objv_tracker,
                            mtime,
                            y);
}


int RGWSI_Role_RADOS::store_path(RGWSI_MetaBackend::Context *ctx,
                                 const std::string& role_id,
                                 const std::string& path,
                                 const std::string& tenant,
                                 RGWObjVersionTracker * const objv_tracker,
                                 const real_time& mtime,
                                 bool exclusive,
                                 optional_yield y)
{
  bufferlist bl;
  RGWSI_MetaBackend_SObj::Context_SObj *sys_ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(ctx);
  return rgw_put_system_obj(*sys_ctx->obj_ctx,
                            svc.zone->get_zone_params().roles_pool,
                            get_role_path_meta_key(path, role_id, tenant),
                            bl,
                            exclusive,
                            objv_tracker,
                            mtime,
                            y);
}


int RGWSI_Role_RADOS::read_info(RGWSI_MetaBackend::Context *ctx,
                                const std::string& role_id,
                                RGWRoleInfo *role,
                                RGWObjVersionTracker * const objv_tracker,
                                real_time * const pmtime,
                                map<std::string, bufferlist> * pattrs,
                                optional_yield y)
{
  bufferlist data_bl;
  RGWSI_MBSObj_GetParams params(&data_bl, pattrs, pmtime);

  int r = svc.meta_be->get_entry(ctx, role_id, params, objv_tracker, y);
  if (r < 0)
    return r;

  auto bl_iter = data_bl.cbegin();
  try  {
    decode(*role, bl_iter);
  } catch (buffer::error& err) {
    ldout(svc.meta_be->ctx(),0) << "ERROR: failed to decode RGWRoleInfo, caught buffer::err " << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSI_Role_RADOS::read_name(RGWSI_MetaBackend::Context *ctx,
                                const std::string& name,
                                const std::string& tenant,
                                std::string& role_id,
                                RGWObjVersionTracker * const objv_tracker,
                                real_time * const pmtime,
                                optional_yield y)
{

  bufferlist data_bl;
  RGWSI_MetaBackend_SObj::Context_SObj *sys_ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(ctx);
  int r = rgw_get_system_obj(*sys_ctx->obj_ctx,
                             svc.zone->get_zone_params().roles_pool,
                             get_role_name_meta_key(name, tenant),
                             data_bl,
                             nullptr,
                             pmtime,
                             y);
  if (r < 0)
    return r;

  auto bl_iter = data_bl.cbegin();
  RGWNameToId nameToId;
  try  {
    decode(nameToId, bl_iter);
  } catch (buffer::error& err) {
    ldout(svc.meta_be->ctx(),0) << "ERROR: failed to decode RGWRoleInfo name, caught buffer::err " << dendl;
    return -EIO;
  }

  role_id = nameToId.obj_id;
  return 0;
}

static int delete_oid(RGWSI_Role_RADOS::Svc svc,
                      RGWSI_MetaBackend::Context *ctx,
                      const std::string& oid,
                      RGWObjVersionTracker * const objv_tracker,
                      optional_yield y)
{
  RGWSI_MetaBackend_SObj::Context_SObj *sys_ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(ctx);
  rgw_raw_obj obj(svc.zone->get_zone_params().roles_pool, oid);
  auto sysobj = sys_ctx->obj_ctx->get_obj(obj);
  int r =  sysobj.wop().remove(y);
  if (r < 0 && r != -ENOENT && r != -ECANCELED) {
    ldout(svc.meta_be->ctx(),0) << "ERROR: RGWSI_Role: could not remove oid = "
                            << oid << " r = "<< r << dendl;
    return r;
  }
  return 0;
}

int RGWSI_Role_RADOS::delete_info(RGWSI_MetaBackend::Context *ctx,
                                   const RGWRoleInfo& info,
                                   RGWObjVersionTracker * const objv_tracker,
                                   optional_yield y)
{
  RGWSI_MBSObj_RemoveParams params;
  int r = svc.meta_be->remove(ctx, info.id, params, objv_tracker, y);
  if (r < 0 && r != -ENOENT && r != -ECANCELED) {
    ldout(svc.meta_be->ctx(),0) << "ERROR: RGWSI_Role: could not remove oid = "
                            << info.id << " r = "<< r << dendl;
  }

  return r;
}

int RGWSI_Role_RADOS::delete_name(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& name,
                                  const std::string& tenant,
                                  RGWObjVersionTracker * const objv_tracker,
                                  optional_yield y)
{
  return delete_oid(svc, ctx, get_role_name_meta_key(name, tenant),
                    objv_tracker, y);

}

int RGWSI_Role_RADOS::delete_path(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& role_id,
                                  const std::string& path,
                                  const std::string& tenant,
                                  RGWObjVersionTracker * const objv_tracker,
                                  optional_yield y)
{
  return delete_oid(svc, ctx, get_role_path_meta_key(path, role_id, tenant),
                    objv_tracker, y);

}
