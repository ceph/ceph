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

class PutRole
{
  RGWSI_Role_RADOS* svc_role;
  RGWSI_MetaBackend::Context *ctx;
  rgw::sal::RGWRole& info;
  RGWObjVersionTracker *objv_tracker;
  const real_time& mtime;
  bool exclusive;
  std::map<std::string, bufferlist> *pattrs;
  optional_yield y;
  const DoutPrefixProvider *dpp;

public:
  PutRole(RGWSI_Role_RADOS* _svc,
          RGWSI_MetaBackend::Context *_ctx,
          rgw::sal::RGWRole& _info,
          RGWObjVersionTracker *_ot,
          const real_time& _mtime,
          bool _exclusive,
          std::map<std::string, bufferlist> *_pattrs,
          optional_yield _y,
          const DoutPrefixProvider *dpp) :
    svc_role(_svc), ctx(_ctx), info(_info), objv_tracker(_ot),
    mtime(_mtime), exclusive(_exclusive), pattrs(_pattrs), y(_y), dpp(dpp)
  {}

  int prepare() {
    if (exclusive) {
      // TODO replace this with a stat call instead we don't really need to read
      // the values here
      real_time _mtime;
      std::string name = info.get_name();
      std::string tenant = info.get_tenant();
      std::string id = info.get_id();

      int ret = svc_role->read_name(ctx, name, tenant, id,
                                    objv_tracker, &_mtime, y, dpp);
      if (ret == 0) {
        ldout(svc_role->ctx(), 0) << "ERROR: name " << info.get_name()
                                  << " already in use for role id "
                                  << info.get_id() << dendl;
        return -EEXIST;
      }
    }


    return 0;
  }

  int put() {
    return svc_role->store_info(ctx, info, objv_tracker,
                                mtime, exclusive, pattrs, y, dpp);
  }

  int complete() {
    // This creates the ancillary objects for roles, role_names & path
    // objv_tracker is set to null for sysobj calls as these aren't tracked via
    // the mdlog. A failure in name/path creation means we delete all the
    // created objects in the transaction
    int r = svc_role->store_name(ctx, info.get_id(), info.get_name(), info.get_tenant(),
                                 nullptr, mtime, exclusive, y, dpp);

    if (r == 0) {
      r = svc_role->store_path(ctx, info.get_id(), info.get_path(), info.get_tenant(),
                               nullptr, mtime, exclusive, y, dpp);
    }

    if (r < 0) {
      svc_role->delete_info(ctx, info, objv_tracker, y, dpp);
      svc_role->delete_name(ctx, info.get_name(), info.get_tenant(), objv_tracker, y, dpp);
    }

    return r;
  }

};


int RGWSI_Role_RADOS::create(RGWSI_MetaBackend::Context *ctx,
                             rgw::sal::RGWRole& info,
                             RGWObjVersionTracker * const objv_tracker,
                             const real_time& mtime,
                             bool exclusive,
                             std::map<std::string, bufferlist> * pattrs,
                             optional_yield y,
                             const DoutPrefixProvider *dpp)
{
  PutRole Op(this, ctx, info, objv_tracker, mtime, exclusive, pattrs, y, dpp);

  int r = Op.prepare();
  if (r < 0) {
    ldout(svc.meta_be->ctx(),0) << __func__ << "ERROR: prepare role failed" << dendl;
    return r;
  }

  r = Op.put();
  if (r < 0) {
    ldout(svc.meta_be->ctx(),0) << __func__ << "ERROR: put role failed" << dendl;
    return r;
  }

  r = Op.complete();
  if (r < 0) {
    ldout(svc.meta_be->ctx(),0) << __func__ << "ERROR: completing PutRole failed" << dendl;
  }
  return r;
}

int RGWSI_Role_RADOS::store_info(RGWSI_MetaBackend::Context *ctx,
                                 const rgw::sal::RGWRole& role,
                                 RGWObjVersionTracker * const objv_tracker,
                                 const real_time& mtime,
                                 bool exclusive,
                                 std::map<std::string, bufferlist> * pattrs,
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp)
{
  bufferlist data_bl;
  encode(role, data_bl);
  RGWSI_MBSObj_PutParams params(data_bl, pattrs, mtime, exclusive);

  return svc.meta_be->put(ctx, get_role_meta_key(role.get_id()), params, objv_tracker, y, dpp);
}

int RGWSI_Role_RADOS::store_name(RGWSI_MetaBackend::Context *ctx,
                                 const std::string& role_id,
                                 const std::string& name,
                                 const std::string& tenant,
                                 RGWObjVersionTracker * const objv_tracker,
                                 const real_time& mtime,
                                 bool exclusive,
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp)
{
  RGWNameToId nameToId;
  nameToId.obj_id = role_id;

  bufferlist data_bl;
  encode(nameToId, data_bl);

  RGWSI_MetaBackend_SObj::Context_SObj *sys_ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(ctx);
  auto& obj_ctx = *sys_ctx->obj_ctx;
  return rgw_put_system_obj(dpp, obj_ctx,
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
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp)
{
  bufferlist bl;
  RGWSI_MetaBackend_SObj::Context_SObj *sys_ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(ctx);
  auto& obj_ctx = *sys_ctx->obj_ctx;
  return rgw_put_system_obj(dpp, obj_ctx,
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
                                rgw::sal::RGWRole *role,
                                RGWObjVersionTracker * const objv_tracker,
                                real_time * const pmtime,
                                std::map<std::string, bufferlist> * pattrs,
                                optional_yield y,
                                const DoutPrefixProvider *dpp)
{
  bufferlist data_bl;
  RGWSI_MBSObj_GetParams params(&data_bl, pattrs, pmtime);

  int r = svc.meta_be->get_entry(ctx, get_role_meta_key(role_id), params, objv_tracker, y, dpp);
  if (r < 0)
    return r;

  auto bl_iter = data_bl.cbegin();
  try  {
    decode(*role, bl_iter);
  } catch (buffer::error& err) {
    ldout(svc.meta_be->ctx(),0) << "ERROR: failed to decode RGWRole, caught buffer::err " << dendl;
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
                                optional_yield y,
                                const DoutPrefixProvider *dpp)
{

  bufferlist data_bl;
  RGWSI_MetaBackend_SObj::Context_SObj *sys_ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(ctx);
  int r = rgw_get_system_obj(*sys_ctx->obj_ctx,
                             svc.zone->get_zone_params().roles_pool,
                             get_role_name_meta_key(name, tenant),
                             data_bl,
                             nullptr,
                             pmtime,
                             y,
                             dpp);
  if (r < 0)
    return r;

  auto bl_iter = data_bl.cbegin();
  RGWNameToId nameToId;
  try  {
    decode(nameToId, bl_iter);
  } catch (buffer::error& err) {
    ldout(svc.meta_be->ctx(),0) << "ERROR: failed to decode RGWRole name, caught buffer::err " << dendl;
    return -EIO;
  }

  role_id = nameToId.obj_id;
  return 0;
}

static int delete_oid(RGWSI_Role_RADOS::Svc svc,
                      RGWSI_MetaBackend::Context *ctx,
                      const std::string& oid,
                      RGWObjVersionTracker * const objv_tracker,
                      optional_yield y,
                      const DoutPrefixProvider *dpp)
{
  RGWSI_MetaBackend_SObj::Context_SObj *sys_ctx = static_cast<RGWSI_MetaBackend_SObj::Context_SObj *>(ctx);
  rgw_raw_obj obj(svc.zone->get_zone_params().roles_pool, oid);
  auto sysobj = sys_ctx->obj_ctx->get_obj(obj);
  int r =  sysobj.wop().remove(dpp, y);
  if (r < 0 && r != -ENOENT && r != -ECANCELED) {
    ldout(svc.meta_be->ctx(),0) << "ERROR: RGWSI_Role: could not remove oid = "
                            << oid << " r = "<< r << dendl;
    return r;
  }
  return 0;
}

int RGWSI_Role_RADOS::delete_info(RGWSI_MetaBackend::Context *ctx,
                                  const rgw::sal::RGWRole& info,
                                  RGWObjVersionTracker * const objv_tracker,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp)
{
  RGWSI_MBSObj_RemoveParams params;
  int r = svc.meta_be->remove(ctx, info.get_id(), params, objv_tracker, y, dpp);
  if (r < 0 && r != -ENOENT && r != -ECANCELED) {
    ldout(svc.meta_be->ctx(),0) << "ERROR: RGWSI_Role: could not remove oid = "
                            << info.get_id() << " r = "<< r << dendl;
  }

  return r;
}

int RGWSI_Role_RADOS::delete_name(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& name,
                                  const std::string& tenant,
                                  RGWObjVersionTracker * const objv_tracker,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp)
{
  return delete_oid(svc, ctx, get_role_name_meta_key(name, tenant),
                    objv_tracker, y, dpp);

}

int RGWSI_Role_RADOS::delete_path(RGWSI_MetaBackend::Context *ctx,
                                  const std::string& role_id,
                                  const std::string& path,
                                  const std::string& tenant,
                                  RGWObjVersionTracker * const objv_tracker,
                                  optional_yield y,
                                  const DoutPrefixProvider *dpp)
{
  return delete_oid(svc, ctx, get_role_path_meta_key(path, role_id, tenant),
                    objv_tracker, y, dpp);

}
