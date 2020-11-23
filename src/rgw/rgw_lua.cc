#include <lua.hpp>
#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "common/dout.h"
#include "rgw_lua_utils.h"
#include "rgw_sal_rados.h"
#include "rgw_lua.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua {

context to_context(const std::string& s) 
{
  if (strcasecmp(s.c_str(), "prerequest") == 0) {
    return context::preRequest;
  }
  if (strcasecmp(s.c_str(), "postrequest") == 0) {
    return context::postRequest;
  }
  return context::none;
}

std::string to_string(context ctx) 
{
  switch (ctx) {
    case context::preRequest:
      return "prerequest";
    case context::postRequest:
      return "postrequest";
    case context::none:
      break;
  }
  return "none";
}

bool verify(const std::string& script, std::string& err_msg) 
{
  lua_State *L = luaL_newstate();
  lua_state_guard guard(L);
  luaL_openlibs(L);
  try {
    if (luaL_loadstring(L, script.c_str()) != LUA_OK) {
      err_msg.assign(lua_tostring(L, -1));
      return false;
    }
  } catch (const std::runtime_error& e) {
    err_msg = e.what();
    return false;
  }
  err_msg = "";
  return true;
}

static const std::string SCRIPT_OID_PREFIX("script.");

int read_script(rgw::sal::RGWRadosStore* store, const std::string& tenant, optional_yield y, context ctx, std::string& script)
{
  RGWSysObjectCtx obj_ctx(store->svc()->sysobj->init_obj_ctx());
  RGWObjVersionTracker objv_tracker;

  const auto script_oid = SCRIPT_OID_PREFIX + to_string(ctx) + tenant;
  rgw_raw_obj obj(store->svc()->zone->get_zone_params().log_pool, script_oid);

  bufferlist bl;
  
  const auto rc = rgw_get_system_obj(
      obj_ctx,
      obj.pool, 
      obj.oid,
      bl,
      &objv_tracker,
      nullptr, 
      y, 
      nullptr, 
      nullptr);

  if (rc < 0) {
    return rc;
  }

  auto iter = bl.cbegin();
  try {
    ceph::decode(script, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  return 0;
}

int write_script(rgw::sal::RGWRadosStore* store, const std::string& tenant, optional_yield y, context ctx, const std::string& script)
{
  RGWSysObjectCtx obj_ctx(store->svc()->sysobj->init_obj_ctx());
  RGWObjVersionTracker objv_tracker;

  const auto script_oid = SCRIPT_OID_PREFIX + to_string(ctx) + tenant;
  rgw_raw_obj obj(store->svc()->zone->get_zone_params().log_pool, script_oid);

  bufferlist bl;
  ceph::encode(script, bl);

  const auto rc = rgw_put_system_obj(
      obj_ctx,
      obj.pool,
      obj.oid,
      bl,
      false,
      &objv_tracker,
      real_time(),
      y);

  if (rc < 0) {
    return rc;
  }

  return 0;
}

int delete_script(rgw::sal::RGWRadosStore* store, const std::string& tenant, optional_yield y, context ctx)
{
  RGWObjVersionTracker objv_tracker;

  const auto script_oid = SCRIPT_OID_PREFIX + to_string(ctx) + tenant;
  rgw_raw_obj obj(store->svc()->zone->get_zone_params().log_pool, script_oid);

  const auto rc = rgw_delete_system_obj(
      store->svc()->sysobj, 
      obj.pool,
      obj.oid,
      &objv_tracker,
      y);

  if (rc < 0 && rc != -ENOENT) {
    return rc;
  }

  return 0;
}
}

