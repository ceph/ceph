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
  if (strcasecmp(s.c_str(), "background") == 0) {
    return context::background;
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
    case context::background:
      return "background";
    case context::none:
      break;
  }
  return "none";
}

bool verify(const std::string& script, std::string& err_msg) 
{
  lua_State *L = luaL_newstate();
  lua_state_guard guard(L);
  open_standard_libs(L);
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

std::string script_oid(context ctx, const std::string& tenant) {
  static const std::string SCRIPT_OID_PREFIX("script.");
  return SCRIPT_OID_PREFIX + to_string(ctx) + "." + tenant;
}


int read_script(const DoutPrefixProvider *dpp, rgw::sal::Store* store, const std::string& tenant, optional_yield y, context ctx, std::string& script)
{
  auto lua_mgr = store->get_lua_manager();

  return lua_mgr->get_script(dpp, y, script_oid(ctx, tenant), script);
}

int write_script(const DoutPrefixProvider *dpp, rgw::sal::Store* store, const std::string& tenant, optional_yield y, context ctx, const std::string& script)
{
  auto lua_mgr = store->get_lua_manager();

  return lua_mgr->put_script(dpp, y, script_oid(ctx, tenant), script);
}

int delete_script(const DoutPrefixProvider *dpp, rgw::sal::Store* store, const std::string& tenant, optional_yield y, context ctx)
{
  auto lua_mgr = store->get_lua_manager();

  return lua_mgr->del_script(dpp, y, script_oid(ctx, tenant));
}

int add_package(const DoutPrefixProvider *dpp, rgw::sal::Store* store, optional_yield y, const std::string& package_name, bool allow_compilation)
{
  auto lua_mgr = store->get_lua_manager();

  return lua_mgr->add_package(dpp, y, package_name, allow_compilation);
}

int remove_package(const DoutPrefixProvider *dpp, rgw::sal::Store* store, optional_yield y, const std::string& package_name)
{
  auto lua_mgr = store->get_lua_manager();

  return lua_mgr->remove_package(dpp, y, package_name);
}

int list_packages(const DoutPrefixProvider *dpp, rgw::sal::Store* store, optional_yield y, packages_t& packages)
{
  auto lua_mgr = store->get_lua_manager();

  return lua_mgr->list_packages(dpp, y, packages);
}

int install_packages(const DoutPrefixProvider *dpp, rgw::sal::Store* store, optional_yield y, packages_t& failed_packages, std::string& output)
{
  auto lua_mgr = store->get_lua_manager();

  return lua_mgr->install_packages(dpp, y, failed_packages, output);
}

}

