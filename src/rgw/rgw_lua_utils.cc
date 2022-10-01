#include <string>
#include <lua.hpp>
#include "common/ceph_context.h"
#include "common/dout.h"
#include "rgw_lua_utils.h"
#include "rgw_lua_version.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua {

// TODO - add the folowing generic functions
// lua_push(lua_State* L, const std::string& str)
// template<typename T> lua_push(lua_State* L, const std::optional<T>& val)
// lua_push(lua_State* L, const ceph::real_time& tp)

constexpr const char* RGWDebugLogAction{"RGWDebugLog"};

int RGWDebugLog(lua_State* L) 
{
  auto cct = reinterpret_cast<CephContext*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));

  auto message = luaL_checkstring(L, 1);
  ldout(cct, 20) << "Lua INFO: " << message << dendl;
  return 0;
}

void create_debug_action(lua_State* L, CephContext* cct) {
  lua_pushlightuserdata(L, cct);
  lua_pushcclosure(L, RGWDebugLog, ONE_UPVAL);
  lua_setglobal(L, RGWDebugLogAction);
}

void stack_dump(lua_State* L) {
  int top = lua_gettop(L);
  std::cout << std::endl << " ----------------  Stack Dump ----------------" << std::endl;
  std::cout << "Stack Size: " << top << std::endl;
  for (int i = 1, j = -top; i <= top; i++, j++) {
    std::cout << "[" << i << "," << j << "]: " << luaL_tolstring(L, i, NULL) << std::endl;
    lua_pop(L, 1);
  }
  std::cout << "--------------- Stack Dump Finished ---------------" << std::endl;
}

void set_package_path(lua_State* L, const std::string& install_dir) {
  if (install_dir.empty()) {
    return;
  }
  lua_getglobal(L, "package");
  if (!lua_istable(L, -1)) {
    return;
  }
  const auto path = install_dir+"/share/lua/"+CEPH_LUA_VERSION+"/?.lua";  
  pushstring(L, path);
  lua_setfield(L, -2, "path");
  
  const auto cpath = install_dir+"/lib/lua/"+CEPH_LUA_VERSION+"/?.so";
  pushstring(L, cpath);
  lua_setfield(L, -2, "cpath");
}

void open_standard_libs(lua_State* L) {
  luaL_openlibs(L);
  unsetglobal(L, "load");
  unsetglobal(L, "loadfile");
  unsetglobal(L, "loadstring");
  unsetglobal(L, "dofile");
  unsetglobal(L, "debug");
  // remove os.exit()
  lua_getglobal(L, "os");
  lua_pushstring(L, "exit");
  lua_pushnil(L);
  lua_settable(L, -3);
}

}
