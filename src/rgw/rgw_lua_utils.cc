#include <string>
#include <lua.hpp>
#include "common/ceph_context.h"
#include "common/dout.h"
#include "rgw_lua_utils.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua {

// TODO - add the folowing generic functions
// lua_push(lua_State* L, const std::string& str)
// template<typename T> lua_push(lua_State* L, const std::optional<T>& val)
// lua_push(lua_State* L, const ceph::real_time& tp)

constexpr const char* RGWDebugLogAction{"RGWDebugLog"};

int RGWDebugLog(lua_State* L) 
{
  auto cct = reinterpret_cast<CephContext*>(lua_touserdata(L, lua_upvalueindex(1)));

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

}
