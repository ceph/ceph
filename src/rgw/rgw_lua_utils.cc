#include <string>
#include <lua.hpp>
#include "common/ceph_context.h"
#include "common/dout.h"
#include "rgw_lua_utils.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua {

void lua_pushstring(lua_State* L, const std::string& str) 
{
  lua_pushstring(L, str.c_str());
}

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
	auto i = lua_gettop(L);
  std::cout << std::endl << " ----------------  Stack Dump ----------------" << std::endl;
  std::cout << "Stack Size: " << i << std::endl;
  while (i > 0) {
    const auto t = lua_type(L, i);
    switch (t) {
      case LUA_TNIL:
        std::cout << i << ": nil" << std::endl;
        break;
      case LUA_TSTRING:
        std::cout << i << ": " << lua_tostring(L, i) << std::endl;
        break;
      case LUA_TBOOLEAN:
        std::cout << i << ": " << lua_toboolean(L, i) << std::endl;
        break;
      case LUA_TNUMBER:
        std::cout << i << ": " << lua_tonumber(L, i) << std::endl;
        break;
      default: 
        std::cout << i << ": " << lua_typename(L, t) << std::endl;
        break;
    }
    i--;
  }
  std::cout << "--------------- Stack Dump Finished ---------------" << std::endl;
}

}
