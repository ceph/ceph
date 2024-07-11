#include <string>
#include <lua.hpp>
#include "common/ceph_context.h"
#include "common/debug.h"
#include "rgw_lua_utils.h"
#include "rgw_lua_version.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua {

// TODO - add the following generic functions
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
  const auto top = lua_gettop(L);
  std::cout << std::endl << " ----------------  Stack Dump ----------------" << std::endl;
  std::cout << "Stack Size: " << top << std::endl;
  for (int i = 1, j = -top; i <= top; i++, j++) {
		std::cout << "[" << i << "," << j << "][" << luaL_typename(L, i) << "]: ";
    switch (lua_type(L, i)) {
      case LUA_TNUMBER:
        std::cout << lua_tonumber(L, i);
        break;
      case LUA_TSTRING:
        std::cout << lua_tostring(L, i);
        break;
      case LUA_TBOOLEAN:
        std::cout << (lua_toboolean(L, i) ? "true" : "false");
        break;
      case LUA_TNIL:
        std::cout << "nil";
        break;
      default:
        std::cout << lua_topointer(L, i);
        break;
    }
		std::cout << std::endl;
  }
  std::cout << "--------------- Stack Dump Finished ---------------" << std::endl;
}

void set_package_path(lua_State* L, const std::string& install_dir) {
  if (install_dir.empty()) {
    return;
  }
  if (lua_getglobal(L, "package") != LUA_TTABLE) {
    return;
  }
  const auto path = install_dir+"/share/lua/"+CEPH_LUA_VERSION+"/?.lua";
  pushstring(L, path);
  lua_setfield(L, -2, "path");
  
  const auto cpath = install_dir+"/lib/lua/"+CEPH_LUA_VERSION+"/?.so;"+install_dir+"/lib64/lua/"+CEPH_LUA_VERSION+"/?.so";
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
  if (lua_getglobal(L, "os") == LUA_TTABLE) {
    lua_pushstring(L, "exit");
    lua_pushnil(L);
    lua_settable(L, -3);
  }
}

// allocator function that verifies against maximum allowed memory value
void* allocator(void* ud, void* ptr, std::size_t osize, std::size_t nsize) {
  auto mem = reinterpret_cast<std::size_t*>(ud); // remaining memory
  // free memory
  if (nsize == 0) {
    if (mem && ptr) {
      *mem += osize;
    }
    free(ptr);
    return nullptr;
  }
  // re/alloc memory
  if (mem) {
    const std::size_t realloc_size = ptr ? osize : 0;
    if (nsize > realloc_size && (nsize - realloc_size) > *mem) {
      return nullptr;
    }
    *mem += realloc_size;
    *mem -= nsize;
  }
  return realloc(ptr, nsize);
}

// create new lua state together with its memory counter
lua_State* newstate(int max_memory) {
  std::size_t* remaining_memory = nullptr;
  if (max_memory > 0) {
    remaining_memory = new std::size_t(max_memory);
  }
  lua_State* L = lua_newstate(allocator, remaining_memory);
  if (!L) {
    delete remaining_memory;
    remaining_memory = nullptr;
  }
  if (L) {
    lua_atpanic(L, [](lua_State* L) -> int {
      const char* msg = lua_tostring(L, -1);
      if (msg == nullptr) msg = "error object is not a string";
      throw std::runtime_error(msg);
    });
  }
  return L;
}

// lua_state_guard ctor
lua_state_guard::lua_state_guard(std::size_t  _max_memory, const DoutPrefixProvider* _dpp) :
  max_memory(_max_memory),
  dpp(_dpp),
  state(newstate(_max_memory)) {
  if (state &&  perfcounter) {
    perfcounter->inc(l_rgw_lua_current_vms, 1);
  }
}

// lua_state_guard dtor
lua_state_guard::~lua_state_guard() {
  lua_State* L = state;
  if (!L) {
    return;
  }
  void* ud = nullptr;
  lua_getallocf(L, &ud);
  auto remaining_memory = static_cast<std::size_t*>(ud);

  if (remaining_memory) {
    const auto used_memory = max_memory - *remaining_memory;
    ldpp_dout(dpp, 20) << "Lua is using: " << used_memory << 
      " bytes (" << 100.0*used_memory/max_memory << "%)" << dendl;
    // dont limit memory during cleanup
    *remaining_memory = 0;
  }
  try {
    lua_close(L);
  } catch (const std::runtime_error& e) {
    ldpp_dout(dpp, 20) << "Lua cleanup failed with: " << e.what() << dendl;
  }

  // TODO: use max_memory and remaining memory to check for leaks
  // this could be done only if we don't zero the remianing memory during clanup

  delete remaining_memory;
  if (perfcounter) {
    perfcounter->dec(l_rgw_lua_current_vms, 1);
  }
}

} // namespace rgw::lua

