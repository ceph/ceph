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
  auto guard = reinterpret_cast<lua_state_guard*>(ud);
  auto mem_in_use = guard->get_mem_in_use();

  // free memory
  if (nsize == 0) {
    if (ptr) {
      guard->set_mem_in_use(mem_in_use - osize);
    }
    free(ptr);
    return nullptr;
  }
  // re/alloc memory
  const std::size_t new_total = mem_in_use - (ptr ? osize : 0) + nsize;
  if (guard->get_max_memory() > 0 && new_total > guard->get_max_memory()) {
    return nullptr;
  }
  guard->set_mem_in_use(new_total);
  return realloc(ptr, nsize);
}

// create new lua state together with reference to the guard
lua_State* newstate(lua_state_guard* guard) {
  lua_State* L = lua_newstate(allocator, guard);
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
lua_state_guard::lua_state_guard(std::size_t _max_memory,
                                 std::uint64_t _max_runtime,
                                 const DoutPrefixProvider* _dpp)
    : max_memory(_max_memory),
      mem_in_use(0),
      max_runtime(std::chrono::milliseconds(_max_runtime)),
      start_time(ceph::real_clock::now()),
      dpp(_dpp),
      state(newstate(this)) {
  if (state) {
    if (max_runtime.count() > 0) {
      set_runtime_hook();
    }
    if (perfcounter) {
      perfcounter->inc(l_rgw_lua_current_vms, 1);
    }
  }
}

// lua_state_guard dtor
lua_state_guard::~lua_state_guard() {
  lua_State* L = state;
  if (!L) {
    return;
  }
  int temp_max_memory = max_memory;
  if (mem_in_use > 0) {
    ldpp_dout(dpp, 20) << "Lua is using: " << mem_in_use << " bytes ("
                       << std::to_string(100.0 -
                                         (100.0 * mem_in_use / max_memory))
                       << "%)" << dendl;
  }
  // dont limit memory during cleanup
  max_memory = 0;
  // clear any runtime hooks
  lua_sethook(L, nullptr, 0, 0);
  try {
    lua_close(L);
  } catch (const std::runtime_error& e) {
    ldpp_dout(dpp, 20) << "Lua cleanup failed with: " << e.what() << dendl;
  }

  // Check for memory leaks
  if (temp_max_memory > 0 && mem_in_use > 0) {
    ldpp_dout(dpp, 1) << "ERROR: Lua memory leak detected: " << mem_in_use
                      << " bytes still in use" << dendl;
  }

  if (perfcounter) {
    perfcounter->dec(l_rgw_lua_current_vms, 1);
  }
}

bool lua_state_guard::set_max_memory(std::size_t _max_memory) {
  if (_max_memory == max_memory) {
    return true;
  }
  ldpp_dout(dpp, 20) << "Lua is using: " << mem_in_use << " bytes ("
                     << std::to_string(100.0 -
                                       (100.0 * mem_in_use / max_memory))
                     << "%)" << dendl;

  if (mem_in_use > _max_memory && _max_memory > 0) {
    max_memory = _max_memory;
    ldpp_dout(dpp, 10) << "Lua memory limit is below current usage" << dendl;
    ldpp_dout(dpp, 20) << "Lua memory limit set to: " << max_memory << " bytes"
                       << dendl;
    return false;
  }

  max_memory = _max_memory;
  ldpp_dout(dpp, 20) << "Lua memory limit set to: "
                     << (max_memory > 0 ? std::to_string(max_memory) : "N/A")
                     << " bytes" << dendl;
  return true;
}

void lua_state_guard::set_mem_in_use(std::size_t _mem_in_use) {
  mem_in_use = _mem_in_use;
}

void lua_state_guard::set_max_runtime(std::uint64_t _max_runtime) {
  if (static_cast<uint64_t>(max_runtime.count()) != _max_runtime) {
    if (_max_runtime > 0) {
      auto omax_runtime = max_runtime.count();
      max_runtime = std::chrono::milliseconds(_max_runtime);
      if (omax_runtime == 0) {
        set_runtime_hook();
      }
    } else {
      max_runtime = std::chrono::milliseconds(0);
      lua_sethook(state, nullptr, 0, 0);
    }
    ldpp_dout(dpp, 20) << "Lua runtime limit set to: "
                       << (max_runtime.count() > 0
                               ? std::to_string(max_runtime.count())
                               : "N/A")
                       << " milliseconds" << dendl;
  }
}

void lua_state_guard::runtime_hook(lua_State* L, lua_Debug* ar) {
  auto now = ceph::real_clock::now();
  lua_getfield(L, LUA_REGISTRYINDEX, max_runtime_key);
  auto max_runtime =
      *static_cast<std::chrono::milliseconds*>(lua_touserdata(L, -1));
  lua_getfield(L, LUA_REGISTRYINDEX, start_time_key);
  auto start_time =
      *static_cast<ceph::real_clock::time_point*>(lua_touserdata(L, -1));
  lua_pop(L, 2);
  auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time);

  if (elapsed > max_runtime) {
    std::string err = "Lua runtime limit exceeded: total elapsed time is " +
                      std::to_string(elapsed.count()) + " ms";
    luaL_error(L, "%s", err.c_str());
  }
}

void lua_state_guard::set_runtime_hook() {
  lua_pushlightuserdata(state,
                        const_cast<std::chrono::milliseconds*>(&max_runtime));
  lua_setfield(state, LUA_REGISTRYINDEX, max_runtime_key);
  lua_pushlightuserdata(state,
                        const_cast<ceph::real_clock::time_point*>(&start_time));
  lua_setfield(state, LUA_REGISTRYINDEX, start_time_key);

  // Check runtime after each line or every 1000 VM instructions
  lua_sethook(state, runtime_hook, LUA_MASKLINE | LUA_MASKCOUNT, 1000);
}

} // namespace rgw::lua

