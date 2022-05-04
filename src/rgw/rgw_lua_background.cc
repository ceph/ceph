#include "rgw_lua_background.h"
#include "rgw_lua.h"
#include "rgw_lua_utils.h"
#include "rgw_perf_counters.h"
#include "include/ceph_assert.h"
#include <lua.hpp>

namespace rgw::lua {

void Background::shutdown(){
  this->stop();
  if (runner.joinable()) {
    runner.join();
  }
}
void Background::stop(){
  stopped = true;
}

void Background::start() {
  if (started) {
    // start the thread only once
    return;
  }
  started = true;
  runner = std::thread(&Background::run, this);
  const auto rc = ceph_pthread_setname(runner.native_handle(),
      "lua_background");
  ceph_assert(rc == 0);
}

int Background::read_script() {
  std::string tenant;
  return rgw::lua::read_script(dpp, store, tenant, null_yield, rgw::lua::context::background, rgw_script);
}

//(1) Loads the script from the object
//(2) Executes the script
//(3) Sleep (configurable)
void Background::run() {
  lua_State* const L = luaL_newstate();
  rgw::lua::lua_state_guard lguard(L);
  open_standard_libs(L);
  set_package_path(L, luarocks_path);
  create_debug_action(L, cct);
  create_background_metatable(L);

  while (!stopped) {
    const auto rc = read_script();
    if (rc == -ENOENT) {
      // no script, nothing to do
    } else if (rc < 0) {
      ldpp_dout(dpp, 1) << "WARNING: failed to read background script. error " << rc << dendl;
    } else {
      auto failed = false;
      try {
        //execute the background lua script
        if (luaL_dostring(L, rgw_script.c_str()) != LUA_OK) {
          const std::string err(lua_tostring(L, -1));
          ldpp_dout(dpp, 1) << "Lua ERROR: " << err << dendl;
          failed = true;
        }
      } catch (const std::exception& e) {
        ldpp_dout(dpp, 1) << "Lua ERROR: " << e.what() << dendl;
        failed = true;
      }
      if (perfcounter) {
        perfcounter->inc((failed ? l_rgw_lua_script_fail : l_rgw_lua_script_ok), 1);
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(execute_interval));
  }
}

void Background::create_background_metatable(lua_State* L) {
  create_metatable<rgw::lua::RGWTable>(L, true, &rgw_map, &m_mutex);
}

} //namespace lua

