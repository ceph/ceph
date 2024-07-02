#include "rgw_sal_rados.h"
#include "rgw_lua_background.h"
#include "rgw_lua.h"
#include "rgw_lua_utils.h"
#include "rgw_perf_counters.h"
#include "include/ceph_assert.h"
#include <lua.hpp>

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua {

const char* RGWTable::INCREMENT = "increment";
const char* RGWTable::DECREMENT = "decrement";

int RGWTable::increment_by(lua_State* L) {
  const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
  auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
  auto decrement = lua_toboolean(L, lua_upvalueindex(THIRD_UPVAL));

  const auto args = lua_gettop(L);
  const auto index = luaL_checkstring(L, 1);

  // by default we increment by 1/-1
  const long long int default_inc = (decrement ? -1 : 1);
  BackgroundMapValue inc_by = default_inc;
  if (args == 2) {
    if (lua_isinteger(L, 2)) {
      inc_by = lua_tointeger(L, 2)*default_inc;
    } else if (lua_isnumber(L, 2)){
      inc_by = lua_tonumber(L, 2)*static_cast<double>(default_inc);
    } else {
      return luaL_error(L, "can increment only by numeric values");
    }
  }

  std::unique_lock l(mtx);

  const auto it = map->find(std::string(index));
  if (it != map->end()) {
    auto& value = it->second;
    if (std::holds_alternative<double>(value) && std::holds_alternative<double>(inc_by)) {
      value = std::get<double>(value) + std::get<double>(inc_by);
    } else if (std::holds_alternative<long long int>(value) && std::holds_alternative<long long int>(inc_by)) {
      value = std::get<long long int>(value) + std::get<long long int>(inc_by);
    } else if (std::holds_alternative<double>(value) && std::holds_alternative<long long int>(inc_by)) {
      value = std::get<double>(value) + static_cast<double>(std::get<long long int>(inc_by));
    } else if (std::holds_alternative<long long int>(value) && std::holds_alternative<double>(inc_by)) {
      value = static_cast<double>(std::get<long long int>(value)) + std::get<double>(inc_by);
    } else {
      mtx.unlock();
      return luaL_error(L, "can increment only numeric values");
    }
  }

  return 0;
}

Background::Background(rgw::sal::Driver* _driver,
    CephContext* _cct,
    rgw::sal::LuaManager* _lua_manager,
    int _execute_interval) :
    execute_interval(_execute_interval)
    , dp(_cct, dout_subsys, "lua background: ")
    , lua_manager(_lua_manager)
    , cct(_cct)
{}

void Background::shutdown(){
  stopped = true;
  cond.notify_all();
  if (runner.joinable()) {
    runner.join();
  }
  started = false;
  stopped = false;
}

void Background::start() {
  if (started) {
    // start the thread only once
    return;
  }
  started = true;
  runner = std::thread(&Background::run, this);
  const char* thread_name = "lua_background";
  if (const auto rc = ceph_pthread_setname(runner.native_handle(), thread_name); rc != 0) {
    ldout(cct, 1) << "ERROR: failed to set lua background thread name to: " << thread_name
      << ". error: " << rc << dendl;
  }
}

void Background::pause() {
  {
    std::unique_lock cond_lock(pause_mutex);
    paused = true;
  }
  cond.notify_all();
}

void Background::resume(rgw::sal::Driver* driver) {
  paused = false;
  cond.notify_all();
}

int Background::read_script() {
  std::unique_lock cond_lock(pause_mutex);
  if (paused) {
    return -EAGAIN;
  }
  std::string tenant;
  return rgw::lua::read_script(&dp, lua_manager, tenant, null_yield, rgw::lua::context::background, rgw_script);
}

const BackgroundMapValue Background::empty_table_value;

const BackgroundMapValue& Background::get_table_value(const std::string& key) const {
  std::unique_lock cond_lock(table_mutex);
  const auto it = rgw_map.find(key);
  if (it == rgw_map.end()) {
    return empty_table_value;
  }
  return it->second;
}

//(1) Loads the script from the object if not paused
//(2) Executes the script
//(3) Sleep (configurable)
void Background::run() {
  const DoutPrefixProvider* const dpp = &dp;
  lua_state_guard lguard(cct->_conf->rgw_lua_max_memory_per_state, dpp);
  auto L = lguard.get();
  if (!L) {
    ldpp_dout(dpp, 1) << "Failed to create state for Lua background thread" << dendl;
    return;
  }
  try {
    open_standard_libs(L);
  set_package_path(L, lua_manager->luarocks_path());
    create_debug_action(L, cct);
    create_background_metatable(L);
  } catch (const std::runtime_error& e) { 
    ldpp_dout(dpp, 1) << "Failed to create initial setup of Lua background thread. error " 
      << e.what() << dendl;
    return;
  }

  while (!stopped) {
    if (paused) {
      ldpp_dout(dpp, 10) << "Lua background thread paused" << dendl;
      std::unique_lock cond_lock(cond_mutex);
      cond.wait(cond_lock, [this]{return !paused || stopped;}); 
      if (stopped) {
        ldpp_dout(dpp, 10) << "Lua background thread stopped" << dendl;
        return;
      }
      ldpp_dout(dpp, 10) << "Lua background thread resumed" << dendl;
    }
    
    const auto rc = read_script();
    if (rc == -ENOENT || rc == -EAGAIN) {
      // either no script or paused, nothing to do
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
      } catch (const std::runtime_error& e) {
        ldpp_dout(dpp, 1) << "Lua ERROR: " << e.what() << dendl;
        failed = true;
      }
      if (perfcounter) {
        perfcounter->inc((failed ? l_rgw_lua_script_fail : l_rgw_lua_script_ok), 1);
      }
    }
    std::unique_lock cond_lock(cond_mutex);
    cond.wait_for(cond_lock, std::chrono::seconds(execute_interval), [this]{return stopped;}); 
  }
  ldpp_dout(dpp, 10) << "Lua background thread stopped" << dendl;
}

void Background::create_background_metatable(lua_State* L) {
  static const char* background_table_name = "RGW";
  create_metatable<RGWTable>(L, "", background_table_name, true, &rgw_map, &table_mutex);
  lua_getglobal(L, background_table_name);
  ceph_assert(lua_istable(L, -1));
}

void Background::set_manager(rgw::sal::LuaManager* _lua_manager) {
  lua_manager = _lua_manager;
}

} //namespace rgw::lua

