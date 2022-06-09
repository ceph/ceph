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
  const auto default_inc = (decrement ? -1 : 1);
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
    } else if (std::holds_alternative<int64_t>(value) && std::holds_alternative<int64_t>(inc_by)) {
      value = std::get<int64_t>(value) + std::get<int64_t>(inc_by);
    } else if (std::holds_alternative<double>(value) && std::holds_alternative<int64_t>(inc_by)) {
      value = std::get<double>(value) + static_cast<double>(std::get<int64_t>(inc_by));
    } else if (std::holds_alternative<int64_t>(value) && std::holds_alternative<double>(inc_by)) {
      value = static_cast<double>(std::get<int64_t>(value)) + std::get<double>(inc_by);
    } else {
      mtx.unlock();
      return luaL_error(L, "can increment only numeric values");
    }
  }

  return 0;
}

Background::Background(rgw::sal::Store* store,
    CephContext* cct,
      const std::string& luarocks_path,
      int execute_interval) :
    execute_interval(execute_interval),
    dp(cct, dout_subsys, "lua background: "),
    store(store),
    cct(cct),
    luarocks_path(luarocks_path) {}

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
  const auto rc = ceph_pthread_setname(runner.native_handle(),
      "lua_background");
  ceph_assert(rc == 0);
}

void Background::pause() {
  {
    std::unique_lock cond_lock(pause_mutex);
    paused = true;
  }
  cond.notify_all();
}

void Background::resume(rgw::sal::Store* _store) {
  store = _store;
  paused = false;
  cond.notify_all();
}

int Background::read_script() {
  std::unique_lock cond_lock(pause_mutex);
  if (paused) {
    return -EAGAIN;
  }
  std::string tenant;
  return rgw::lua::read_script(&dp, store, tenant, null_yield, rgw::lua::context::background, rgw_script);
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
  lua_State* const L = luaL_newstate();
  rgw::lua::lua_state_guard lguard(L);
  open_standard_libs(L);
  set_package_path(L, luarocks_path);
  create_debug_action(L, cct);
  create_background_metatable(L);
  const DoutPrefixProvider* const dpp = &dp;

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
      } catch (const std::exception& e) {
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
  create_metatable<rgw::lua::RGWTable>(L, true, &rgw_map, &table_mutex);
}

} //namespace lua

