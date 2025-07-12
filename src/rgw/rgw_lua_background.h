#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include <string>
#include <unordered_map>
#include <variant>
#include "rgw_lua_utils.h"
#include "rgw_realm_reloader.h"
#ifdef WITH_RADOSGW_INTEL_TBB
#include <tbb/concurrent_hash_map.h>
#endif

namespace rgw::lua {

//Interval between each execution of the script is set to 5 seconds
constexpr const int INIT_EXECUTE_INTERVAL = 5;

//Writeable meta table named RGW with mutex protection
using BackgroundMapValue =
    std::variant<std::string, long long int, double, bool>;

class ProtectedMap {
 private:
 static const BackgroundMapValue empty_table_value;
#ifdef WITH_RADOSGW_INTEL_TBB
 mutable tbb::concurrent_hash_map<std::string, BackgroundMapValue> map;
#else
 mutable std::unordered_map<std::string, BackgroundMapValue> map;
 mutable std::mutex mtx;
#endif
 public:
#ifndef WITH_RADOSGW_INTEL_TBB
  std::mutex& get_mutex() const { return mtx; }
#endif

  BackgroundMapValue& get(const std::string& key) const {
#ifdef WITH_RADOSGW_INTEL_TBB
    tbb::concurrent_hash_map<std::string, BackgroundMapValue>::accessor acc;
    if (map.find(acc, key)) {
      return acc->second;
    }
#else
    std::lock_guard l(mtx);
    const auto it = map.find(key);
    if (it != map.end()) {
      return it->second;
    }
#endif
    return const_cast<BackgroundMapValue&>(empty_table_value);
  }

  #ifdef WITH_RADOSGW_INTEL_TBB
  tbb::concurrent_hash_map<std::string, BackgroundMapValue>& get_map() const {
    return map;
  }
  #else
  std::unordered_map<std::string, BackgroundMapValue>& get_map() const {
    return map;
  }
  #endif

  bool find(const std::string& key) const {
#ifdef WITH_RADOSGW_INTEL_TBB
    tbb::concurrent_hash_map<std::string, BackgroundMapValue>::accessor acc;
    return map.find(acc, key);
#else
    std::lock_guard l(mtx);
    return map.find(key) != map.end();
#endif
  }

  void erase(lua_State* L, const std::string& key, const char* name) {
#ifdef WITH_RADOSGW_INTEL_TBB
    map.erase(key);
#else
    std::lock_guard l(mtx);
    if (const auto it = map.find(key); it != map.end()) {
      update_erased_iterator<
          std::unordered_map<std::string, BackgroundMapValue> >(L, name, it,
                                                                map.erase(it));
    }
#endif
  }

  void insert(const std::string& key, BackgroundMapValue value) {
#ifdef WITH_RADOSGW_INTEL_TBB
    tbb::concurrent_hash_map<std::string, BackgroundMapValue>::accessor acc;
    if (map.find(acc, key)) {
      acc->second = value;
    } else {
      map.insert(acc, key);
      acc->second = value;
    }
#else
    map.insert_or_assign(key, value);
#endif
  }

  int size() const {
#ifndef WITH_RADOSGW_INTEL_TBB
    std::lock_guard l(mtx);
#endif
    return map.size();
  }
};

struct RGWTable : EmptyMetaTable {

  static const char* INCREMENT;
  static const char* DECREMENT;

  static int increment_by(lua_State* L);

  static int IndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    const auto map = reinterpret_cast<ProtectedMap*>(
        lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, INCREMENT) == 0) {
      lua_pushlightuserdata(L, map);
      lua_pushboolean(L, false /*increment*/);
      lua_pushcclosure(L, increment_by, TWO_UPVALS);
      return ONE_RETURNVAL;
    } 
    if (strcasecmp(index, DECREMENT) == 0) {
      lua_pushlightuserdata(L, map);
      lua_pushboolean(L, true /*decrement*/);
      lua_pushcclosure(L, increment_by, TWO_UPVALS);
      return ONE_RETURNVAL;
    }

    if (map->find(std::string(index))) {
      std::visit([L](auto&& value) { pushvalue(L, value); },
                 map->get(std::string(index)));
    } else {
      lua_pushnil(L);
    }
    return ONE_RETURNVAL;
  }

  static int LenClosure(lua_State* L) {
    const auto map = reinterpret_cast<ProtectedMap*>(
        lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
    lua_pushinteger(L, map->size());

    return ONE_RETURNVAL;
  }

  static int NewIndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto map = reinterpret_cast<ProtectedMap*>(
        lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto index = luaL_checkstring(L, 2);
    
    if (strcasecmp(index, INCREMENT) == 0 || strcasecmp(index, DECREMENT) == 0) {
        return luaL_error(L, "increment/decrement are reserved function names for RGW");
    }

    size_t len;
    BackgroundMapValue value;
    const int value_type = lua_type(L, 3);

    switch (value_type) {
      case LUA_TNIL:
        // erase the element. since in lua: "t[index] = nil" is removing the entry at "t[index]"
        if (map->find(std::string(index))) {
          map->erase(L, std::string(index), name);
        }
        return NO_RETURNVAL;
      case LUA_TBOOLEAN:
        value = static_cast<bool>(lua_toboolean(L, 3));
        len = sizeof(bool);
        break;
      case LUA_TNUMBER:
         if (lua_isinteger(L, 3)) {
          value = lua_tointeger(L, 3);
          len = sizeof(long long int);
         } else {
          value = lua_tonumber(L, 3);
          len = sizeof(double);
         }
         break;
      case LUA_TSTRING:
      {
        const auto str = lua_tolstring(L, 3, &len);
        value = std::string{str, len};
        break;
      }
      default:
        return luaL_error(L, "unsupported value type for RGW table");
    }

    if (len + strnlen(index, MAX_LUA_VALUE_SIZE)
      > MAX_LUA_VALUE_SIZE) {
      return luaL_error(L, "Lua maximum size of entry limit exceeded");
    } else if (map->size() > MAX_LUA_KEY_ENTRIES) {
      return luaL_error(L, "Lua max number of entries limit exceeded");
    } else {
      map->insert(std::string(index), value);
    }

    return NO_RETURNVAL;
  }

#ifndef WITH_RADOSGW_INTEL_TBB
  static int PairsClosure(lua_State* L) { return Pairs<ProtectedMap>(L); }
#endif
};

class Background : public RGWRealmReloader::Pauser {
 public:
  static const BackgroundMapValue empty_table_value;

 private:
  ProtectedMap rgw_map;
  bool stopped = false;
  bool started = false;
  bool paused = false;
  int execute_interval;
  const DoutPrefix dp;
  rgw::sal::LuaManager* lua_manager; 
  CephContext* const cct;
  std::thread runner;
#ifndef WITH_RADOSGW_INTEL_TBB
  mutable std::mutex table_mutex;
#endif
  std::mutex cond_mutex;
  std::mutex pause_mutex;
  std::condition_variable cond;

  void run();

  std::string rgw_script;
  int read_script();
  std::unique_ptr<lua_state_guard> initialize_lguard_state();

 public:
  Background(rgw::sal::Driver* _driver,
      CephContext* _cct,
      rgw::sal::LuaManager* _lua_manager,
      int _execute_interval = INIT_EXECUTE_INTERVAL);

  ~Background() override = default;
  void start();
  void shutdown();
  void create_background_metatable(lua_State* L);
  const BackgroundMapValue& get_table_value(const std::string& key) const;
  template <typename T>
  void put_table_value(const std::string& key, T value) {
#ifdef WITH_RADOSGW_INTEL_TBB
    rgw_map.insert(key, value);
#else
    std::unique_lock cond_lock(table_mutex);
    auto map = rgw_map.get_map();
    map[key] = value;
#endif
  }

  // update the manager after 
  void set_manager(rgw::sal::LuaManager* _lua_manager);
  void pause() override;
  void resume(rgw::sal::Driver* _driver) override;
};

} //namespace rgw::lua

