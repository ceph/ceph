#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include <string>
#include <unordered_map>
#include <variant>
#include "rgw_lua_utils.h"
#include "rgw_realm_reloader.h"

namespace rgw::lua {

//Interval between each execution of the script is set to 5 seconds
constexpr const int INIT_EXECUTE_INTERVAL = 5;

//Writeable meta table named RGW with mutex protection
using BackgroundMapValue = std::variant<std::string, int64_t, double, bool>;
using BackgroundMap  = std::unordered_map<std::string, BackgroundMapValue>;

inline void pushvalue(lua_State* L, const std::string& value) {
  pushstring(L, value);
}

inline void pushvalue(lua_State* L, int64_t value) {
  lua_pushinteger(L, value);
}

inline void pushvalue(lua_State* L, double value) {
  lua_pushnumber(L, value);
}

inline void pushvalue(lua_State* L, bool value) {
  lua_pushboolean(L, value);
}


struct RGWTable : EmptyMetaTable {

  static const char* INCREMENT;
  static const char* DECREMENT;

  static std::string TableName() {return "RGW";}
  static std::string Name() {return TableName() + "Meta";}
  
  static int increment_by(lua_State* L);

  static int IndexClosure(lua_State* L) {
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
    auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, INCREMENT) == 0) {
      lua_pushlightuserdata(L, map);
      lua_pushlightuserdata(L, &mtx);
      lua_pushboolean(L, false /*increment*/);
      lua_pushcclosure(L, increment_by, THREE_UPVALS);
      return ONE_RETURNVAL;
    } 
    if (strcasecmp(index, DECREMENT) == 0) {
      lua_pushlightuserdata(L, map);
      lua_pushlightuserdata(L, &mtx);
      lua_pushboolean(L, true /*decrement*/);
      lua_pushcclosure(L, increment_by, THREE_UPVALS);
      return ONE_RETURNVAL;
    }

    std::lock_guard l(mtx);

    const auto it = map->find(std::string(index));
    if (it == map->end()) {
      lua_pushnil(L);
    } else {
      std::visit([L](auto&& value) { pushvalue(L, value); }, it->second);
    }
    return ONE_RETURNVAL;
  }

  static int LenClosure(lua_State* L) {
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
    auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    std::lock_guard l(mtx);

    lua_pushinteger(L, map->size());

    return ONE_RETURNVAL;
  }

  static int NewIndexClosure(lua_State* L) {
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
    auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto index = luaL_checkstring(L, 2);
    
    if (strcasecmp(index, INCREMENT) == 0 || strcasecmp(index, DECREMENT) == 0) {
        return luaL_error(L, "increment/decrement are reserved function names for RGW");
    }

    std::unique_lock l(mtx);

    size_t len;
    BackgroundMapValue value;
    const int value_type = lua_type(L, 3);

    switch (value_type) {
      case LUA_TNIL:
        map->erase(std::string(index));
        return NO_RETURNVAL;
      case LUA_TBOOLEAN:
        value = static_cast<bool>(lua_toboolean(L, 3));
        len = sizeof(bool);
        break;
      case LUA_TNUMBER:
         if (lua_isinteger(L, 3)) {
          value = lua_tointeger(L, 3);
          len = sizeof(int64_t);
         } else {
          value = lua_tonumber(L, 3);
          len = sizeof(double);
         }
         break;
      case LUA_TSTRING:
        value = lua_tolstring(L, 3, &len);
        break;
      default:
        l.unlock();
        return luaL_error(L, "unsupported value type for RGW table");
    }

    if (len + strnlen(index, MAX_LUA_VALUE_SIZE)
      > MAX_LUA_VALUE_SIZE) {
      return luaL_error(L, "Lua maximum size of entry limit exceeded");
    } else if (map->size() > MAX_LUA_KEY_ENTRIES) {
      l.unlock();
      return luaL_error(L, "Lua max number of entries limit exceeded");
    } else {
      map->insert_or_assign(index, value);
    }

    return NO_RETURNVAL;
  }

  static int PairsClosure(lua_State* L) {
    auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
    ceph_assert(map);
    lua_pushlightuserdata(L, map);
    lua_pushcclosure(L, stateless_iter, ONE_UPVAL); // push the stateless iterator function
    lua_pushnil(L);                                 // indicate this is the first call
    // return stateless_iter, nil

    return TWO_RETURNVALS;
  }
  
  static int stateless_iter(lua_State* L) {
    // based on: http://lua-users.org/wiki/GeneralizedPairsAndIpairs
    auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
    typename BackgroundMap::const_iterator next_it;
    if (lua_isnil(L, -1)) {
      next_it = map->begin();
    } else {
      const char* index = luaL_checkstring(L, 2);
      const auto it = map->find(std::string(index));
      ceph_assert(it != map->end());
      next_it = std::next(it);
    }

    if (next_it == map->end()) {
      // index of the last element was provided
      lua_pushnil(L);
      lua_pushnil(L);
      // return nil, nil
    } else {
      pushstring(L, next_it->first);
      std::visit([L](auto&& value) { pushvalue(L, value); }, next_it->second);
      // return key, value
    }

    return TWO_RETURNVALS;
  }
};

class Background : public RGWRealmReloader::Pauser {

private:
  BackgroundMap rgw_map;
  bool stopped = false;
  bool started = false;
  bool paused = false;
  int execute_interval;
  const DoutPrefix dp;
  rgw::sal::Store* store;
  CephContext* const cct;
  const std::string luarocks_path;
  std::thread runner;
  mutable std::mutex table_mutex;
  std::mutex cond_mutex;
  std::mutex pause_mutex;
  std::condition_variable cond;
  static const BackgroundMapValue empty_table_value;

  void run();

protected:
  std::string rgw_script;
  virtual int read_script();

public:
  Background(rgw::sal::Store* store,
      CephContext* cct,
      const std::string& luarocks_path,
      int execute_interval = INIT_EXECUTE_INTERVAL);

    virtual ~Background() = default;
    void start();
    void shutdown();
    void create_background_metatable(lua_State* L);
    const BackgroundMapValue& get_table_value(const std::string& key) const;
    template<typename T>
    void put_table_value(const std::string& key, T value) {
      std::unique_lock cond_lock(table_mutex);
      rgw_map[key] = value;
    }
    
    void pause() override;
    void resume(rgw::sal::Store* _store) override;
};

} //namepsace lua

