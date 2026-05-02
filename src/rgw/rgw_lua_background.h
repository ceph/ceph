#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include <string>
#include <set>
#include <variant>
#include <shared_mutex>
#include <mutex>
#include <boost/lockfree/queue.hpp>
#include "rgw_lua_utils.h"
#include "rgw_realm_reloader.h"
#include <tbb/concurrent_hash_map.h>

namespace rgw::lua {

//Interval between each execution of the script is set to 5 seconds
constexpr const int INIT_EXECUTE_INTERVAL = 5;

//Writeable meta table named RGW
using BackgroundMapValue = std::variant<std::string, long long int, double, bool>;
using BackgroundMap = tbb::concurrent_hash_map<std::string, BackgroundMapValue>;

struct RGWTable : EmptyMetaTable {

  static const char* INCREMENT;
  static const char* DECREMENT;

  static int increment_by(lua_State* L);

  static int IndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
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

    BackgroundMap::const_accessor accessor;

    if (map->find(accessor, std::string(index))) {
      std::visit([L](auto&& value) { pushvalue(L, value); }, accessor->second);
    } else {
      lua_pushnil(L);
    }
    return ONE_RETURNVAL;
  }

  static int LenClosure(lua_State* L) {
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));

    lua_pushinteger(L, map->size());

    return ONE_RETURNVAL;
  }

  static int NewIndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(THIRD_UPVAL)));
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
        {
          std::lock_guard l(mtx);
          const std::string key = std::string(index);
          for (auto it = map->begin(); it != map->end(); ++it) {
            if (it->first == key) {
              auto next_it = it;
              ++next_it;
              update_erased_iterator<BackgroundMap>(L, name, it, next_it);
              map->erase(key);
              break;
            }
          }
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
      BackgroundMap::accessor accessor;
      map->insert(accessor, std::string(index));
      accessor->second = value;
    }

    return NO_RETURNVAL;
  }

  static int PairsClosure(lua_State* L) {
    return Pairs<BackgroundMap>(L);
  }
};

class Background : public RGWRealmReloader::Pauser {
public:
  static const BackgroundMapValue empty_table_value;

private:
  BackgroundMap rgw_map;
  bool stopped = false;
  bool started = false;
  bool paused = false;
  int execute_interval;
  const DoutPrefix dp;
  rgw::sal::LuaManager* lua_manager; 
  CephContext* const cct;
  std::thread runner;
  mutable std::mutex table_mutex;
  std::mutex cond_mutex;
  std::mutex pause_mutex;
  std::condition_variable cond;

  std::map<std::string, std::unique_ptr<std::vector<char>>> lua_bytecode_cache; // script-name -> bytecode
//  bool updating = false;
  std::shared_mutex updating_mutex;
  boost::lockfree::queue<std::string*> processing_q{16};

  void run();

  std::string rgw_script;
  int read_script();
  std::unique_ptr<lua_state_guard> initialize_lguard_state();

  void process_scripts();

 public:
  Background(CephContext* _cct,
             rgw::sal::LuaManager* _lua_manager,
             int _execute_interval = INIT_EXECUTE_INTERVAL);

  ~Background() override = default;
  void start();
  void shutdown();
  void create_background_metatable(lua_State* L);
  BackgroundMapValue get_table_value(const std::string& key) const;

  template<typename T>
  void put_table_value(const std::string& key, T value) {
    BackgroundMap::accessor accessor;
    rgw_map.insert(accessor, key);
    accessor->second = value;
  }

  // update the manager after 
  void set_manager(rgw::sal::LuaManager* _lua_manager);
  void pause() override;
  // Does not actually use `Driver` argument.
  void resume(rgw::sal::Driver*) override;

  // for lua bytecode caching
  void process_script_add(std::string script);
  int get_script_bytecode(std::string key, std::vector<char>& lua_bytecode);
};

} //namespace rgw::lua

