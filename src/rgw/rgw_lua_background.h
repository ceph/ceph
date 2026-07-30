#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include <iterator>
#include <string>
#include <string_view>
#include <set>
#include <unordered_map>
#include <variant>
#include <shared_mutex>
#include <boost/lockfree/queue.hpp>
#include "rgw_lua_utils.h"
#include "rgw_realm_reloader.h"

namespace rgw::lua {

//Interval between each execution of the script is set to 5 seconds
constexpr const int INIT_EXECUTE_INTERVAL = 5;

//Writeable meta table named RGW with mutex protection
using BackgroundMapValue = std::variant<std::string, long long int, double, bool>;
using BackgroundMap  = std::unordered_map<std::string, BackgroundMapValue>;

struct RGWTable : EmptyMetaTable {

  static constexpr std::string_view INCREMENT = "increment";
  static constexpr std::string_view DECREMENT = "decrement";

  static int increment_by(lua_State* L);

  static int IndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(THIRD_UPVAL)));
    const auto index = lua_checkstring_view(L, 2);

    if (INCREMENT == index) {
      lua_pushlightuserdata(L, map);
      lua_pushlightuserdata(L, &mtx);
      lua_pushboolean(L, false /*increment*/);
      lua_pushcclosure(L, increment_by, THREE_UPVALS);
      return ONE_RETURNVAL;
    } 
    if (DECREMENT == index) {
      lua_pushlightuserdata(L, map);
      lua_pushlightuserdata(L, &mtx);
      lua_pushboolean(L, true /*decrement*/);
      lua_pushcclosure(L, increment_by, THREE_UPVALS);
      return ONE_RETURNVAL;
    }

    std::lock_guard l(mtx);

    const auto it = find_string_map_entry(*map, index);
    if (it == map->end()) {
      lua_pushnil(L);
      return ONE_RETURNVAL;
    }

    std::visit([L](auto&& value) { pushvalue(L, value); }, it->second);
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
    const auto name = table_name_upvalue(L);
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(THIRD_UPVAL)));
    const auto index = lua_checkstring_view(L, 2);
    
    if (INCREMENT == index || DECREMENT == index) {
      return luaL_error(L, "increment/decrement are reserved function names for RGW");
    }

    if (lua_isnil(L, 3)) {
      std::unique_lock l(mtx);

      // In Lua, "t[index] = nil" removes the entry at "t[index]".
      if (const auto it = find_string_map_entry(*map, index); it != map->end()) {
        update_erased_iterator<BackgroundMap>(L, name, it, map->erase(it));
      }

      return NO_RETURNVAL;
    }

    std::size_t value_size = 0;
    BackgroundMapValue value;
    const int value_type = lua_type(L, 3);

    switch (value_type) {
      case LUA_TBOOLEAN:
        value = static_cast<bool>(lua_toboolean(L, 3));
        value_size = sizeof(bool);
        break;
      case LUA_TNUMBER:
         if (lua_isinteger(L, 3)) {
          value = lua_tointeger(L, 3);
          value_size = sizeof(long long int);
          break;
         }

         value = lua_tonumber(L, 3);
         value_size = sizeof(double);
         break;
      case LUA_TSTRING:
      {
        const auto text = lua_checkstring_view(L, 3);
        value = std::string { text };
        value_size = std::size(text);
        break;
      }
      default:
        return luaL_error(L, "unsupported value type for RGW table");
    }

    if (MAX_LUA_VALUE_SIZE < std::size(index) + value_size) {
      return luaL_error(L, "Lua maximum size of entry limit exceeded");
    }

    // Lock only for shared-map inspection and mutation, after Lua value validation.
    std::unique_lock l(mtx);
    const auto existing = find_string_map_entry(*map, index);

    if (existing == map->end() && MAX_LUA_KEY_ENTRIES <= std::size(*map)) {
      l.unlock();
      return luaL_error(L, "Lua max number of entries limit exceeded");
    }

    map->insert_or_assign(std::string { index }, std::move(value));

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
  const BackgroundMapValue& get_table_value(const std::string& key) const;
  template<typename T>
  void put_table_value(const std::string& key, T value) {
    std::unique_lock cond_lock(table_mutex);
    rgw_map[key] = value;
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
