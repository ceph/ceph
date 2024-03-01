#pragma once

#include <type_traits>
#include <variant>
#include <string.h>
#include <memory>
#include <map>
#include <string>
#include <string_view>
#include <ctime>
#include <lua.hpp>

#include "include/common_fwd.h"
#include "rgw_perf_counters.h"

// a helper type traits structs for detecting std::variant
template<class>
struct is_variant : std::false_type {};
template<class... Ts> 
struct is_variant<std::variant<Ts...>> : 
    std::true_type {};

class DoutPrefixProvider;

namespace rgw::lua {

// push ceph time in string format: "%Y-%m-%d %H:%M:%S"
template <typename CephTime>
const char* pushtime(lua_State* L, const CephTime& tp)
{
  const auto tt = CephTime::clock::to_time_t(tp);
  const auto tm = *std::localtime(&tt);
  char buff[64];
  const auto len = std::strftime(buff, sizeof(buff), "%Y-%m-%d %H:%M:%S", &tm);
  return lua_pushlstring(L, buff, len+1);
}

static inline const char* pushstring(lua_State* L, std::string_view str)
{
  return lua_pushlstring(L, str.data(), str.size());
}

inline void pushvalue(lua_State* L, const std::string& value) {
  pushstring(L, value);
}

inline void pushvalue(lua_State* L, long long value) {
  lua_pushinteger(L, value);
}

inline void pushvalue(lua_State* L, double value) {
  lua_pushnumber(L, value);
}

inline void pushvalue(lua_State* L, bool value) {
  lua_pushboolean(L, value);
}

inline void unsetglobal(lua_State* L, const char* name) 
{
  lua_pushnil(L);
  lua_setglobal(L, name);
}

// dump the lua stack to stdout
void stack_dump(lua_State* L);

class lua_state_guard {
  const std::size_t max_memory;
  const DoutPrefixProvider* const dpp;
  lua_State* const state;
public:
  lua_state_guard(std::size_t _max_memory, const DoutPrefixProvider* _dpp);
  ~lua_state_guard();
  lua_State* get() { return state; }
};

int dostring(lua_State* L, const char* str);

constexpr const int MAX_LUA_VALUE_SIZE = 1000;
constexpr const int MAX_LUA_KEY_ENTRIES = 100000;

constexpr auto ONE_UPVAL    = 1;
constexpr auto TWO_UPVALS   = 2;
constexpr auto THREE_UPVALS = 3;
constexpr auto FOUR_UPVALS  = 4;
constexpr auto FIVE_UPVALS  = 5;

constexpr auto FIRST_UPVAL    = 1;
constexpr auto SECOND_UPVAL   = 2;
constexpr auto THIRD_UPVAL    = 3;
constexpr auto FOURTH_UPVAL   = 4;
constexpr auto FIFTH_UPVAL    = 5;

constexpr auto NO_RETURNVAL    = 0;
constexpr auto ONE_RETURNVAL    = 1;
constexpr auto TWO_RETURNVALS   = 2;
constexpr auto THREE_RETURNVALS = 3;
constexpr auto FOUR_RETURNVALS  = 4;

// create_metatable() is a utility functions to create a metatable
// and tie it to an unnamed table
//
// it add an __index method to it, to allow reading values
// if "readonly" parameter is set to "false", it will also add
// a __newindex method to it, to allow writing values
// if the "toplevel" parameter is set to "true", it will name the
// table as well as the metatable, this would allow direct access from
// the lua script.
// the name of the metatable will be a fully qualified name, based on the names
// of the metatables that contain it, separated by dots.
// it also adds a __len and  __pairs methods for iterable entities.
//
// The MetaTable is expected to be a class with the following members:
// Type (typename) - the type of the "upvalue" (the type that the meta table represent)
// IndexClosure (static function return "int" and accept "lua_State*") 
// NewIndexClosure (static function return "int" and accept "lua_State*") 
// PairsClosure (static function return "int" and accept "lua_State*") 
// LenClosure (static function return "int" and accept "lua_State*") 
//
// e.g.
// struct MyStructMetaTable {
//
//   using Type = MyStruct;
//
//   static int IndexClosure(lua_State* L) {
//     const auto name = tatable_name_upvalue(L);
//     const auto value = reinterpret_cast<const Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
//     ...
//   }

//   static int NewIndexClosure(lua_State* L) {
//     const auto name = table_name_upvalue(L);
//     auto value = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
//     ...
//   }
// };
//
// If the structs inherits from EmptyMetatable then the only function that needs 
// to be implemented is IndexClosure. The other functions are implemented in 
// the base class stating that the table is "readonly" and "not-iterable".

template<typename MetaTable, typename... Upvalues>
void create_metatable(lua_State* L, std::string_view parent_name, std::string_view field_name,
    bool toplevel, Upvalues... upvalues)
{
  const std::string qualified_name(fmt::format("{}{}{}", 
        parent_name,
        (parent_name.empty() ? "" : "."),
        field_name));
  constexpr auto upvals_size = sizeof...(upvalues);
  const std::array<void*, upvals_size> upvalue_arr = {upvalues...};
  // create table
  lua_newtable(L);
  if (toplevel) {
    // duplicate the table to make sure it remain in the stack
    lua_pushvalue(L, -1);
    // give table a name (in cae of "toplevel")
    lua_setglobal(L, qualified_name.c_str());
  }

  // create/reuse metatable
  const auto metatable_is_new = luaL_newmetatable(L, qualified_name.c_str());
  if (!metatable_is_new) {
    // tie metatable to table
    lua_setmetatable(L, -2);
    return;
  }

  const auto table_stack_pos = lua_gettop(L);

  // add "index" closure to metatable
  lua_pushliteral(L, "__index");
  pushstring(L, qualified_name);
  for (const auto upvalue : upvalue_arr) {
    lua_pushlightuserdata(L, upvalue);
  }
  lua_pushcclosure(L, MetaTable::IndexClosure, upvals_size+1);
  lua_rawset(L, table_stack_pos);

  // add "newindex" closure to metatable
  lua_pushliteral(L, "__newindex");
  pushstring(L, qualified_name);
  for (const auto upvalue : upvalue_arr) {
    lua_pushlightuserdata(L, upvalue);
  }
  lua_pushcclosure(L, MetaTable::NewIndexClosure, upvals_size+1);
  lua_rawset(L, table_stack_pos);

  // add "pairs" closure to metatable
  lua_pushliteral(L, "__pairs");
  pushstring(L, qualified_name);
  for (const auto upvalue : upvalue_arr) {
    lua_pushlightuserdata(L, upvalue);
  }
  lua_pushcclosure(L, MetaTable::PairsClosure, upvals_size+1);
  lua_rawset(L, table_stack_pos);

  // add "len" closure to metatable
  lua_pushliteral(L, "__len");
  for (const auto upvalue : upvalue_arr) {
    lua_pushlightuserdata(L, upvalue);
  }
  // "name" is not needed at the "len" closure
  lua_pushcclosure(L, MetaTable::LenClosure, upvals_size);
  lua_rawset(L, table_stack_pos);

  // tie metatable to table
  lua_setmetatable(L, -2);
}

template<typename MetaTable>
void create_metatable(lua_State* L, const std::string_view parent_name, const std::string_view field_name,
    bool toplevel, std::unique_ptr<typename MetaTable::Type>& ptr)
{
  if (ptr) {
    create_metatable<MetaTable>(L, parent_name, field_name, toplevel, reinterpret_cast<void*>(ptr.get()));
  } else {
    lua_pushnil(L);
  }
}
// following struct may be used as a base class for other MetaTable classes
// note, however, this is not mandatory to use it as a base
struct EmptyMetaTable {
  // by default everything is "readonly"
  // to change, overload this function in the derived
  static int NewIndexClosure(lua_State* L) {
    return luaL_error(L, "trying to write to readonly field");
  }
  
  // by default nothing is iterable
  // to change, overload this function in the derived
  static int PairsClosure(lua_State* L) {
    return luaL_error(L, "trying to iterate over non-iterable field");
  }
  
  // by default nothing is iterable
  // to change, overload this function in the derived
  static int LenClosure(lua_State* L) {
    return luaL_error(L, "trying to get length of non-iterable field");
  }

  static int error_unknown_field(lua_State* L, const std::string& index, const std::string& table) {
    return luaL_error(L, "unknown field name: %s provided to: %s",
                      index.c_str(), table.c_str());
  }
};

// create a debug log action
// it expects CephContext to be captured
// it expects one string parameter, which is the message to log
// could be executed from any context that has CephContext
// e.g.
//    RGWDebugLog("hello world from lua")
//
void create_debug_action(lua_State* L, CephContext* cct);

// set the packages search path according to:
// package.path  = "<install_dir>/share/lua/5.3/?.lua"
// package.cpath = "<install_dir>/lib/lua/5.3/?.so"
void set_package_path(lua_State* L, const std::string& install_dir);

// open standard lua libs and remove the following functions:
// os.exit()
// load()
// loadfile()
// loadstring()
// dofile()
// and the "debug" library
void open_standard_libs(lua_State* L);

typedef int MetaTableClosure(lua_State* L);

// get iterator name from table name
inline std::string get_iterator_name(const std::string_view name) {
  return fmt::format("{}.Iterator", name);
}

// the fully qualified name of the table is expected to be the 1st upvalue of the closure
inline const char* table_name_upvalue(lua_State* L) {
  const auto name = reinterpret_cast<const char*>(lua_tostring(L, lua_upvalueindex(FIRST_UPVAL)));
  ceph_assert(name);
  return name;
}

// copy the input iterator into a new iterator with memory allocated as userdata
// - allow for string conversion of the iterator (into its key)
// - storing the iterator in the metadata table to be used for iterator invalidation handling
// - since we have only one iterator per map/table we don't allow for nested loops or another iterationn
// after breaking out of an iteration
template<typename MapType>
typename MapType::iterator* create_iterator_metadata(lua_State* L, const std::string_view name, 
    const typename MapType::iterator& start_it, const typename MapType::iterator& end_it) {
  using Iterator = typename MapType::iterator;
  const std::string qualified_name = get_iterator_name(name);
  // create metatable for userdata
  // metatable is created before the userdata to save on allocation if the metatable already exists
  const auto metatable_is_new = luaL_newmetatable(L, qualified_name.c_str());
  const auto metatable_pos = lua_gettop(L);
  int userdata_pos;
  Iterator* new_it = nullptr;
  if (!metatable_is_new) {
    // metatable already exists
    lua_pushliteral(L, "__iterator");
    const auto type = lua_rawget(L, metatable_pos);
		ceph_assert(type != LUA_TNIL);
    auto old_it = reinterpret_cast<typename MapType::iterator*>(lua_touserdata(L, -1));
    // verify we are not mid-iteration
    if (*old_it != end_it) {
      luaL_error(L, "Trying to iterate '%s' before previous iteration finished", name.data());
      return nullptr;
    } 
    // we use the same memory buffer
    new_it = old_it;
    *new_it = start_it;
    // push the userdata so it could be tied to the metatable
    lua_pushlightuserdata(L, new_it);
    userdata_pos = lua_gettop(L);
  } else {
    // new metatable
    auto it_buff = lua_newuserdata(L, sizeof(Iterator));
    userdata_pos = lua_gettop(L);
    new_it = new (it_buff) Iterator(start_it);
  }
  // push the metatable again so it could be tied to the userdata
  lua_pushvalue(L, metatable_pos);
  // store the iterator pointer in the metatable
  lua_pushliteral(L, "__iterator");
  lua_pushlightuserdata(L, new_it);
  lua_rawset(L, metatable_pos); 
  // add indication that we are "mid iteration"
  // add "tostring" closure to metatable
  lua_pushliteral(L, "__tostring");
  lua_pushlightuserdata(L, new_it);
  lua_pushcclosure(L, [](lua_State* L) {
      // the key of the table is expected to be convertible to char*
      static_assert(std::is_constructible<typename MapType::key_type, const char*>());
      auto iter = reinterpret_cast<Iterator*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
      ceph_assert(iter);
      pushstring(L, (*iter)->first);
      return ONE_RETURNVAL;
    }, ONE_UPVAL);
  lua_rawset(L, metatable_pos);
  // define a finalizer of the iterator
  lua_pushliteral(L, "__gc");
  lua_pushlightuserdata(L, new_it);
  lua_pushcclosure(L, [](lua_State* L) {
      auto iter = reinterpret_cast<Iterator*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
      ceph_assert(iter);
      iter->~Iterator();
      return NO_RETURNVAL;
    }, ONE_UPVAL);
  lua_rawset(L, metatable_pos);
  // tie userdata and metatable
  lua_setmetatable(L, userdata_pos);
  return new_it;
}

// when a value of a table is erased while the table is being iterated
// this function allows updating the stored iterator with a new valid one
template<typename MapType>
void update_erased_iterator(lua_State* L, const std::string_view name, const typename MapType::iterator& old_it, const typename MapType::iterator& new_it) {
  // a metatable exists for the iterator
  if (luaL_getmetatable(L, get_iterator_name(name).c_str()) != LUA_TNIL) {
    const auto metatable_pos = lua_gettop(L);
    lua_pushliteral(L, "__iterator");
    if (lua_rawget(L, metatable_pos) != LUA_TNIL) {
      // an iterator was stored
      auto stored_it = reinterpret_cast<typename MapType::iterator*>(lua_touserdata(L, -1));
      ceph_assert(stored_it);
      if (old_it == *stored_it) {
        // changed the stored iterator to the iteator
        *stored_it = new_it;
      }
    }
  }
}

// __newindex implementation for any map type holding strings
// or other types constructable from "char*"
// this function allow deletion of an entry by setting "nil" to the entry
// it also limits the size of the entry: key + value cannot exceed MAX_LUA_VALUE_SIZE
// and limits the number of entries in the map, to not exceed MAX_LUA_KEY_ENTRIES
template<typename MapType=std::map<std::string, std::string>>
int StringMapWriteableNewIndex(lua_State* L) {
  static_assert(std::is_constructible<typename MapType::key_type, const char*>());
  static_assert(std::is_constructible<typename MapType::mapped_type, const char*>());
  const auto name = table_name_upvalue(L);
  const auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
  ceph_assert(map);

  const char* index = luaL_checkstring(L, 2);

  if (lua_isnil(L, 3) == 0) {
    const char* value = luaL_checkstring(L, 3);
    if (strnlen(value, MAX_LUA_VALUE_SIZE) + strnlen(index, MAX_LUA_VALUE_SIZE)
        > MAX_LUA_VALUE_SIZE) {
      return luaL_error(L, "Lua maximum size of entry limit exceeded");
    } else if (map->size() > MAX_LUA_KEY_ENTRIES) {
      return luaL_error(L, "Lua max number of entries limit exceeded");
    } else {
      map->insert_or_assign(index, value);
    }
  } else {
    // erase the element. since in lua: "t[index] = nil" is removing the entry at "t[index]"
    if (const auto it = map->find(index); it != map->end()) {
      // index was found
      update_erased_iterator<MapType>(L, name, it, map->erase(it));
    }
  }

  return NO_RETURNVAL;
}

// implements the lua next() function for iterating over a table
// first argument is a table and the second argument is an index in this table
// returns the next index of the table and its associated value
// when input index is nil, the function returns the initial value and index
// when the it reaches the last entry of the table it return nil as the index and value
template<typename MapType, typename ValueMetaType=void>
int next(lua_State* L) {
  using Iterator = typename MapType::iterator;
  const auto name = table_name_upvalue(L);
  auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
  ceph_assert(map);
  Iterator* next_it = nullptr;

  if (lua_isnil(L, 2)) {
    // pop the 2 nils
    lua_pop(L, 2);
    // create userdata
    next_it = create_iterator_metadata<MapType>(L, name, map->begin(), map->end());
  } else {
    next_it = reinterpret_cast<Iterator*>(lua_touserdata(L, 2));
    *next_it = std::next(*next_it);
  }
  ceph_assert(next_it);

  if (*next_it == map->end()) {
    // index of the last element was provided
    lua_pushnil(L);
    lua_pushnil(L);
    return TWO_RETURNVALS;
    // return nil, nil
  }

  // key (userdata iterator) is already on the stack
  // push the value
	using ValueType = typename MapType::mapped_type;
	auto& value = (*next_it)->second;
  if constexpr(std::is_constructible<std::string, ValueType>()) {
    // as an std::string
    pushstring(L, value);
  } else if constexpr(is_variant<ValueType>()) {
    // as an std::variant
    std::visit([L](auto&& value) { pushvalue(L, value); }, value);
  } else {
    // as a metatable
    create_metatable<ValueMetaType>(L, name, (*next_it)->first,
        false, &(value));
  }
  // return key, value
  return TWO_RETURNVALS;
}

// a generic "Pairs" utility function that could be used as the metatable's "PairsClosure" function
template<typename MapType, MetaTableClosure Next=next<MapType>>
int Pairs(lua_State* L) {
    const auto name = table_name_upvalue(L);
    auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    ceph_assert(map);
    pushstring(L, name);
    lua_pushlightuserdata(L, map);
    lua_pushcclosure(L, Next, TWO_UPVALS); // push the "next()" function
    lua_pushnil(L);                        // indicate this is the first call
    // return next, nil

    return TWO_RETURNVALS;
}


template<typename MapType=std::map<std::string, std::string>,
  MetaTableClosure NewIndex=EmptyMetaTable::NewIndexClosure>
struct StringMapMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    const auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    const auto it = map->find(std::string(index));
    if (it == map->end()) {
      lua_pushnil(L);
    } else {
      pushstring(L, it->second);
    }
    return ONE_RETURNVAL;
  }

  static int NewIndexClosure(lua_State* L) {
    return NewIndex(L);
  }

  static int PairsClosure(lua_State* L) {
    return Pairs<MapType>(L);
  }
  
  static int LenClosure(lua_State* L) {
    const auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));

    lua_pushinteger(L, map->size());

    return ONE_RETURNVAL;
  }
};

} // namespace rgw::lua

