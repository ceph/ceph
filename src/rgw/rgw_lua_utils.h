#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <ctime>
#include <lua.hpp>

#include "include/common_fwd.h"

namespace rgw::lua {

// push ceph time in string format: "%Y-%m-%d %H:%M:%S"
template <typename CephTime>
void pushtime(lua_State* L, const CephTime& tp)
{
  const auto tt = CephTime::clock::to_time_t(tp);
  const auto tm = *std::localtime(&tt);
  char buff[64];
  std::strftime(buff, sizeof(buff), "%Y-%m-%d %H:%M:%S", &tm);
  lua_pushstring(L, buff);
}

static inline void pushstring(lua_State* L, std::string_view str)
{
  lua_pushlstring(L, str.data(), str.size());
}

static inline void unsetglobal(lua_State* L, const char* name) 
{
  lua_pushnil(L);
  lua_setglobal(L, name);
}

// dump the lua stack to stdout
void stack_dump(lua_State* L);

class lua_state_guard {
  lua_State* l;
public:
  lua_state_guard(lua_State* _l) : l(_l) {}
  ~lua_state_guard() {lua_close(l);}
  void reset(lua_State* _l=nullptr) {l = _l;}
};

constexpr auto ONE_UPVAL    = 1;
constexpr auto TWO_UPVALS   = 2;
constexpr auto THREE_UPVALS = 3;
constexpr auto FOUR_UPVALS  = 4;
constexpr auto FIVE_UPVALS  = 5;

constexpr auto NO_RETURNVAL    = 0;
constexpr auto ONE_RETURNVAL    = 1;
constexpr auto TWO_RETURNVALS   = 2;
constexpr auto THREE_RETURNVALS = 3;
constexpr auto FOUR_RETURNVALS  = 4;
// utility functions to create a metatable
// and tie it to an unnamed table
//
// add an __index method to it, to allow reading values
// if "readonly" parameter is set to "false", it will also add
// a __newindex method to it, to allow writing values
// if the "toplevel" parameter is set to "true", it will name the
// table as well as the metatable, this would allow direct access from
// the lua script.
//
// The MetaTable is expected to be a class with the following members:
// Name (static function returning the unique name of the metatable)
// TableName (static function returning the unique name of the table - needed only for "toplevel" tables)
// Type (typename) - the type of the "upvalue" (the type that the meta table represent)
// IndexClosure (static function return "int" and accept "lua_State*") 
// NewIndexClosure (static function return "int" and accept "lua_State*") 
// e.g.
// struct MyStructMetaTable {
//   static std::string TableName() {
//     return "MyStruct";
//   }
//
//   using Type = MyStruct;
//
//   static int IndexClosure(lua_State* L) {
//     const auto value = reinterpret_cast<const Type*>(lua_touserdata(L, lua_upvalueindex(1)));
//     ...
//   }

//   static int NewIndexClosure(lua_State* L) {
//     auto value = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));
//     ...
//   }
// };
//

template<typename MetaTable, typename... Upvalues>
void create_metatable(lua_State* L, bool toplevel, Upvalues... upvalues)
{
  constexpr auto upvals_size = sizeof...(upvalues);
  const std::array<void*, upvals_size> upvalue_arr = {upvalues...};
  // create table
  lua_newtable(L);
  if (toplevel) {
    // duplicate the table to make sure it remain in the stack
    lua_pushvalue(L, -1);
    // give table a name (in cae of "toplevel")
    lua_setglobal(L, MetaTable::TableName().c_str());
  }
  // create metatable
  [[maybe_unused]] const auto rc = luaL_newmetatable(L, MetaTable::Name().c_str());
  lua_pushliteral(L, "__index");
  for (const auto upvalue : upvalue_arr) {
    lua_pushlightuserdata(L, upvalue);
  }
  lua_pushcclosure(L, MetaTable::IndexClosure, upvals_size);
  lua_rawset(L, -3);
  lua_pushliteral(L, "__newindex");
  for (const auto upvalue : upvalue_arr) {
    lua_pushlightuserdata(L, upvalue);
  }
  lua_pushcclosure(L, MetaTable::NewIndexClosure, upvals_size);
  lua_rawset(L, -3);
  lua_pushliteral(L, "__pairs");
  for (const auto upvalue : upvalue_arr) {
    lua_pushlightuserdata(L, upvalue);
  }
  lua_pushcclosure(L, MetaTable::PairsClosure, upvals_size);
  lua_rawset(L, -3);
  lua_pushliteral(L, "__len");
  for (const auto upvalue : upvalue_arr) {
    lua_pushlightuserdata(L, upvalue);
  }
  lua_pushcclosure(L, MetaTable::LenClosure, upvals_size);
  lua_rawset(L, -3);
  // tie metatable and table
  lua_setmetatable(L, -2);
}

template<typename MetaTable>
void create_metatable(lua_State* L, bool toplevel, std::unique_ptr<typename MetaTable::Type>& ptr)
{
  if (ptr) {
    create_metatable<MetaTable>(L, toplevel, reinterpret_cast<void*>(ptr.get()));
  } else {
    lua_pushnil(L);
  }
}

// following struct may be used as a base class for other MetaTable classes
// note, however, this is not mandatory to use it as a base
struct EmptyMetaTable {
  // by default everythinmg is "readonly"
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
// package.path = "<install_dir>/share/lua/5.3/?.lua"                                                                         │                         LuaRocks.
// package.cpath= "<install_dir>/lib/lua/5.3/?.so"
void set_package_path(lua_State* L, const std::string& install_dir);

// open standard lua libs and remove the following functions:
// os.exit()
// load()
// loadfile()
// loadstring()
// dofile()
// and the "debug" library
void open_standard_libs(lua_State* L);

}

