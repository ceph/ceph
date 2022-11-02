#include "rgw_lua_data_filter.h"
#include "rgw_lua_utils.h"
#include "rgw_lua_request.h"
#include "rgw_lua_background.h"
#include "rgw_process_env.h"
#include <lua.hpp>

namespace rgw::lua {

void push_bufferlist_byte(lua_State* L, bufferlist::iterator& it) {
    char byte[1];
    it.copy(1, byte);
    lua_pushlstring(L, byte, 1);
}

struct BufferlistMetaTable : public EmptyMetaTable {

  static std::string TableName() {return "Data";}
  static std::string Name() {return TableName() + "Meta";}
  
  static int IndexClosure(lua_State* L) {
    auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(1)));
    const auto index = luaL_checkinteger(L, 2);
    if (index <= 0 || index > bl->length()) {
      // lua arrays start from 1
      lua_pushnil(L);
      return ONE_RETURNVAL;
    }
    auto it = bl->begin(index-1);
    if (it != bl->end()) {
      push_bufferlist_byte(L, it);
    } else {
      lua_pushnil(L);
    }
    
    return ONE_RETURNVAL;
  }

  static int PairsClosure(lua_State* L) {
    auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(1)));
    ceph_assert(bl);
    lua_pushlightuserdata(L, bl);
    lua_pushcclosure(L, stateless_iter, ONE_UPVAL); // push the stateless iterator function
    lua_pushnil(L);                                 // indicate this is the first call
    // return stateless_iter, nil

    return TWO_RETURNVALS;
  }
  
  static int stateless_iter(lua_State* L) {
    // based on: http://lua-users.org/wiki/GeneralizedPairsAndIpairs
    auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(1)));
    lua_Integer index;
    if (lua_isnil(L, -1)) {
      index = 1;
    } else {
      index = luaL_checkinteger(L, -1) + 1;
    }

    // lua arrays start from 1
    auto it = bl->begin(index-1);

    if (index > bl->length()) {
      // index of the last element was provided
      lua_pushnil(L);
      lua_pushnil(L);
      // return nil, nil
    } else {
      lua_pushinteger(L, index);
      push_bufferlist_byte(L, it);
      // return key, value
    }

    return TWO_RETURNVALS;
  }
  
  static int LenClosure(lua_State* L) {
    const auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(1)));

    lua_pushinteger(L, bl->length());

    return ONE_RETURNVAL;
  }
};

int RGWObjFilter::execute(bufferlist& bl, off_t offset, const char* op_name) const {
  auto L = luaL_newstate();
  lua_state_guard lguard(L);

  open_standard_libs(L);

  create_debug_action(L, s->cct);  

  // create the "Data" table
  create_metatable<BufferlistMetaTable>(L, true, &bl);
  lua_getglobal(L, BufferlistMetaTable::TableName().c_str());
  ceph_assert(lua_istable(L, -1));

  // create the "Request" table
  request::create_top_metatable(L, s, op_name);

  // create the "Offset" variable
  lua_pushinteger(L, offset);
  lua_setglobal(L, "Offset");

  if (s->penv.lua_background) {
    // create the "RGW" table
    s->penv.lua_background->create_background_metatable(L);
    lua_getglobal(L, rgw::lua::RGWTable::TableName().c_str());
    ceph_assert(lua_istable(L, -1));
  }

  try {
    // execute the lua script
    if (luaL_dostring(L, script.c_str()) != LUA_OK) {
      const std::string err(lua_tostring(L, -1));
      ldpp_dout(s, 1) << "Lua ERROR: " << err << dendl;
      return -EINVAL;
    }
  } catch (const std::runtime_error& e) {
    ldpp_dout(s, 1) << "Lua ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RGWGetObjFilter::handle_data(bufferlist& bl,
                  off_t bl_ofs,
                  off_t bl_len) {
  filter.execute(bl, bl_ofs, "get_obj");
  // return value is ignored since we don't want to fail execution if lua script fails
  return RGWGetObj_Filter::handle_data(bl, bl_ofs, bl_len);
}

int RGWPutObjFilter::process(bufferlist&& data, uint64_t logical_offset) {
  filter.execute(data, logical_offset, "put_obj");
  // return value is ignored since we don't want to fail execution if lua script fails
  return rgw::putobj::Pipe::process(std::move(data), logical_offset); 
}

} // namespace rgw::lua

