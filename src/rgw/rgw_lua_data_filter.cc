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
  static int IndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
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
    return Pairs<bufferlist, stateless_iter>(L);
  }
  
  static int stateless_iter(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
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
  lua_state_guard lguard(s->cct->_conf->rgw_lua_max_memory_per_state, s);
  auto L = lguard.get();
  if (!L) {
    ldpp_dout(s, 1) << "Failed to create state for Lua data context" << dendl;
    return -ENOMEM;
  }
  try {
    open_standard_libs(L);

    create_debug_action(L, s->cct);  

    // create the "Data" table
    static const char* data_metatable_name = "Data";
    create_metatable<BufferlistMetaTable>(L, "", data_metatable_name, true, &bl);
    lua_getglobal(L, data_metatable_name);
    ceph_assert(lua_istable(L, -1));

    // create the "Request" table
    request::create_top_metatable(L, s, op_name);

    // create the "Offset" variable
    lua_pushinteger(L, offset);
    lua_setglobal(L, "Offset");

    if (s->penv.lua.background) {
      // create the "RGW" table
      s->penv.lua.background->create_background_metatable(L);
    }

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

