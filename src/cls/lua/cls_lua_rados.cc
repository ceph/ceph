/*
 * Lua Bindings for RADOS Object Class
 */
#include "objclass/objclass.h"
#include "lua_core/lua_core.h"

CLS_VER(1,0)
CLS_NAME(lua)

/*
 * cls_cxx_create
 */
static int clslua_create(lua_State *lua)
{
  cls_method_context_t hctx = clslua_get_hctx(lua);
  int exclusive = lua_toboolean(lua, 1);

  int ret = cls_cxx_create(hctx, exclusive);
  return clslua_opresult(lua, (ret == 0), ret, 0);
}

/*
 * cls_cxx_remove
 */
static int clslua_remove(lua_State *lua)
{
  cls_method_context_t hctx = clslua_get_hctx(lua);

  int ret = cls_cxx_remove(hctx);
  return clslua_opresult(lua, (ret == 0), ret, 0);
}

/*
 * cls_cxx_stat
 */
static int clslua_stat(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);

  uint64_t size;
  time_t mtime;
  int ret = cls_cxx_stat(hctx, &size, &mtime);
  if (!ret) {
    lua_pushinteger(L, size);
    lua_pushinteger(L, mtime);
  }
  return clslua_opresult(L, (ret == 0), ret, 2);
}

/*
 * cls_cxx_read
 */
static int clslua_read(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  int offset = luaL_checkinteger(L, 1);
  int length = luaL_checkinteger(L, 2);
  bufferlist *bl = clslua_pushbufferlist(L, NULL);
  int ret = cls_cxx_read(hctx, offset, length, bl);
  return clslua_opresult(L, (ret >= 0), ret, 1);
}

/*
 * cls_cxx_write
 */
static int clslua_write(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  int offset = luaL_checkinteger(L, 1);
  int length = luaL_checkinteger(L, 2);
  bufferlist *bl = clslua_checkbufferlist(L, 3);
  int ret = cls_cxx_write(hctx, offset, length, bl);
  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * cls_cxx_write_full
 */
static int clslua_write_full(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  bufferlist *bl = clslua_checkbufferlist(L, 1);
  int ret = cls_cxx_write_full(hctx, bl);
  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * cls_cxx_getxattr
 */
static int clslua_getxattr(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *name = luaL_checkstring(L, 1);
  bufferlist  *bl = clslua_pushbufferlist(L, NULL);
  int ret = cls_cxx_getxattr(hctx, name, bl);
  return clslua_opresult(L, (ret >= 0), ret, 1);
}

/*
 * cls_cxx_getxattrs
 */
static int clslua_getxattrs(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);

  map<string, bufferlist> attrs;
  int ret = cls_cxx_getxattrs(hctx, &attrs);
  if (ret < 0)
    return clslua_opresult(L, 0, ret, 0);

  lua_createtable(L, 0, attrs.size());

  for (auto it = attrs.cbegin(); it != attrs.cend(); it++) {
    lua_pushstring(L, it->first.c_str());
    bufferlist  *bl = clslua_pushbufferlist(L, NULL);
    *bl = it->second; // xfer ownership... will be GC'd
    lua_settable(L, -3);
  }

  return clslua_opresult(L, 1, ret, 1);
}

/*
 * cls_cxx_setxattr
 */
static int clslua_setxattr(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *name = luaL_checkstring(L, 1);
  bufferlist *bl = clslua_checkbufferlist(L, 2);
  int ret = cls_cxx_setxattr(hctx, name, bl);
  return clslua_opresult(L, (ret == 0), ret, 1);
}

/*
 * cls_cxx_map_get_val
 */
static int clslua_map_get_val(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *key = luaL_checkstring(L, 1);
  bufferlist  *bl = clslua_pushbufferlist(L, NULL);
  int ret = cls_cxx_map_get_val(hctx, key, bl);
  return clslua_opresult(L, (ret == 0), ret, 1);
}

/*
 * cls_cxx_map_set_val
 */
static int clslua_map_set_val(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *key = luaL_checkstring(L, 1);
  bufferlist *val = clslua_checkbufferlist(L, 2);
  int ret = cls_cxx_map_set_val(hctx, key, val);
  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * cls_cxx_map_clear
 */
static int clslua_map_clear(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  int ret = cls_cxx_map_clear(hctx);
  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * cls_cxx_map_get_keys
 */
static int clslua_map_get_keys(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *start_after = luaL_checkstring(L, 1);
  int max_to_get = luaL_checkinteger(L, 2);

  std::set<string> keys;
  int ret = cls_cxx_map_get_keys(hctx, start_after, max_to_get, &keys);
  if (ret < 0)
    return clslua_opresult(L, 0, ret, 0);

  lua_createtable(L, 0, keys.size());

  for (auto it = keys.cbegin(); it != keys.cend(); it++) {
    const std::string& key = *it;
    lua_pushstring(L, key.c_str());
    lua_pushboolean(L, 1);
    lua_settable(L, -3);
  }

  return clslua_opresult(L, 1, ret, 1);
}

/*
 * cls_cxx_map_get_vals
 */
static int clslua_map_get_vals(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *start_after = luaL_checkstring(L, 1);
  const char *filter_prefix= luaL_checkstring(L, 2);
  int max_to_get = luaL_checkinteger(L, 3);

  map<string, bufferlist> kvpairs;
  int ret = cls_cxx_map_get_vals(hctx, start_after, filter_prefix,
      max_to_get, &kvpairs);
  if (ret < 0)
    return clslua_opresult(L, 0, ret, 0);

  lua_createtable(L, 0, kvpairs.size());

  for (auto it = kvpairs.cbegin(); it != kvpairs.cend(); it++) {
    lua_pushstring(L, it->first.c_str());
    bufferlist  *bl = clslua_pushbufferlist(L, NULL);
    *bl = it->second; // xfer ownership... will be GC'd
    lua_settable(L, -3);
  }

  return clslua_opresult(L, 1, ret, 1);
}

/*
 * cls_cxx_map_read_header
 */
static int clslua_map_read_header(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  bufferlist *bl = clslua_pushbufferlist(L, NULL);
  int ret = cls_cxx_map_read_header(hctx, bl);
  return clslua_opresult(L, (ret >= 0), ret, 1);
}

/*
 * cls_cxx_map_write_header
 */
static int clslua_map_write_header(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  bufferlist *bl = clslua_checkbufferlist(L, 1);
  int ret = cls_cxx_map_write_header(hctx, bl);
  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * cls_cxx_map_set_vals
 */
static int clslua_map_set_vals(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  luaL_checktype(L, 1, LUA_TTABLE);

  map<string, bufferlist> kvpairs;

  lua_pushnil(L);
  while (lua_next(L, 1) != 0) {
    /*
     * In the case of a numeric key a copy is made on the stack because
     * converting to a string would otherwise manipulate the original key and
     * cause problems for iteration.
     */
    string key;
    int type_code = lua_type(L, -2);
    switch (type_code) {
      case LUA_TSTRING:
        key.assign(lua_tolstring(L, -2, NULL));
        break;

      case LUA_TNUMBER:
        lua_pushvalue(L, -2);
        key.assign(lua_tolstring(L, -1, NULL));
        lua_pop(L, 1);
        break;

      default:
        lua_pushfstring(L, "map_set_vals: invalid key type (%s)",
            lua_typename(L, type_code));
        return clslua_opresult(L, 0, -EINVAL, 0, true);
    }

    bufferlist val;
    type_code = lua_type(L, -1);
    switch (type_code) {
      case LUA_TSTRING:
        {
          size_t len;
          const char *data = lua_tolstring(L, -1, &len);
          val.append(data, len);
        }
        break;

      default:
        lua_pushfstring(L, "map_set_vals: invalid val type (%s) for key (%s)",
            lua_typename(L, type_code), key.c_str());
        return clslua_opresult(L, 0, -EINVAL, 0, true);
    }

    kvpairs[key] = val;

    lua_pop(L, 1);
  }

  int ret = cls_cxx_map_set_vals(hctx, &kvpairs);

  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * cls_cxx_map_remove_key
 */
static int clslua_map_remove_key(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  const char *key = luaL_checkstring(L, 1);
  int ret = cls_cxx_map_remove_key(hctx, key);
  return clslua_opresult(L, (ret == 0), ret, 0);
}

/*
 * cls_current_version
 */
static int clslua_current_version(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  uint64_t version = cls_current_version(hctx);
  lua_pushinteger(L, version);
  return clslua_opresult(L, 1, 0, 1);
}

/*
 * cls_current_subop_num
 */
static int clslua_current_subop_num(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  int num = cls_current_subop_num(hctx);
  lua_pushinteger(L, num);
  return clslua_opresult(L, 1, 0, 1);
}

/*
 * cls_current_subop_version
 */
static int clslua_current_subop_version(lua_State *L)
{
  cls_method_context_t hctx = clslua_get_hctx(L);
  string s;
  cls_cxx_subop_version(hctx, &s);
  lua_pushstring(L, s.c_str());
  return clslua_opresult(L, 1, 0, 1);
}

/*
 * Entrypoint for the shared lib when dlopen is called
 */
void __cls_init()
{
  CLS_LOG(20, "Loaded lua RADOS class!");

  /* 
   * Functions to add to clslua_lib
   */
  list<luaL_Reg> functions;

  // data
  functions.push_back({"create", clslua_create});
  functions.push_back({"remove", clslua_remove});
  functions.push_back({"stat", clslua_stat});
  functions.push_back({"read", clslua_read});
  functions.push_back({"write", clslua_write});
  functions.push_back({"write_full", clslua_write_full});
  
  // xattr
  functions.push_back({"getxattr", clslua_getxattr});
  functions.push_back({"getxattrs", clslua_getxattrs});
  functions.push_back({"setxattr", clslua_setxattr});
  
  // omap
  functions.push_back({"map_clear", clslua_map_clear});
  functions.push_back({"map_get_keys", clslua_map_get_keys});
  functions.push_back({"map_get_vals", clslua_map_get_vals});
  functions.push_back({"map_read_header", clslua_map_read_header});
  functions.push_back({"map_write_header", clslua_map_write_header});
  functions.push_back({"map_get_val", clslua_map_get_val});
  functions.push_back({"map_set_val", clslua_map_set_val});
  functions.push_back({"map_set_vals", clslua_map_set_vals});
  functions.push_back({"map_remove_key", clslua_map_remove_key});

  // env
  functions.push_back({"current_version", clslua_current_version});
  functions.push_back({"current_subop_num", clslua_current_subop_num});
  functions.push_back({"current_subop_version", clslua_current_subop_version});

  /* 
   * Lua files with helper functions to add to the environment
   */
  list<string> env;

  cls_init("lua", "objclass", functions, env);
}  



