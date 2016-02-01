#ifndef CEPH_CLS_LUA_H
#define CEPH_CLS_LUA_H

#include <lua.hpp>
#include "include/types.h"
#include "objclass/objclass.h"

#define LOG_LEVEL_DEFAULT 10

extern "C" {
  // built into liblua convenience library (see: src/lua)
  int luaopen_cmsgpack(lua_State *L);
}

/*
 * Input parameter encoding.
 */
enum InputEncoding {
  JSON_ENC,
  BUFFERLIST_ENC,
};

/*
 * Input/output for cls interfaces get stuffed in Ceph bufferlists
 */
int luaopen_bufferlist(lua_State *L);
bufferlist *clslua_checkbufferlist(lua_State *L, int pos = 1);
bufferlist *clslua_pushbufferlist(lua_State *L, bufferlist *set);

/*
 * Grabs the full method handler context
 */
cls_method_context_t clslua_get_hctx(lua_State *L);

/*
 * Handle result of a cls_cxx_* call
 */
int clslua_opresult(lua_State *L, int ok, int ret, int nargs,
    bool error_on_stack = false);

/*
 * Main handler. Proxies the Lua VM and the Lua-defined handler.
 */
int eval_generic(cls_method_context_t hctx, bufferlist *in, bufferlist *out,
    InputEncoding in_enc);

/*
 * Entrypoint into Lua VM base class
 */
void cls_init(string cls_name, list<luaL_Reg> clslua_lib_add);

#endif
