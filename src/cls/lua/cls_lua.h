#ifndef CEPH_CLS_LUA_H
#define CEPH_CLS_LUA_H

#include <lua.hpp>
#include "include/types.h"

#define LOG_LEVEL_DEFAULT 10

int luaopen_bufferlist(lua_State *L);

bufferlist *clslua_checkbufferlist(lua_State *L, int pos = 1);
bufferlist *clslua_pushbufferlist(lua_State *L, bufferlist *set);

#endif
