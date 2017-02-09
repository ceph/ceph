// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Michael Sevilla <mikesevilla3@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "mdstypes.h"
#include "MDSRank.h"
#include "Mantle.h"
#include "msg/Messenger.h"

#include <fstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_balancer
#undef DOUT_COND
#define DOUT_COND(cct, l) l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_balancer
#undef dout_prefix
#define dout_prefix *_dout << "mds.mantle "

int dout_wrapper(lua_State *L)
{
  #undef dout_prefix
  #define dout_prefix *_dout << "lua.balancer "

  /* Lua indexes the stack from the bottom up */
  int bottom = -1 * lua_gettop(L);
  if (!lua_isinteger(L, bottom) || bottom == 0) {
    dout(0) << "WARNING: BAL_LOG has no message" << dendl;
    return -EINVAL;
  }

  /* bottom of the stack is the log level */
  int level = lua_tointeger(L, bottom);

  /* rest of the stack is the message */
  string s = "";
  for (int i = bottom + 1; i < 0; i++)
    lua_isstring(L, i) ? s.append(lua_tostring(L, i)) : s.append("<empty>");

  dout(level) << s << dendl;
  return 0;
}

int Mantle::start()
{
  /* build lua vm state */
  L = luaL_newstate();
  if (!L) {
    dout(0) << "WARNING: mantle could not load Lua state" << dendl;
    return -ENOEXEC;
  }

  /* balancer policies can use basic Lua functions */
  luaopen_base(L);

  /* setup debugging */
  lua_register(L, "BAL_LOG", dout_wrapper);

  return 0;
}

int Mantle::execute(const string &script)
{
  if (L == NULL) {
    dout(0) << "ERROR: mantle was not started" << dendl;
    return -ENOENT;
  }

  /* load the balancer */
  if (luaL_loadstring(L, script.c_str())) {
    dout(0) << "WARNING: mantle could not load balancer: "
            << lua_tostring(L, -1) << dendl;
    return -EINVAL;
  }

  /* compile/execute balancer */
  int ret = lua_pcall(L, 0, LUA_MULTRET, 0);

  if (ret) {
    dout(0) << "WARNING: mantle could not execute script: "
            << lua_tostring(L, -1) << dendl;
    return -EINVAL;
  }

  return 0;
}

int Mantle::balance(const string &script,
                    mds_rank_t whoami,
                    const vector < map<string, double> > &metrics,
                    map<mds_rank_t,double> &my_targets)
{
  if (start() != 0)
    return -ENOEXEC;

  /* tell the balancer which mds is making the decision */
  lua_pushinteger(L, int(whoami));
  lua_setfield(L, -2, "whoami");

  /* global mds metrics to hold all dictionaries */
  lua_newtable(L);

  /* push name of mds (i) and its metrics onto Lua stack */
  for (unsigned i=0; i < metrics.size(); i++) {
    lua_pushinteger(L, i);
    lua_newtable(L);

    /* push values into this mds's table; setfield assigns key/pops val */
    for (map<string, double>::const_iterator it = metrics[i].begin();
         it != metrics[i].end();
         ++it) {
      lua_pushnumber(L, it->second);
      lua_setfield(L, -2, it->first.c_str());
    }

    /* in global mds table at stack[-3], set k=stack[-1] to v=stack[-2] */
    lua_rawset(L, -3);
  }

  /* set the name of the global mds table */
  lua_setglobal(L, "mds");

  int ret = execute(script);
  if (ret != 0) {
    lua_close(L);
    return ret;
  }

  /* parse response by iterating over Lua stack */
  if (lua_istable(L, -1) == 0) {
    dout(0) << "WARNING: mantle script returned a malformed response" << dendl;
    lua_close(L);
    return -EINVAL;
  }

  /* fill in return value */
  mds_rank_t it = mds_rank_t(0);
  for (lua_pushnil(L); lua_next(L, -2); lua_pop(L, 1)) {
    if (!lua_isnumber(L, -1)) {
      dout(0) << "WARNING: mantle script returned a malformed response" << dendl;
      lua_close(L);
      return -EINVAL;
    }
    my_targets[it] = (lua_tonumber(L, -1));
    it++;
  }

  lua_close(L);
  return 0;
}
