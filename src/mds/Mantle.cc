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
#include "common/Clock.h"
#include "CInode.h"

#include <fstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_balancer
#undef dout_prefix
#define dout_prefix *_dout << "mds.mantle "
#define mantle_dout(lvl) \
  do {\
    auto subsys = ceph_subsys_mds;\
    if ((dout_context)->_conf->subsys.should_gather(ceph_subsys_mds_balancer, lvl)) {\
      subsys = ceph_subsys_mds_balancer;\
    }\
    dout_impl(dout_context, ceph::dout::need_dynamic(subsys), lvl) dout_prefix

#define mantle_dendl dendl; } while (0)


static int dout_wrapper(lua_State *L)
{
  int level = luaL_checkinteger(L, 1);
  lua_concat(L, lua_gettop(L)-1);
  mantle_dout(ceph::dout::need_dynamic(level)) << lua_tostring(L, 2)
					       << mantle_dendl;
  return 0;
}

int Mantle::balance(std::string_view script,
                    mds_rank_t whoami,
                    const std::vector<std::map<std::string, double>> &metrics,
                    std::map<mds_rank_t, double> &my_targets)
{
  lua_settop(L, 0); /* clear the stack */

  /* load the balancer */
  if (luaL_loadstring(L, script.data())) {
    mantle_dout(0) << "WARNING: mantle could not load balancer: "
            << lua_tostring(L, -1) << mantle_dendl;
    return -EINVAL;
  }

  /* tell the balancer which mds is making the decision */
  lua_pushinteger(L, (lua_Integer)whoami);
  lua_setglobal(L, "whoami");

  /* global mds metrics to hold all dictionaries */
  lua_newtable(L);

  /* push name of mds (i) and its metrics onto Lua stack */
  for (size_t i=0; i < metrics.size(); i++) {
    lua_newtable(L);

    /* push values into this mds's table; setfield assigns key/pops val */
    for (const auto &it : metrics[i]) {
      lua_pushnumber(L, it.second);
      lua_setfield(L, -2, it.first.c_str());
    }

    /* in global mds table at stack[-3], set k=stack[-1] to v=stack[-2] */
    lua_seti(L, -2, i);
  }

  /* set the name of the global mds table */
  lua_setglobal(L, "mds");

  assert(lua_gettop(L) == 1);
  if (lua_pcall(L, 0, 1, 0) != LUA_OK) {
    mantle_dout(0) << "WARNING: mantle could not execute script: "
            << lua_tostring(L, -1) << mantle_dendl;
    return -EINVAL;
  }

  /* parse response by iterating over Lua stack */
  if (lua_istable(L, -1) == 0) {
    mantle_dout(0) << "WARNING: mantle script returned a malformed response" << mantle_dendl;
    return -EINVAL;
  }

  /* fill in return value */
  for (lua_pushnil(L); lua_next(L, -2); lua_pop(L, 1)) {
    if (!lua_isinteger(L, -2) || !lua_isnumber(L, -1)) {
      mantle_dout(0) << "WARNING: mantle script returned a malformed response" << mantle_dendl;
      return -EINVAL;
    }
    mds_rank_t rank(lua_tointeger(L, -2));
    my_targets[rank] = lua_tonumber(L, -1);
  }

  return 0;
}

Mantle::Mantle (void)
{
  /* build lua vm state */
  L = luaL_newstate();
  if (!L) {
    mantle_dout(0) << "WARNING: mantle could not load Lua state" << mantle_dendl;
    throw std::bad_alloc();
  }

  /* balancer policies can use basic Lua functions */
  luaopen_base(L);
  luaopen_coroutine(L);
  luaopen_string(L);
  luaopen_math(L);
  luaopen_table(L);
  luaopen_utf8(L);

  /* setup debugging */
  lua_register(L, "BAL_LOG", dout_wrapper);
}
