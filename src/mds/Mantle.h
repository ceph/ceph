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

#ifndef CEPH_MANTLE_H
#define CEPH_MANTLE_H

#include <string_view>

#include <lua.hpp>
#include <vector>
#include <map>
#include <string>

#include "mdstypes.h"

class Mantle {
  public:
    Mantle();
    ~Mantle() { if (L) lua_close(L); }
    int balance(std::string_view script,
                mds_rank_t whoami,
                const std::vector <std::map<std::string, double>> &metrics,
                std::map<mds_rank_t,double> &my_targets);

  protected:
    lua_State *L;
};

#endif
