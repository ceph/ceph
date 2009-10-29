// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __MONCAPS_H
#define __MONCAPS_H

#include "include/types.h"

#define MON_CAP_R 0x1
#define MON_CAP_W 0x2
#define MON_CAP_X 0x4

#define MON_CAP_RW  (MON_CAP_R | MON_CAP_W)
#define MON_CAP_RX  (MON_CAP_R | MON_CAP_X)
#define MON_CAP_ALL (MON_CAP_R | MON_CAP_W | MON_CAP_X)

typedef __u8 rwx_t;

struct MonServiceCap {
  rwx_t allow;
  rwx_t deny;
  MonServiceCap() : allow(0), deny(0) {}
};

class MonCaps {
  rwx_t default_action;
  map<int, MonServiceCap> services_map;
  bool get_next_token(string s, size_t& pos, string& token);
  bool is_rwx(string& token, rwx_t& cap_val);
  int get_service_id(string& token);
  bool allow_all;
public:
  MonCaps() : default_action(0), allow_all(false) {}
  bool parse(bufferlist::iterator& iter);
  rwx_t get_caps(int service);
  void set_allow_all(bool allow) { allow_all = allow; }
};

#endif
