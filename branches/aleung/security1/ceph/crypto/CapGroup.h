// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#ifndef __CAPGROUP_H
#define __CAPGROUP_H

#include<iostream>
using namespace std;

#include "include/types.h"

class CapGroup {
 private:
  gid_t group_id;
  list<uid_t> users;

 public:
  friend class OSD;
  friend class Locker;
  CapGroup () { }
  CapGroup (gid_t id) { group_id = id; }
  
  gid_t get_gid() { return group_id; }
  void set_gid(gid_t id) { group_id = id; }

  void add_user(uid_t user) {
    users.push_back(user);
  }
  void remove_user(uid_t user) {
    users.remove(user);
  }

  bool contains(uid_t user) {
    for (list<uid_t>::iterator ui = users.begin();
	 ui != users.end();
	 ui++) {
      //uid_t test& = *ui;
      if (*ui == user)
	return true;
    }
    return false;
  }
};

#endif
