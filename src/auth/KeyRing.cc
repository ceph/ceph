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

#include <errno.h>
#include <map>

#include "config.h"
#include "include/str_list.h"

#include "Crypto.h"
#include "auth/KeyRing.h"

#define DOUT_SUBSYS auth
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "auth: "


using namespace std;

KeyRing g_keyring;

bool KeyRing::load(const char *filename_list)
{
  string k = filename_list;
  string filename;
  list<string> ls;
  get_str_list(k, ls);
  bufferlist bl;
  bool loaded = false;
  for (list<string>::iterator p = ls.begin(); p != ls.end(); p++) {
    // subst in home dir?
    size_t pos = p->find("~/");
    if (pos != string::npos)
      p->replace(pos, 1, getenv("HOME"));

    if (bl.read_file(p->c_str(), true) == 0) {
      loaded = true;
      filename = *p;
      break;
    }
  }
  if (!loaded) {
    dout(0) << "can't open key file(s) " << filename_list << dendl;
    return false;
  }

  bufferlist::iterator p = bl.begin();
  decode(p);

  dout(1) << "loaded key file " << filename << dendl;
  return true;
}

void KeyRing::print(ostream& out)
{
  for (map<EntityName, EntityAuth>::iterator p = keys.begin();
       p != keys.end();
       ++p) {
    out << p->first << "\t" << p->second.auth_uid << std::endl;
    out << "\tkey: " << p->second.key << std::endl;

    for (map<string, bufferlist>::iterator q = p->second.caps.begin();
	 q != p->second.caps.end();
	 ++q) {
      bufferlist::iterator dataiter = q->second.begin();
      string caps;
      ::decode(caps, dataiter);
      out << "\tcaps: [" << q->first << "] " << caps << std::endl;
    }
  }
}

void KeyRing::import(KeyRing& other)
{
  for (map<EntityName, EntityAuth>::iterator p = other.keys.begin();
       p != other.keys.end();
       ++p) {
    dout(10) << " importing " << p->first << " " << p->second << dendl;
    keys[p->first] = p->second;
  }
}


