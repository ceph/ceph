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
#define dout_prefix *_dout << "auth: "


using namespace std;

KeyRing g_keyring;

int KeyRing::load(const char *filename)
{
  if (!filename)
    return -EINVAL;

  bufferlist bl;
  int ret = bl.read_file(filename);
  if (ret) {
    derr << "error reading file: " << filename << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    decode(iter);
  }
  catch (const buffer::error &err) {
    derr << "error parsing file " << filename << dendl;
    return -EIO;
  }

  dout(2) << "KeyRing::load: loaded key file " << filename << dendl;
  return 0;
}

void KeyRing::print(ostream& out)
{
  for (map<EntityName, EntityAuth>::iterator p = keys.begin();
       p != keys.end();
       ++p) {
    out << p->first << std::endl;
    out << "\t key: " << p->second.key << std::endl;
    out << "\tauid: " << p->second.auid << std::endl;

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


