// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "include/types.h"
#include "include/librados.hpp"

using namespace librados;

#include <iostream>

#include <stdlib.h>
#include <time.h>

void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  str[0] = '\0';
  for (int i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

class C_Watch : public Rados::WatchCtx {
public:
  C_Watch() {}
  void notify(uint8_t opcode, uint64_t ver) {
    cout << "C_Watch::notify() opcode=" << (int)opcode << " ver=" << ver << std::endl;
  }
};

int main(int argc, const char **argv) 
{
  Rados rados;
  if (rados.initialize(argc, argv) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  cout << "rados_initialize completed" << std::endl;
  cout << "*** press enter to continue ***" << std::endl;
  getchar();

  time_t tm;
  bufferlist bl, bl2, blf;
  char buf[128];

  time(&tm);
  snprintf(buf, 128, "%s", ctime(&tm));
  bl.append(buf, strlen(buf));
  blf.append(buf, 16);

  const char *oid = "bar";

  pool_t pool;

  int r = rados.open_pool("data", &pool);
  cout << "open pool result = " << r << " pool = " << pool << std::endl;

  r = rados.write(pool, oid, 0, bl, bl.length());
  uint64_t objver = rados.get_last_version(pool);
  cout << "rados.write returned " << r << " last_ver=" << objver << std::endl;

  uint64_t handle;
  C_Watch wc;
  r = rados.watch(pool, oid, objver, &handle, &wc);
  cout << "rados.watch returned " << r << std::endl;

  cout << "*** press enter to continue ***" << std::endl;
  getchar();
  r = rados.notify(pool, oid, objver);
  cout << "rados.notify returned " << r << std::endl;
  cout << "*** press enter to continue ***" << std::endl;
  getchar();

  r = rados.notify(pool, oid, objver);
  cout << "rados.notify returned " << r << std::endl;
  cout << "*** press enter to continue ***" << std::endl;
  getchar();

  r = rados.unwatch(pool, oid, handle);
  cout << "rados.unwatch returned " << r << std::endl;
  cout << "*** press enter to continue ***" << std::endl;
  getchar();

  r = rados.notify(pool, oid, objver);
  cout << "rados.notify returned " << r << std::endl;
  cout << "*** press enter to continue ***" << std::endl;
  getchar();
  rados.set_assert_version(pool, objver);

  r = rados.write(pool, oid, 0, bl, bl.length() - 1);
  cout << "rados.write returned " << r << std::endl;

  exit(0);
  r = rados.write(pool, oid, 0, bl, bl.length() - 2);
  cout << "rados.write returned " << r << std::endl;
  r = rados.write(pool, oid, 0, bl, bl.length() - 3);
  cout << "rados.write returned " << r << std::endl;
  r = rados.write_full(pool, oid, blf);
  cout << "rados.write_full returned " << r << std::endl;
  r = rados.read(pool, oid, 0, bl, bl.length());
  cout << "rados.read returned " << r << std::endl;
  r = rados.trunc(pool, oid, 8);
  cout << "rados.trunc returned " << r << std::endl;
  r = rados.read(pool, oid, 0, bl, bl.length());
  cout << "rados.read returned " << r << std::endl;
  r = rados.exec(pool, oid, "crypto", "md5", bl, bl2);
  cout << "exec returned " << r <<  " buf size=" << bl2.length() << std::endl;
  const unsigned char *md5 = (const unsigned char *)bl2.c_str();
  char md5_str[bl2.length()*2 + 1];
  buf_to_hex(md5, bl2.length(), md5_str);
  cout << "md5 result=" << md5_str << std::endl;

  r = rados.exec(pool, oid, "crypto", "sha1", bl, bl2);
  cout << "exec returned " << r << std::endl;
  const unsigned char *sha1 = (const unsigned char *)bl2.c_str();
  char sha1_str[bl2.length()*2 + 1];
  buf_to_hex(sha1, bl2.length(), sha1_str);
  cout << "sha1 result=" << sha1_str << std::endl;

  r = rados.exec(pool, oid, "acl", "set", bl, bl2);
  r = rados.exec(pool, oid, "acl", "get", bl, bl2);
  cout << "exec returned " << r << std::endl;
  if (bl2.length() > 0) {
    cout << "attr=" << bl2.c_str() << std::endl;
  }

  int size = rados.read(pool, oid, 0, bl2, 128);
  cout << "read result=" << bl2.c_str() << std::endl;
  cout << "size=" << size << std::endl;

  const char *oid2 = "jjj10.rbd";
  r = rados.exec(pool, oid2, "rbd", "snap_list", bl, bl2);
  cout << "snap_list result=" << r << std::endl;
  r = rados.exec(pool, oid2, "rbd", "snap_add", bl, bl2);
  cout << "snap_add result=" << r << std::endl;

  if (r > 0) {
    char *s = bl2.c_str();
    for (int i=0; i<r; i++, s += strlen(s) + 1)
      cout << s << endl;
  }

  Rados::ListCtx ctx;
  rados.list_objects_open(pool, &ctx);
  int entries;
  do {
    list<string> vec;
    r = rados.list_objects_more(ctx, 2, vec);
    entries = vec.size();
    cout << "list result=" << r << " entries=" << entries << std::endl;
    list<string>::iterator iter;
    for (iter = vec.begin(); iter != vec.end(); ++iter) {
      cout << *iter << std::endl;
    }
  } while (entries);
  rados.list_objects_close(ctx);


  map<string, bufferlist> attrset;
  rados.getxattrs(pool, oid, attrset);

  map<string, bufferlist>::iterator it;
  for (it = attrset.begin(); it != attrset.end(); ++it) {
    cout << "xattr: " << it->first << std::endl;
  }
  
#if 0
  r = rados.remove(pool, oid);
  cout << "remove result=" << r << std::endl;
  rados.close_pool(pool);
#endif
  rados.shutdown();

  return 0;
}

