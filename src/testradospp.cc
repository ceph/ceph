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

#include "include/librados.h"

#include <iostream>

#include <stdlib.h>
#include <time.h>

void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  for (int i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

int main(int argc, const char **argv) 
{
  Rados rados;
  if (!rados.initialize(0, NULL)) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  time_t tm;
  bufferlist bl, bl2;
  char buf[128];

  time(&tm);
  snprintf(buf, 128, "%s", ctime(&tm));
  bl.append(buf, strlen(buf));

  object_t oid;
  memset(&oid, 0, sizeof(oid));
  oid.ino = 0x2010;

  rados_pool_t pool;
  int r = rados.open_pool("data", &pool);
  cout << "open pool result = " << r << " pool = " << pool << std::endl;

  rados.write(pool, oid, 0, bl, bl.length());

  r = rados.exec(pool, oid, "crypto", "md5", bl, bl2);
  cout << "exec returned " << r << std::endl;
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

  int size = rados.read(pool, oid, 0, bl2, 128);
  cout << "read result=" << bl2.c_str() << std::endl;
  cout << "size=" << size << std::endl;

  vector<string> vec;
  r = rados.list(pool, vec);
  cout << "read result=" << r << " pool=" << hex << pool << dec << std::endl;
#if 0
  r = rados.remove(pool, oid);
  cout << "remove result=" << r << std::endl;
  rados.close_pool(pool);
#endif

  return 0;
}

