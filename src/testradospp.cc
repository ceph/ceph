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
  bl.append(buf, strlen(buf) + 1);

  object_t oid;
  memset(&oid, 0, sizeof(oid));
  oid.ino = 0x2010;

  rados_pool_t pool;
  int r = rados.open_pool("data", &pool);
  printf("open pool result = %d, pool = %d\n", r, pool);

  rados.write(pool, oid, 0, bl, bl.length());
  rados.exec(pool, oid, "test", "foo", bl, bl.length(), bl2, 1024);
  printf("exec result=%s\n", bl2.c_str());
  int size = rados.read(pool, oid, 0, bl2, 128);

  rados.close_pool(pool);

  cout << "read result=" << bl2.c_str() << std::endl;
  cout << "size=" << size << std::endl;

  return 0;
}

