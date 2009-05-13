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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, const char **argv) 
{
  if (rados_initialize(argc, argv)) {
    printf("error initializing\n");
    exit(1);
  }

  time_t tm;
  char buf[128], buf2[128];

  time(&tm);
  snprintf(buf, 128, "%s", ctime(&tm));

  struct ceph_object oid;
  memset(&oid, 0, sizeof(oid));
  oid.ino = 0x2010;

  rados_write(&oid, buf, 0, strlen(buf) + 1);
  rados_exec(&oid, "code", 0, 128, buf, 128);
  printf("exec result=%s\n", buf);
  int size = rados_read(&oid, buf2, 0, 128);

  printf("read result=%s\n", buf2);
  printf("size=%d\n", size);

  rados_deinitialize();

  return 0;
}

