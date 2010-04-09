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
  int i, r;

  if (rados_initialize(argc, argv) < 0) {
    printf("error initializing\n");
    exit(1);
  }

  /* create a pool */
  r = rados_create_pool("foo");
  printf("rados_create_pool = %d\n", r);

  rados_pool_t pool;
  r = rados_open_pool("foo", &pool);
  printf("rados_open_pool = %d, pool = %p\n", r, pool);

  /* list objects */
  rados_list_ctx_t pctx;
  rados_pool_init_ctx(&pctx);
  const char *poolname;
  while (rados_pool_list_next(pool, &poolname, &pctx) == 0)
    printf("rados_pool_list_next got object '%s'\n", poolname);
  rados_pool_close_ctx(&pctx);

  /* snapshots */
  r = rados_snap_create(pool, "snap1");
  printf("rados_snap_create snap1 = %d\n", r);
  rados_snap_t snaps[10];
  r = rados_snap_list(pool, snaps, 10);
  for (i=0; i<r; i++) {
    char name[100];
    rados_snap_get_name(pool, snaps[i], name, sizeof(name));
    printf("rados_snap_list got snap %lld %s\n", snaps[i], name);
  }
  rados_snap_t snapid;
  r = rados_snap_lookup(pool, "snap1", &snapid);
  printf("rados_snap_lookup snap1 got %lld, result %d\n", snapid, r);
  r = rados_snap_remove(pool, "snap1");
  printf("rados_snap_remove snap1 = %d\n", r);

  /* sync io */
  time_t tm;
  char buf[128], buf2[128];
  time(&tm);
  snprintf(buf, 128, "%s", ctime(&tm));
  const char *oid = "foo_object";
  r = rados_write(pool, oid, 0, buf, strlen(buf) + 1);
  printf("rados_write = %d\n", r);
  r = rados_read(pool, oid, 0, buf2, sizeof(buf2));
  printf("rados_read = %d\n", r);
  if (memcmp(buf, buf2, r))
    printf("*** content mismatch ***\n");

  /* attrs */
  r = rados_setxattr(pool, oid, "attr1", "bar", 3);
  printf("rados_setxattr attr1=bar = %d\n", r);
  char val[10];
  r = rados_getxattr(pool, oid, "attr1", val, sizeof(val));
  printf("rados_getxattr attr1 = %d\n", r);
  if (memcmp(val, "bar", 3))
    printf("*** attr value mismatch ***\n");

  __u64 size;
  time_t mtime;
  r = rados_stat(pool, oid, &size, &mtime);
  printf("rados_stat size = %lld mtime = %d = %d\n", size, (int)mtime, r);

  /* tmap */

  /* exec */
  rados_exec(pool, oid, "crypto", "md5", buf, strlen(buf) + 1, buf, 128);
  printf("exec result=%s\n", buf);
  r = rados_read(pool, oid, 0, buf2, 128);
  printf("read result=%s\n", buf2);
  printf("size=%d\n", r);

  /* aio */
  rados_completion_t a, b;
  rados_aio_write(pool, "a", 0, buf, 100, &a);
  rados_aio_write(pool, "../b/bb_bb_bb\\foo\\bar", 0, buf, 100, &b);
  rados_aio_wait_for_safe(a);
  printf("a safe\n");
  rados_aio_wait_for_safe(b);
  printf("b safe\n");
  rados_aio_release(a);
  rados_aio_release(b);

  rados_read(pool, "../b/bb_bb_bb\\foo\\bar", 0, buf2, 128);


  const char *entry;
  rados_list_ctx_t ctx;
  rados_pool_init_ctx(&ctx);
  while (rados_pool_list_next(pool, &entry, &ctx) >= 0) {
    printf("list entry: %s\n", entry);
  }
  rados_pool_close_ctx(&ctx);
  
  /* delete a pool */
  r = rados_delete_pool(pool);
  printf("rados_delete_pool = %d\n", r);  
  r = rados_close_pool(pool);

  rados_deinitialize();

  return 0;
}

