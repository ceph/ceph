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

#include "include/rados/librados.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, const char **argv) 
{
  char tmp[32];
  int i, r;
  rados_t cl;

  if (rados_create(&cl, NULL) < 0) {
    printf("error initializing\n");
    exit(1);
  }

  if (rados_conf_read_file(cl, "/etc/ceph/ceph.conf")) {
    printf("error reading configuration file\n");
    exit(1);
  }

  // Try to set a configuration option that doesn't exist.
  // This should fail.
  if (!rados_conf_set(cl, "config option that doesn't exist",
                     "some random value")) {
    printf("error: succeeded in setting nonexistent config option\n");
    exit(1);
  }

  if (rados_conf_get(cl, "log to stderr", tmp, sizeof(tmp))) {
    printf("error: failed to read log_to_stderr from config\n");
    exit(1);
  }

  // Can we change it? 
  if (rados_conf_set(cl, "log to stderr", "2")) {
    printf("error: error setting log_to_stderr\n");
    exit(1);
  }
  rados_reopen_log(cl);
  if (rados_conf_get(cl, "log to stderr", tmp, sizeof(tmp))) {
    printf("error: failed to read log_to_stderr from config\n");
    exit(1);
  }
  if (tmp[0] != '2') {
    printf("error: new setting for log_to_stderr failed to take effect.\n");
    exit(1);
  }

  if (rados_connect(cl)) {
    printf("error connecting\n");
    exit(1);
  }

  /* create an io_ctx */
  r = rados_pool_create(cl, "foo");
  printf("rados_ioctx_create = %d\n", r);

  rados_ioctx_t io_ctx;
  r = rados_ioctx_open(cl, "foo", &io_ctx);
  printf("rados_ioctx_open = %d, io_ctx = %p\n", r, io_ctx);

  /* list all pools */
  {
    int buf_sz = rados_pool_list(cl, NULL, 0);
    printf("need buffer size of %d\n", buf_sz);
    char buf[buf_sz];
    int r = rados_pool_list(cl, buf, buf_sz);
    if (r != buf_sz) {
      printf("buffer size mismatch: got %d the first time, but %d "
	     "the second.\n", buf_sz, r);
      exit(1);
    }
    const char *b = buf;
    printf("begin pools.\n");
    while (1) {
      if (b[0] == '\0')
        break;
      printf(" pool: '%s'\n", b);
      b += strlen(b) + 1;
    };
    printf("end pools.\n");
  }


  /* stat */
  struct rados_ioctx_stat_t st;
  r = rados_ioctx_stat(io_ctx, &st);
  printf("rados_ioctx_stat = %d, %lld KB, %lld objects\n", r, (long long)st.num_kb, (long long)st.num_objects);

  /* snapshots */
  r = rados_ioctx_snap_create(io_ctx, "snap1");
  printf("rados_ioctx_snap_create snap1 = %d\n", r);
  rados_snap_t snaps[10];
  r = rados_ioctx_snap_list(io_ctx, snaps, 10);
  for (i=0; i<r; i++) {
    char name[100];
    rados_ioctx_snap_get_name(io_ctx, snaps[i], name, sizeof(name));
    printf("rados_ioctx_snap_list got snap %lld %s\n", (long long)snaps[i], name);
  }
  rados_snap_t snapid;
  r = rados_ioctx_snap_lookup(io_ctx, "snap1", &snapid);
  printf("rados_ioctx_snap_lookup snap1 got %lld, result %d\n", (long long)snapid, r);
  r = rados_ioctx_snap_remove(io_ctx, "snap1");
  printf("rados_ioctx_snap_remove snap1 = %d\n", r);

  /* sync io */
  time_t tm;
  char buf[128], buf2[128];
  time(&tm);
  snprintf(buf, 128, "%s", ctime(&tm));
  const char *oid = "foo_object";
  r = rados_write(io_ctx, oid, buf, strlen(buf) + 1, 0);
  printf("rados_write = %d\n", r);
  r = rados_read(io_ctx, oid, buf2, sizeof(buf2), 0);
  printf("rados_read = %d\n", r);
  if (memcmp(buf, buf2, r))
    printf("*** content mismatch ***\n");

  /* attrs */
  r = rados_setxattr(io_ctx, oid, "attr1", "bar", 3);
  printf("rados_setxattr attr1=bar = %d\n", r);
  char val[10];
  r = rados_getxattr(io_ctx, oid, "attr1", val, sizeof(val));
  printf("rados_getxattr attr1 = %d\n", r);
  if (memcmp(val, "bar", 3))
    printf("*** attr value mismatch ***\n");

  uint64_t size;
  time_t mtime;
  r = rados_stat(io_ctx, oid, &size, &mtime);
  printf("rados_stat size = %lld mtime = %d = %d\n", (long long)size, (int)mtime, r);
  r = rados_stat(io_ctx, "does_not_exist", NULL, NULL);
  printf("rados_stat(does_not_exist) = %d\n", r);

  /* exec */
  rados_exec(io_ctx, oid, "crypto", "md5", buf, strlen(buf) + 1, buf, 128);
  printf("exec result=%s\n", buf);
  r = rados_read(io_ctx, oid, buf2, 128, 0);
  printf("read result=%s\n", buf2);
  printf("size=%d\n", r);

  /* aio */
  rados_completion_t a, b;
  rados_aio_create_completion(0, 0, 0, &a);
  rados_aio_create_completion(0, 0, 0, &b);
  rados_aio_write(io_ctx, "a", a, buf, 100, 0);
  rados_aio_write(io_ctx, "../b/bb_bb_bb\\foo\\bar", b, buf, 100, 0);
  rados_aio_wait_for_safe(a);
  printf("a safe\n");
  rados_aio_wait_for_safe(b);
  printf("b safe\n");
  rados_aio_release(a);
  rados_aio_release(b);

  rados_read(io_ctx, "../b/bb_bb_bb\\foo\\bar", buf2, 128, 0);

  /* list objects */
  rados_list_ctx_t h;
  r = rados_objects_list_open(io_ctx, &h);
  printf("rados_list_objects_open = %d, h = %p\n", r, h);
  const char *poolname;
  while (rados_objects_list_next(h, &poolname) == 0)
    printf("rados_list_objects_next got object '%s'\n", poolname);
  rados_objects_list_close(h);

  /* stat */
  r = rados_ioctx_stat(io_ctx, &st);
  printf("rados_stat_pool = %d, %lld KB, %lld objects\n", r, (long long)st.num_kb, (long long)st.num_objects);

  /* delete a pool */
  printf("rados_delete_pool = %d\n", r);  
  rados_ioctx_close(io_ctx);

  r = rados_pool_delete(cl, "foo");
  printf("rados_ioctx_pool_delete = %d\n", r);

  rados_shutdown(cl);

  return 0;
}

