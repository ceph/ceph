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

#include "common/errno.h"
#include "libceph.h"

#include <errno.h>
#include <iostream>

using std::cout;
using std::cerr;

int main(int argc, const char **argv)
{
  ceph_mount_t *cmount;
  int ret = ceph_create(&cmount, NULL);
  if (ret) {
    cerr << "ceph_create failed with error: " << ret << std::endl;
    return 1;
  }

  ceph_conf_read_file(cmount, NULL);
  ceph_conf_parse_argv(cmount, argc, argv);

  char buf[128];
  ret = ceph_conf_get(cmount, "log file", buf, sizeof(buf));
  if (ret) {
    cerr << "ceph_conf_get(\"log file\") failed with error " << ret << std::endl;
  }
  else {
    cout << "log_file = \"" << buf << "\"" << std::endl;
  }

  ret = ceph_mount(cmount, NULL);
  if (ret) {
    cerr << "ceph_mount error: " << ret << std::endl;
    return 1;
  }
  cout << "Successfully mounted Ceph!" << std::endl;

  ceph_dir_result_t *foo_dir;
  ret = ceph_opendir(cmount, "foo", &foo_dir);
  if (ret != -ENOENT) {
    cerr << "ceph_opendir error: unexpected result from trying to open foo: "
	 << cpp_strerror(ret) << std::endl;
    return 1;
  }

  ret = ceph_mkdir(cmount, "foo",  0777);
  if (ret) {
    cerr << "ceph_mkdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  }

  struct stat stbuf;
  ret = ceph_lstat(cmount, "foo", &stbuf);
  if (ret) {
    cerr << "ceph_lstat error: " << cpp_strerror(ret) << std::endl;
    return 1;
  }

  if (!S_ISDIR(stbuf.st_mode)) {
    cerr << "ceph_lstat(foo): foo is not a directory? st_mode = "
	 << stbuf.st_mode << std::endl;
    return 1;
  }

  ret = ceph_rmdir(cmount, "foo");
  if (ret) {
    cerr << "ceph_rmdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  }

  ret = ceph_mkdirs(cmount, "foo/bar/baz",  0777);
  if (ret) {
    cerr << "ceph_mkdirs error: " << cpp_strerror(ret) << std::endl;
    return 1;
  }
  ret = ceph_rmdir(cmount, "foo/bar/baz");
  if (ret) {
    cerr << "ceph_rmdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  }
  ret = ceph_rmdir(cmount, "foo/bar");
  if (ret) {
    cerr << "ceph_rmdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  }
  ret = ceph_rmdir(cmount, "foo");
  if (ret) {
    cerr << "ceph_rmdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  }

  ceph_shutdown(cmount);

  return 0;
}
