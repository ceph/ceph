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
#include "include/ceph/libceph.h"
#include <stdlib.h>

#include <errno.h>
#include <dirent.h>
#include <iostream>
#include <fcntl.h>
#include <sys/xattr.h>
#include <string.h>

using std::cout;
using std::cerr;

int main(int argc, const char **argv)
{
  struct ceph_mount_info *cmount;
    cout << "calling ceph_create..." << std::endl;
  int ret = ceph_create(&cmount, NULL);
  if (ret) {
    cerr << "ceph_create failed with error: " << ret << std::endl;
    return 1;
  }
    cout << "calling ceph_conf_read_file..." << ret << std::endl;
  ceph_conf_read_file(cmount, NULL);
    cout << "calling ceph_conf_parse_argv..." << ret << std::endl;
  ceph_conf_parse_argv(cmount, argc, argv);

  char buf[128];
    cout << "calling ceph_conf_get..." << ret << std::endl;
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

  // what if foo is already there???
  struct ceph_dir_result *foo_dir;
  ret = ceph_opendir(cmount, "foo", &foo_dir);
  if (ret != -ENOENT) {
    cerr << "ceph_opendir error: unexpected result from trying to open foo: "
	 << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_opendir: success" << std::endl;
  }

  ret = ceph_mkdir(cmount, "foo",  0777);
  if (ret) {
    cerr << "ceph_mkdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_mkdir: success" << std::endl;
  }

  struct stat stbuf;
  ret = ceph_lstat(cmount, "foo", &stbuf);
  if (ret) {
    cerr << "ceph_lstat error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_lstat: success" << std::endl;
  }

  if (!S_ISDIR(stbuf.st_mode)) {
    cerr << "ceph_lstat(foo): foo is not a directory? st_mode = "
	 << stbuf.st_mode << std::endl;
    return 1;
  } else {
    cout << "ceph_lstat: mode success" << std::endl;
  }

  ret = ceph_rmdir(cmount, "foo");
  if (ret) {
    cerr << "ceph_rmdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_rmdir: success" << std::endl;
  }

  ret = ceph_mkdirs(cmount, "foo/bar/baz",  0777);
  if (ret) {
    cerr << "ceph_mkdirs error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_mkdirs: success" << std::endl;
  }
  ret = ceph_rmdir(cmount, "foo/bar/baz");
  if (ret) {
    cerr << "ceph_rmdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_rmdir: success" << std::endl;
  }
  ret = ceph_rmdir(cmount, "foo/bar");
  if (ret) {
    cerr << "ceph_rmdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_rmdir: success" << std::endl;
  }
  ret = ceph_rmdir(cmount, "foo");
  if (ret) {
    cerr << "ceph_rmdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_rmdir: success" << std::endl;
  }
  ret = ceph_open(cmount, "barfile", O_CREAT, 0666);
  if (ret < 0) {
    cerr << "ceph_open O_CREAT error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_open: success" << std::endl;
  }
  ret = ceph_setxattr(cmount, "barfile", "user.testxattr", (void *) "AYBABTU", 7, XATTR_CREATE);
  if (ret < 0) {
    cerr << "ceph_setxattr error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_setxattr: success" << std::endl;
  }
  char *outlist;
  outlist = (char *) malloc(256);
  ret = ceph_listxattr(cmount, "barfile", outlist, 0);
  free(outlist);
  if (ret < 1) {
    cerr << "ceph_listxattr error (a): should have returned > 0" << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_listxattr: success" << std::endl;
  }
  
  outlist = (char *) malloc(ret);
  ret = ceph_listxattr(cmount, "barfile", outlist, ret);
  if (ret < 1) {
    cerr << "ceph_listxattr error (b): should have returned > 0" << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_listxattr: success" << std::endl;
  }
  void *aybabtu;
  aybabtu = malloc(8);
  ret = ceph_getxattr(cmount, "barfile", "user.testxattr", aybabtu, 7);
  if (ret < 1) {
    cerr << "ceph_getxattr error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_getxattr: success" << std::endl;
  }
  char aybabtu_reference[]="AYBABTU";
  if (strncmp((char *) aybabtu, aybabtu_reference,7)) {
    cerr << "ceph_getxattr error: no match (" << aybabtu << ") should be (" << aybabtu_reference << cpp_strerror(ret) << std::endl;
  }
  cout << "Attempting lstat on '/.'" << std::endl;
  ret = ceph_lstat(cmount, "/.", &stbuf);
  if (ret) {
    cerr << "ceph_lstat error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_lstat: success" << std::endl;
  }
  cout << "Attempting lstat on '.'" << std::endl;
  ret = ceph_lstat(cmount, ".", &stbuf);
  if (ret) {
    cerr << "ceph_lstat error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_lstat: success" << std::endl;
  }
  cout << "Attempting readdir_r" << std::endl;
  ret = ceph_mkdir(cmount, "readdir_r_test",  0777);
  if (ret) {
    cerr << "ceph_mkdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_mkdir: success" << std::endl;
  }
  struct ceph_dir_result *readdir_r_test_dir;
  ret = ceph_opendir(cmount, "readdir_r_test", &readdir_r_test_dir);
  if (ret != 0) {
    cerr << "ceph_opendir error: unexpected result from trying to open readdir_r_test: "
	 << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_opendir: success" << std::endl;
  }
  ret = ceph_open(cmount, "readdir_r_test/opened_file", O_CREAT, 0666);
  if (ret < 0) {
    cerr << "ceph_open O_CREAT error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_open: success" << std::endl;
  }

  struct dirent * result;
  result = (struct dirent *) malloc(sizeof(struct dirent));
  ret = ceph_readdir_r(cmount, readdir_r_test_dir, result);
  if (ret != 0) {
    cerr << "ceph_readdir_r: fail, returned: " << ret << std::endl;
  } else {
    cerr << "ceph_readdir_r: success: " << *result->d_name << std::endl;
    return 1;
  }
  
  ceph_shutdown(cmount);

  return 0;
}
