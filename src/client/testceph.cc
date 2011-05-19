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
  int my_fd;
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
  //ret = ceph_closedir(cmount, foo_dir);
  //if (ret == 0) {
  //  cerr << "ceph_closedir success" << std::endl;
  //} else {
  //  cerr << "ceph_closedir error: " << cpp_strerror(ret) << std::endl;
  //}
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
  my_fd = ret = ceph_open(cmount, "barfile", O_CREAT, 0666);
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
  ret = ceph_close(cmount,my_fd);
  if (ret < 0) {
    cerr << "ceph_close error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_close: success" << std::endl;
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
  cout << "Setting up readdir test" << std::endl;
  ret = ceph_mkdir(cmount, "readdir_test",  0777);
  if (ret) {
    cerr << "ceph_mkdir error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_mkdir: success" << std::endl;
  }
  my_fd = ret = ceph_open(cmount, "readdir_test/opened_file_1", O_CREAT, 0666);
  if (ret < 0) {
    cerr << "ceph_open O_CREAT error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_open: success" << std::endl;
  }
  ret = ceph_close(cmount, my_fd);
  if (ret < 0) {
    cerr << "ceph_close error: " << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_close: success" << std::endl;
  }

  struct ceph_dir_result *readdir_test_dir;
  ret = ceph_opendir(cmount, "readdir_test", &readdir_test_dir);
  if (ret != 0) {
    cerr << "ceph_opendir error: unexpected result from trying to open readdir_test: "
	 << cpp_strerror(ret) << std::endl;
    return 1;
  } else {
    cout << "ceph_opendir: success" << std::endl;
  }
  cout << "Attempting readdir on opened directory..." << std::endl;
  struct dirent * result;
  //result = (struct dirent *) malloc(sizeof(struct dirent));
  result = ceph_readdir(cmount, readdir_test_dir);
  if (result == (dirent *) NULL) {
    cout << "ceph_readdir: failed to read any entries" << std::endl;
  }
  loff_t telldir_result;
  while ( result != (dirent *) NULL) {
    cout << "ceph_readdir: dirent->d_name: (" << result->d_name << ")" << std::endl;
    cout << "ceph_telldir: starting" << std::endl;
    telldir_result = ceph_telldir(cmount, readdir_test_dir);
    if (telldir_result > -1) {
      cout << "ceph_telldir: offset: from return code:" << telldir_result << std::endl;
    } else {
      cout << "ceph_telldir: failed" << std::endl;
    }
    cout << "ceph_readdir: lookup success: trying for another..." << std::endl;
    result = ceph_readdir(cmount, readdir_test_dir);
  }
  cout << "ceph_readdir: finished" << std::endl;

  // tell us that we're at the end of the directory:
  cout << "ceph_telldir: starting" << std::endl;
  telldir_result = ceph_telldir(cmount, readdir_test_dir);
  if (telldir_result > -1) {
    cout << "ceph_telldir: offset: from return code:" << telldir_result << std::endl;
  } else {
    cout << "ceph_telldir: failed" << std::endl;
  }

  //ret = ceph_closedir(cmount,readdir_test_dir);
  //if (ret == 0) {
  //  cerr << "ceph_closedir success" << std::endl;
  //} else {
  //  cerr << "ceph_closedir error: " << cpp_strerror(ret) << std::endl;
  //}
  
  ceph_shutdown(cmount);

  return 0;
}
