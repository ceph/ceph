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



#include <iostream>
using namespace std;


#define _FCNTL_H
#include <bits/fcntl.h>


// ceph stuff
#include "common/config.h"
#include "client/Client.h"
#include "msg/SimpleMessenger.h"

// syscall fun
#include <sys/syscall.h>
#include <unistd.h>
#include <sys/types.h>
//#include <sys/stat.h>
#define CEPH_FD_OFF  50000


/****** startup etc *******/

class LdCeph {
public:
  // globals
  bool    started;
  char   *mount_point;
  char   *mount_point_parent;
  int     mount_point_len;

  Client *client;

  filepath fp_mount_point;
  filepath cwd;
  bool     cwd_above_mp, cwd_in_mp;

  const char *get_ceph_path(const char *orig, char *buf) {
    if (!started) return 0;

    // relative path?    BUG: this won't catch "blah/../../asdf"
    if (orig[0] && 
        orig[0] != '/' && 
        !(orig[0] == '.' && orig[1] == '.')) {
      
      if (cwd_in_mp) return orig;   // inside mount point, definitely ceph
      if (!cwd_above_mp) return 0;  // not above mount point, definitely not ceph
    
      // relative, above mp.
      filepath o = orig;
      filepath p = cwd;
      for (unsigned b = 0; b < o.depth(); b++) {
        if (o[b] == "..")
          p.pop_dentry();
        else
          p.push_dentry(o[b]);
      }

      // FIXME rewrite
      if (strncmp(p.c_str(), mount_point, mount_point_len) == 0) {
        if (p.c_str()[mount_point_len] == 0) 
          return "/";
        if (p.c_str()[mount_point_len] == '/') {
          strcpy(buf, p.c_str() + mount_point_len);
          return buf;
        }
      }
      return 0;
    } else {
      // absolute
      if (strncmp(orig, mount_point, mount_point_len) == 0) {
        if (orig[mount_point_len] == 0) 
          return "/";
        if (orig[mount_point_len] == '/')
          return orig + mount_point_len;
      }
      return 0;
    }
  }

  void refresh_cwd() {
    char buf[255];
    syscall(SYS_getcwd, buf, 255);
    cwd = buf;
    
    if (strncmp(buf, mount_point, mount_point_len) == 0 &&
        (buf[mount_point_len] == 0 ||
         buf[mount_point_len] == '/'))
      cwd_in_mp = true;
    else {
      if (cwd.depth() > fp_mount_point.depth())
        cwd_above_mp = false;
      else {
        cwd_above_mp = true;
        for (unsigned i=0; i<cwd.depth(); i++) {
          if (cwd[i] != fp_mount_point[i]) {
            cwd_above_mp = false;
            break;
          }
        }
      }
    }
    //cout << "refresh_cwd '" << cwd << "', above=" << cwd_above_mp << ", in=" << cwd_in_mp << endl;
  }
  
  
  LdCeph() : 
    started(false),
    mount_point(0), mount_point_parent(0),
    mount_point_len(0),
    cwd_above_mp(false), cwd_in_mp(false) {
    cerr << "ldceph init " << std::endl;

    // args
    vector<const char *> args;
    env_to_vec(args);
    parse_config_options(args);
  
    // load monmap
    MonMap monmap;
    int r = monmap.read(".ceph_monmap");
    assert(r >= 0);
    
    // start up network
    rank.start_rank();
    
    client = new Client(rank.register_entity(entity_name_t(entity_name_t::TYPE_CLIENT,-1)), &monmap);
    client->init();
    r = client->mount();
    if (r < 0) {
      // failure
      cerr << "ldceph init: mount failed " << r << std::endl;
      delete client;
      client = 0;
    } else {
      // success
      started = true;
      mount_point = "/ceph";
      mount_point_parent = "/";
      mount_point_len = 5;

      fp_mount_point = mount_point;

      cerr << "ldceph init: mounted on " << mount_point << " as client" << client->get_nodeid() << std::endl;

      refresh_cwd();
    }
  }
  ~LdCeph() {
    cout << "ldceph fini" << std::endl;
    if (false && client) {  
      client->unmount();
      client->shutdown();
      delete client;
      client = 0;
      rank.wait();
    }
  }    

} ldceph;



/****** original functions ****/



/****** captured functions ****/


#define MYFD(f)      ((fd) > CEPH_FD_OFF && ldceph.started)
#define TO_FD(fd)    (fd > 0 ? fd+CEPH_FD_OFF:fd)
#define FROM_FD(fd)  (fd - CEPH_FD_OFF)

extern "C" {
  
  // open/close
  //int open(const char *pathname, int flags) {
  int open(const char *pathname, int flags, mode_t mode) {
    char buf[255];
    if (const char *c = ldceph.get_ceph_path(pathname, buf))
      return TO_FD(ldceph.client->open(c, flags));
    else
      return syscall(SYS_open, pathname, flags, mode);
  }

  int creat(const char *pathname, mode_t mode) {
    return open(pathname, O_CREAT|O_WRONLY|O_TRUNC, mode);
  }
  int close(int fd) {
    if (MYFD(fd)) 
      return ldceph.client->close(FROM_FD(fd));
    else
      return syscall(SYS_close, fd);
  }
  
  
  // read/write
  ssize_t write(int fd, const void *buf, size_t count) {
    if (MYFD(fd)) 
      return ldceph.client->write(FROM_FD(fd), (char*)buf, count);
    else
      return syscall(SYS_write, fd, buf, count);
  }

  ssize_t read(int fd, void *buf, size_t count) {
    if (MYFD(fd)) 
      return ldceph.client->read(FROM_FD(fd), (char*)buf, count);
    else
      return syscall(SYS_read, fd, buf, count);
  }

  //int fsync(int fd);
  //int fdatasync(int fd);


  // namespace
  int rmdir(const char *pathname) {
    char buf[255];
    if (const char *c = ldceph.get_ceph_path(pathname, buf))
      return ldceph.client->rmdir(c);
    else
      return syscall(SYS_rmdir, pathname);
  }
  int mkdir(const char *pathname, mode_t mode) {
    char buf[255];
    if (const char *c = ldceph.get_ceph_path(pathname, buf)) 
      return ldceph.client->mkdir(c, mode);
    else
      return syscall(SYS_mkdir, pathname, mode);
  }
  int unlink(const char *pathname) {
    char buf[255];
    if (const char *c = ldceph.get_ceph_path(pathname, buf))
      return ldceph.client->unlink(c);
    else
      return syscall(SYS_unlink, pathname);
  }

  int stat(const char *pathname, struct stat *st) {
    //int __xstat64(int __ver, const char *pathname, struct stat64 *st64) {  // stoopid GLIBC
    //struct stat *st = (struct stat*)st64;
    char buf[255];
    if (const char *c = ldceph.get_ceph_path(pathname, buf))
      return ldceph.client->lstat(c, st);   // FIXME
    else
      return syscall(SYS_stat, pathname, st);
  }
  //int fstat(int filedes, struct stat *buf);
  //int lstat(const char *file_name, struct stat *buf);

  int chdir(const char *pathname) {
    char buf[255];
    if (const char *c = ldceph.get_ceph_path(pathname, buf)) {
      int r = ldceph.client->chdir(c);
      if (r == 0) {
        if (!ldceph.cwd_in_mp)
          syscall(SYS_chdir, ldceph.mount_point_parent);
        ldceph.cwd_in_mp = true;
        ldceph.cwd_above_mp = false;
        ldceph.cwd = ldceph.mount_point;
        filepath fpc = c;
        ldceph.cwd.append(fpc);
      }
      return r;
    } else {
      int r = syscall(SYS_chdir, pathname);
      if (r) {
        ldceph.refresh_cwd();
      }
      return r;
    }
  }
  char *getcwd(char *buf, size_t size) {
    strncpy(buf, ldceph.cwd.c_str(), size);
    return buf;
  }
  //int fchdir(int fd);

  


}
