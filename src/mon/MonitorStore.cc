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

#include "MonitorStore.h"
#include "common/Clock.h"
#include "common/debug.h"
#include "common/entity_name.h"
#include "common/errno.h"
#include "common/run_cmd.h"
#include "common/safe_io.h"
#include "common/config.h"
#include "common/sync_filesystem.h"

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include "include/compat.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, dir)
static ostream& _prefix(std::ostream *_dout, const string& dir) {
  return *_dout << "store(" << dir << ") ";
}

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sstream>
#include <sys/file.h>

int MonitorStore::mount()
{
  char t[1024];

  dout(1) << "mount" << dendl;
  // verify dir exists
  DIR *d = ::opendir(dir.c_str());
  if (!d) {
    dout(1) << "basedir " << dir << " dne" << dendl;
    return -ENOENT;
  }
  ::closedir(d);

  // open lockfile
  snprintf(t, sizeof(t), "%s/lock", dir.c_str());
  lock_fd = ::open(t, O_CREAT|O_RDWR, 0600);
  if (lock_fd < 0)
    return -errno;
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(lock_fd, F_SETLK, &l);
  if (r < 0) {
    dout(0) << "failed to lock " << t << ", is another ceph-mon still running?" << dendl;
    return -errno;
  }

  if ((!g_conf->chdir.empty()) && (dir[0] != '/')) {
    // combine it with the cwd, in case fuse screws things up (i.e. fakefuse)
    string old = dir;
    char cwd[PATH_MAX];
    char *p = getcwd(cwd, sizeof(cwd));
    dir = p;
    dir += "/";
    dir += old;
  }
  return 0;
}

int MonitorStore::umount()
{
  int close_err = TEMP_FAILURE_RETRY(::close(lock_fd));
  assert (0 == close_err);
  return 0;
}

int MonitorStore::mkfs()
{
  int err;

  err = ::mkdir(dir.c_str(), 0700);
  if (err < 0 && errno != EEXIST) {
    err = -errno;
    derr << "MonitorStore::mkfs: unable to create " << dir << ": " << cpp_strerror(err) << dendl;
    return err;
  }

  int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    err = -errno;
    derr << "MonitorStore::mkfs: unable to open " << dir << ": " << cpp_strerror(err) << dendl;
    return err;
  }
  int close_err = TEMP_FAILURE_RETRY(::close(fd));
  assert (0 == close_err);

  dout(0) << "created monfs at " << dir << " for "
	  << g_conf->name.get_id() << dendl;
  return 0;
}

version_t MonitorStore::get_int(const char *a, const char *b)
{
  char fn[1024];
  if (b)
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  else
    snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    int err = errno;
    if (err == ENOENT) {
      // Non-existent files are treated as containing 0.
      return 0;
    }
    derr << "MonitorStore::get_int: failed to open '" << fn << "': "
	 << cpp_strerror(err) << dendl;
    assert(0 == "failed to open");
    return 0;
  }
  
  char buf[20];
  memset(buf, 0, sizeof(buf));
  int r = safe_read(fd, buf, sizeof(buf) - 1);
  if (r < 0) {
    derr << "MonitorStore::get_int: failed to read '" << fn << "': "
	 << cpp_strerror(r) << dendl;
    int close_err = TEMP_FAILURE_RETRY(::close(fd));
    assert(0 == close_err);
    assert(0); // the file exists; so this is a different failure
    return 0;
  }
  int close_err = TEMP_FAILURE_RETRY(::close(fd));
  assert (0 == close_err);
  
  version_t val = atoi(buf);
  
  if (b) {
    dout(15) << "get_int " << a << "/" << b << " = " << val << dendl;
  } else {
    dout(15) << "get_int " << a << " = " << val << dendl;
  }
  return val;
}


void MonitorStore::put_int(version_t val, const char *a, const char *b)
{
  char fn[1024];
  snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  if (b) {
    int r = ::mkdir(fn, 0755);
    if ((r < 0) && (errno != EEXIST)) {
      int err = -errno;
      derr << __func__ << " failed to create dir " << fn << ": "
	   << cpp_strerror(err) << dendl;
      ceph_abort();
    }
    dout(15) << "set_int " << a << "/" << b << " = " << val << dendl;
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "set_int " << a << " = " << val << dendl;
  }
  
  char vs[30];
  snprintf(vs, sizeof(vs), "%lld\n", (unsigned long long)val);

  char tfn[1024];
  snprintf(tfn, sizeof(tfn), "%s.new", fn);

  int fd = TEMP_FAILURE_RETRY(::open(tfn, O_WRONLY|O_CREAT|O_TRUNC, 0600));
  if (fd < 0) {
    int err = errno;
    derr << "MonitorStore::put_int: failed to open '" << tfn << "': "
	 << cpp_strerror(err) << dendl;
    ceph_abort();
  }
  int r = safe_write(fd, vs, strlen(vs));
  if (r) {
    derr << "MonitorStore::put_int: failed to write to '" << tfn << "': "
	 << cpp_strerror(r) << dendl;
    ceph_abort();
  }
  r = ::fsync(fd);
  if (r) {
    derr << "Monitor::put_int: failed to fsync fd for '" << tfn << "': "
	 << cpp_strerror(r) << dendl;
    ceph_abort();
  }
  if (TEMP_FAILURE_RETRY(::close(fd))) {
    derr << "MonitorStore::put_int: failed to close fd for '" << tfn << "': "
	 << cpp_strerror(r) << dendl;
    ceph_abort();
  }
  if (::rename(tfn, fn)) {
    int err = errno;
    derr << "MonitorStore::put_int: failed to rename '" << tfn << "' to "
	 << "'" << fn << "': " << cpp_strerror(err) << dendl;
    ceph_abort();
  }
}

// kludge to associate a global version number with each per-machine paxos state
version_t MonitorStore::get_global_version(const char *a, version_t b)
{
  char fn[1024], fn2[1024];
  snprintf(fn, sizeof(fn), "%s_gv", a);
  snprintf(fn2, sizeof(fn2), "%llu", (long long unsigned)b);
  return get_int(fn, fn2);
}

// ----------------------------------------
// buffers

bool MonitorStore::exists_bl_ss(const char *a, const char *b)
{
  char fn[1024];
  if (b) {
    dout(15) << "exists_bl " << a << "/" << b << dendl;
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "exists_bl " << a << dendl;
    snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  }
  
  struct stat st;
  int r = ::stat(fn, &st);
  //char buf[80];
  //dout(15) << "exists_bl stat " << fn << " r=" << r << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
  if (r) {
    assert (errno == ENOENT);
  }
  return r == 0;
}

void MonitorStore::erase_ss(const char *a, const char *b)
{
  char fn[1024];
  char dr[1024];
  snprintf(dr, sizeof(dr), "%s/%s", dir.c_str(), a);
  if (b) {
    dout(15) << "erase_ss " << a << "/" << b << dendl;
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "erase_ss " << a << dendl;
    strcpy(fn, dr);
  }
  int r = ::unlink(fn);
  assert(0 == r || ENOENT == errno); // callers don't check for existence first

  ::rmdir(dr);  // sloppy attempt to clean up empty dirs
}

int MonitorStore::get_bl_ss(bufferlist& bl, const char *a, const char *b)
{
  char fn[1024];
  if (b) {
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  }
  
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    char buf[80];
    if (b) {
      dout(15) << "get_bl " << a << "/" << b << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    } else {
      dout(15) << "get_bl " << a << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    }
    return -errno;
  }

  // get size
  struct stat st;
  int rc = ::fstat(fd, &st);
  assert(rc == 0);
  __int32_t len = st.st_size;
 
  // read buffer
  bl.clear();
  bufferptr bp(len);
  int off = 0;
  while (off < len) {
    dout(20) << "reading at off " << off << " of " << len << dendl;
    int r = ::read(fd, bp.c_str()+off, len-off);
    if (r < 0) {
      char buf[80];
      dout(0) << "errno on read " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    }
    assert(r>0);
    off += r;
  }
  bl.append(bp);
  int close_err = TEMP_FAILURE_RETRY(::close(fd));
  assert (0 == close_err);

  if (b) {
    dout(15) << "get_bl " << a << "/" << b << " = " << bl.length() << " bytes" << dendl;
  } else {
    dout(15) << "get_bl " << a << " = " << bl.length() << " bytes" << dendl;
  }

  return len;
}

void MonitorStore::write_bl_ss(bufferlist& bl, const char *a, const char *b, bool append)
{
  int err = 0;
  char fn[1024];
  snprintf(fn, sizeof(fn), "%s/%s", dir.c_str(), a);
  if (b) {
    int r = ::mkdir(fn, 0755);
    if ((r < 0) && (errno != EEXIST)) {
      err = -errno;
      derr << __func__ << " failed to create dir " << fn
	   << ": " << cpp_strerror(err) << dendl;
      assert(0 == "failed to create dir");
    }
    dout(15) << "put_bl " << a << "/" << b << " = " << bl.length() << " bytes" << dendl;
    snprintf(fn, sizeof(fn), "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "put_bl " << a << " = " << bl.length() << " bytes" << dendl;
  }
  
  char tfn[1024];
  int fd;
  if (append) {
    fd = ::open(fn, O_WRONLY|O_CREAT|O_APPEND, 0600);
    if (fd < 0) {
      err = -errno;
      derr << "failed to open " << fn << "for append: "
	   << cpp_strerror(err) << dendl;
      assert(0 == "failed to open for append");
    }
  } else {
    snprintf(tfn, sizeof(tfn), "%s.new", fn);
    fd = ::open(tfn, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    if (fd < 0) {
      err = -errno;
      derr << "failed to open " << tfn << ": " << cpp_strerror(err) << dendl;
      assert(0 == "failed to open");
    }
  }
  
  err = bl.write_fd(fd);
  assert(!err);
  err = ::fsync(fd);
  assert(!err);
  err = TEMP_FAILURE_RETRY(::close(fd));
  assert (!err); // this really can't fail, right? right?...
  if (!append) {
    err = ::rename(tfn, fn);
    if (err < 0) {
      err = -errno;
      derr << __func__ << " failed to rename '" << tfn << "' -> '"
	   << fn << "': " << cpp_strerror(err) << dendl;
      assert(0 == "failed to rename");
    }
  }
}

void MonitorStore::put_bl_sn_map(const char *a,
				map<version_t,bufferlist>::iterator start,
				map<version_t,bufferlist>::iterator end)
{
  int err = 0;
  int close_err = 0;
  version_t first = start->first;
  map<version_t,bufferlist>::iterator lastp = end;
  --lastp;
  version_t last = lastp->first;
  dout(15) <<  "put_bl_sn_map " << a << "/[" << first << ".." << last << "]" << dendl;

  // only do a big sync if there are several values, or if the feature is disabled.
  if (g_conf->mon_sync_fs_threshold <= 0 ||
      last - first < (unsigned)g_conf->mon_sync_fs_threshold) {
    // just do them individually
    for (map<version_t,bufferlist>::iterator p = start; p != end; ++p) {
      put_bl_sn(p->second, a, p->first);
    }
    return;
  }

  // make sure dir exists
  char dfn[1024];
  snprintf(dfn, sizeof(dfn), "%s/%s", dir.c_str(), a);
  int r = ::mkdir(dfn, 0755);
  if ((r < 0) && (errno != EEXIST)) {
    err = -errno;
    derr << __func__ << " failed to create dir " << dfn << ": "
	 << cpp_strerror(err) << dendl;
    assert(0 == "failed to create dir");
  }

  for (map<version_t,bufferlist>::iterator p = start; p != end; ++p) {
    char tfn[1024], fn[1024];

    snprintf(fn, sizeof(fn), "%s/%llu", dfn, (long long unsigned)p->first);
    snprintf(tfn, sizeof(tfn), "%s.new", fn);

    int fd = ::open(tfn, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    if (fd < 0) {
      int err = -errno;
      derr << "failed to open " << tfn << ": " << cpp_strerror(err) << dendl;
      assert(0 == "failed to open");
    }

    err = p->second.write_fd(fd);
    close_err = TEMP_FAILURE_RETRY(::close(fd));
    assert (0 == close_err);
    if (err < 0)
      assert(0 == "failed to write");
  }

  // sync them all
  int dirfd = ::open(dir.c_str(), O_RDONLY);
  if (dirfd < 0) {
    err = -errno;
    derr << "failed to open " << dir << ": " << cpp_strerror(err) << dendl;
    assert(0 == "failed to open temp file");
  }
  sync_filesystem(dirfd);
  close_err = TEMP_FAILURE_RETRY(::close(dirfd));
  assert (0 == close_err);
    
  // rename them all into place
  for (map<version_t,bufferlist>::iterator p = start; p != end; ++p) {
    char tfn[1024], fn[1024];
    
    snprintf(fn, sizeof(fn), "%s/%llu", dfn, (long long unsigned)p->first);
    snprintf(tfn, sizeof(tfn), "%s.new", fn);
    
    err = ::rename(tfn, fn);
    if (err < 0)
      assert(0 == "failed to rename");
  }
    
  // fsync the dir (to commit the renames)
  dirfd = ::open(dir.c_str(), O_RDONLY);
  if (dirfd < 0) {
    err = -errno;
    derr << __func__ << " failed to open " << dir
	 << ": " << cpp_strerror(err) << dendl;
    assert(0 == "failed to open dir");
  }
  err = ::fsync(dirfd);
  if (err < 0) {
    err = -errno;
    derr << __func__ << " failed to fsync " << dir
	 << ": " << cpp_strerror(err) << dendl;
    assert(0 == "failed to fsync");
  }
  close_err = TEMP_FAILURE_RETRY(::close(dirfd));
  assert (0 == close_err);
}

void MonitorStore::sync()
{
  int dirfd = ::open(dir.c_str(), O_RDONLY);
  if (dirfd < 0) {
    int err = -errno;
    derr << __func__ << " failed to open " << dir
	 << ": " << cpp_strerror(err) << dendl;
    assert(0 == "failed to open dir for syncing");
  }
  sync_filesystem(dirfd);
  int close_err = TEMP_FAILURE_RETRY(::close(dirfd));
  assert (0 == close_err);
}
