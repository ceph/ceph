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

#include "config.h"

#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_dout << dbeginl << g_clock.now() << " store(" << dir <<") "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_derr << dbeginl << g_clock.now() << " store(" << dir <<") "

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

void MonitorStore::mount()
{
  dout(1) << "mount" << dendl;
  // verify dir exists
  DIR *d = ::opendir(dir.c_str());
  if (!d) {
    derr(1) << "basedir " << dir << " dne" << dendl;
    assert(0);
  }
  ::closedir(d);

  if (g_conf.use_abspaths) {
    // combine it with the cwd, in case fuse screws things up (i.e. fakefuse)
    string old = dir;
    char cwd[200];
    getcwd(cwd, 200);
    dir = cwd;
    dir += "/";
    dir += old;
  }
}


void MonitorStore::mkfs()
{
  dout(1) << "mkfs" << dendl;

  char cmd[200];
  sprintf(cmd, "test -d %s && /bin/rm -r %s ; mkdir -p %s", dir.c_str(), dir.c_str(), dir.c_str());
  dout(1) << cmd << dendl;
  system(cmd);
}


version_t MonitorStore::get_int(const char *a, const char *b)
{
  char fn[200];
  if (b)
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  else
    sprintf(fn, "%s/%s", dir.c_str(), a);
  
  FILE *f = ::fopen(fn, "r");
  if (!f) 
    return 0;
  
  char buf[20];
  ::fgets(buf, 20, f);
  ::fclose(f);
  
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
  char fn[200];
  sprintf(fn, "%s/%s", dir.c_str(), a);
  if (b) {
    ::mkdir(fn, 0755);
    dout(15) << "set_int " << a << "/" << b << " = " << val << dendl;
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "set_int " << a << " = " << val << dendl;
  }
  
  char vs[30];
#ifdef __LP64__
  sprintf(vs, "%ld\n", val);
#else
  sprintf(vs, "%lld\n", val);
#endif

  char tfn[200];
  sprintf(tfn, "%s.new", fn);

  int fd = ::open(tfn, O_WRONLY|O_CREAT, 0644);
  assert(fd >= 0);
  ::write(fd, vs, strlen(vs));
  ::close(fd);
  ::rename(tfn, fn);
}


// ----------------------------------------
// buffers

bool MonitorStore::exists_bl_ss(const char *a, const char *b)
{
  char fn[200];
  if (b) {
    dout(15) << "exists_bl " << a << "/" << b << dendl;
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "exists_bl " << a << dendl;
    sprintf(fn, "%s/%s", dir.c_str(), a);
  }
  
  struct stat st;
  int r = ::stat(fn, &st);
  //dout(15) << "exists_bl stat " << fn << " r=" << r << " errno " << errno << " " << strerror(errno) << dendl;
  return r == 0;
}


int MonitorStore::get_bl_ss(bufferlist& bl, const char *a, const char *b)
{
  char fn[200];
  if (b) {
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    sprintf(fn, "%s/%s", dir.c_str(), a);
  }
  
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    if (b) {
      dout(15) << "get_bl " << a << "/" << b << " DNE" << dendl;
    } else {
      dout(15) << "get_bl " << a << " DNE" << dendl;
    }
    return 0;
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
    if (r < 0) derr(0) << "errno on read " << strerror(errno) << dendl;
    assert(r>0);
    off += r;
  }
  bl.append(bp);
  ::close(fd);

  if (b) {
    dout(15) << "get_bl " << a << "/" << b << " = " << bl.length() << " bytes" << dendl;
  } else {
    dout(15) << "get_bl " << a << " = " << bl.length() << " bytes" << dendl;
  }

  return len;
}

int MonitorStore::put_bl_ss(bufferlist& bl, const char *a, const char *b)
{
  char fn[200];
  sprintf(fn, "%s/%s", dir.c_str(), a);
  if (b) {
    ::mkdir(fn, 0755);
    dout(15) << "put_bl " << a << "/" << b << " = " << bl.length() << " bytes" << dendl;
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "put_bl " << a << " = " << bl.length() << " bytes" << dendl;
  }
  
  char tfn[200];
  sprintf(tfn, "%s.new", fn);
  int fd = ::open(tfn, O_WRONLY|O_CREAT, 0644);
  assert(fd >= 0);
  
  // write data
  for (list<bufferptr>::const_iterator it = bl.buffers().begin();
       it != bl.buffers().end();
       it++)  {
    int r = ::write(fd, it->c_str(), it->length());
    if (r != (int)it->length())
      derr(0) << "put_bl_ss ::write() returned " << r << " not " << it->length() << dendl;
    if (r < 0) 
      derr(0) << "put_bl_ss ::write() errored out, errno is " << strerror(errno) << dendl;
  }

  ::fsync(fd);
  ::close(fd);
  ::rename(tfn, fn);

  return 0;
}
