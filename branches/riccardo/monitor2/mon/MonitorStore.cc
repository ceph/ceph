// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " store(" << dir <<") "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " store(" << dir <<") "

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


void MonitorStore::init()
{
  dout(1) << "init" << endl;
  // verify dir exists
  DIR *d = ::opendir(dir.c_str());
  if (!d) {
    derr(1) << "basedir " << dir << " dne" << endl;
    assert(0);
  }
  ::closedir(d);
}


void MonitorStore::mkfs()
{
  dout(1) << "mkfs" << endl;

  char cmd[200];
  sprintf(cmd, "/bin/rm -r %s ; mkdir %s", dir.c_str(), dir.c_str());
  dout(1) << cmd << endl;
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
    dout(15) << "get_int " << a << "/" << b << " = " << val << endl;
  } else {
    dout(15) << "get_int " << a << " = " << val << endl;
  }
  return val;
}


void MonitorStore::put_int(version_t val, const char *a, const char *b)
{
  char fn[200];
  sprintf(fn, "%s/%s", dir.c_str(), a);
  if (b) {
    ::mkdir(fn, 0755);
    dout(15) << "set_int " << a << "/" << b << " = " << val << endl;
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "set_int " << a << " = " << val << endl;
  }
  
  char vs[30];
  sprintf(vs, "%lld\n", val);

  char tfn[200];
  sprintf(tfn, "%s.new", fn);

  int fd = ::open(tfn, O_WRONLY|O_CREAT);
  assert(fd > 0);
  ::fchmod(fd, 0644);
  ::write(fd, vs, strlen(vs));
  ::close(fd);
  ::rename(tfn, fn);
}


// ----------------------------------------
// buffers

bool MonitorStore::exists_bl(const char *a, const char *b)
{
  char fn[200];
  if (b) {
    dout(15) << "exists_bl " << a << "/" << b << endl;
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "exists_bl " << a << endl;
    sprintf(fn, "%s/%s", dir.c_str(), a);
  }
  
  struct stat st;
  int r = ::stat(fn, &st);
  return r == 0;
}


int MonitorStore::get_bl(bufferlist& bl, const char *a, const char *b)
{
  char fn[200];
  if (b) {
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    sprintf(fn, "%s/%s", dir.c_str(), a);
  }
  
  int fd = ::open(fn, O_RDONLY);
  if (!fd) {
    if (b) {
      dout(15) << "get_bl " << a << "/" << b << " DNE" << endl;
    } else {
      dout(15) << "get_bl " << a << " DNE" << endl;
    }
    return 0;
  }

  // read size
  __int32_t len = 0;
  ::read(fd, &len, sizeof(len));
  
  // read buffer
  bl.clear();
  bufferptr bp(len);
  ::read(fd, bp.c_str(), len);
  bl.append(bp);
  ::close(fd);

  if (b) {
    dout(15) << "get_bl " << a << "/" << b << " = " << bl.length() << " bytes" << endl;
  } else {
    dout(15) << "get_bl " << a << " = " << bl.length() << " bytes" << endl;
  }

  return len;
}

int MonitorStore::put_bl(bufferlist& bl, const char *a, const char *b)
{
  char fn[200];
  sprintf(fn, "%s/%s", dir.c_str(), a);
  if (b) {
    ::mkdir(fn, 0755);
    dout(15) << "put_bl " << a << "/" << b << " = " << bl.length() << " bytes" << endl;
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "put_bl " << a << " = " << bl.length() << " bytes" << endl;
  }
  
  char tfn[200];
  sprintf(tfn, "%s.new", fn);
  int fd = ::open(tfn, O_WRONLY|O_CREAT);
  assert(fd);
  
  // write size
  __int32_t len = bl.length();
  ::write(fd, &len, sizeof(len));

  // write data
  for (list<bufferptr>::const_iterator it = bl.buffers().begin();
       it != bl.buffers().end();
       it++) 
    ::write(fd, it->c_str(), it->length());
  ::fchmod(fd, 0644);
  ::fsync(fd);
  ::close(fd);
  ::rename(tfn, fn);

  return 0;
}
