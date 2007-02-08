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


void MonitorStore::init()
{
  // verify dir exists
  DIR *d = ::opendir(dir);
  if (!d) {
	derr(1) << "basedir " << dir << " dne" << endl;
	assert(0);
  }
  ::closedir(d);
}


version_t MonitorStore::get_int(const char *a, const char *b)
{
  char fn[200];
  if (b)
	sprintf(fn, "%s/%s/%s", dir, a, b);
  else
	sprintf(fn, "%s/%s", dir, a);

  FILE *f = ::fopen(fn, "r");
  if (!f) 
	return 0;

  char buf[20];
  ::fgets(buf, 20, f);
  ::fclose(f);

  version_t val = atoi(buf);

  if (b) {
	dout(10) << "get_int " << a << "/" << b << " = " << val << endl;
  } else {
	dout(10) << "get_int " << a << " = " << val << endl;
  }
  return val;
}


void MonitorStore::set_int(version_t val, const char *a, const char *b)
{
  char fn[200];
  if (b) {
	dout(10) << "set_int " << a << "/" << b << " = " << val << endl;
	sprintf(fn, "%s/%s/%s", dir, a, b);
  } else {
	dout(10) << "set_int " << a << " = " << val << endl;
	sprintf(fn, "%s/%s", dir, a);
  }
  
  FILE *f = ::fopen(fn, "w");
  assert(f);
  ::fprintf(f, "%lld\n", val);
  ::fclose(f);
}
