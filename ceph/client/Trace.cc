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



#include "Trace.h"

#include <iostream>
#include <cassert>
#include <map>
#include <ext/rope>
using namespace __gnu_cxx;

#include "common/Mutex.h"

#include "config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


Mutex trace_lock;

class TokenList {
public:
  string filename;
  char *data;
  int len;
  list<const char *> tokens;
 
  int ref;

  TokenList() : data(0), ref(0) {}
  ~TokenList() {
    delete[] data;
  }
};

map<string, TokenList*> traces;


//
Trace::Trace(const char* f)
{
  string filename = f;

  trace_lock.Lock();
  
  if (traces.count(filename))
    tl = traces[filename];
  else {
    tl = new TokenList;
    tl->filename = filename;

    // open file
    crope cr;
    int fd = open(filename.c_str(), O_RDONLY);
    assert(fd > 0);
    char buf[100];
    while (1) {
      int r = read(fd, buf, 100);
      if (r == 0) break;
      assert(r > 0);
      cr.append(buf, r);
    }
    close(fd);
    
    // copy
    tl->len = cr.length()+1;
    tl->data = new char[tl->len];
    memcpy(tl->data, cr.c_str(), cr.length());
    tl->data[tl->len-1] = '\n';

    // index!
    int o = 0;
    while (o < tl->len) {
      char *n = tl->data + o;
      
      // find newline
      while (tl->data[o] != '\n') o++;
      assert(tl->data[o] == '\n');
      tl->data[o] = 0;
      
      if (tl->data + o > n) tl->tokens.push_back(n);
      o++;
    }

    dout(1) << "trace " << filename << " loaded with " << tl->tokens.size() << " tokens" << endl;
    traces[filename] = tl;
  }

  tl->ref++;

  trace_lock.Unlock();
}

Trace::~Trace()
{
  trace_lock.Lock();
  
  tl->ref--;
  if (tl->ref == 0) {
    traces.erase(tl->filename);
    delete tl;
  }

  trace_lock.Unlock();
}


list<const char*>& Trace::get_list() 
{
  return tl->tokens;
}
