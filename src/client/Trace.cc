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



#include "Trace.h"
#include "common/debug.h"

#include <iostream>
#include <map>

#include "common/Mutex.h"

#include "common/config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>





void Trace::start()
{
  //cout << "start" << std::endl;
  delete fs;

  fs = new ifstream();
  fs->open(filename);
  if (!fs->is_open()) {
    //generic_dout(0) << "** unable to open trace file " << filename << dendl;
    ceph_abort();
  }
  //generic_dout(2) << "opened traced file '" << filename << "'" << dendl;
  
  // read first line
  getline(*fs, line);
  //cout << "first line is " << line << std::endl;

  _line = 1;
}

const char *Trace::peek_string(string &buf, const char *prefix)
{
  //if (prefix) cout << "prefix '" << prefix << "' line '" << line << "'" << std::endl;
  if (prefix &&
      strstr(line.c_str(), "/prefix") == line.c_str()) {
    buf.clear();
    buf.append(prefix);
    buf.append(line.c_str() + strlen("/prefix"));
  } else {
    buf = line;
  }
  return buf.c_str();
}


const char *Trace::get_string(string &buf, const char *prefix)
{
  peek_string(buf, prefix);

  //cout << "buf is " << buf << std::endl;
  // read next line (and detect eof early)
  _line++;
  getline(*fs, line);
  //cout << "next line is " << line << std::endl;

  return buf.c_str();
}
