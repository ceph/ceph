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


#ifndef __SYNTHETICCLIENT_H
#define __SYNTHETICCLIENT_H

#include <pthread.h>
#include <sstream>

#include "Client.h"
#include "include/Distribution.h"

#include "Trace.h"

#define SYNCLIENT_MODE_RANDOMWALK  1
#define SYNCLIENT_MODE_FULLWALK    2
#define SYNCLIENT_MODE_REPEATWALK  3

#define SYNCLIENT_MODE_MAKEDIRMESS  7
#define SYNCLIENT_MODE_MAKEDIRS     8      // dirs files depth
#define SYNCLIENT_MODE_STATDIRS     9     // dirs files depth
#define SYNCLIENT_MODE_READDIRS     10     // dirs files depth

#define SYNCLIENT_MODE_MAKEFILES    11     // num count private
#define SYNCLIENT_MODE_MAKEFILES2   12     // num count private
#define SYNCLIENT_MODE_CREATESHARED 13     // num
#define SYNCLIENT_MODE_OPENSHARED   14     // num count
#define SYNCLIENT_MODE_IOR2         15
#define SYNCLIENT_MODE_MIXED        16
#define SYNCLIENT_MODE_RENEWAL      17

#define SYNCLIENT_MODE_WRITEFILE   20
#define SYNCLIENT_MODE_READFILE    21
#define SYNCLIENT_MODE_WRITEBATCH  22
#define SYNCLIENT_MODE_WRSHARED    23

#define SYNCLIENT_MODE_TRACE       30

#define SYNCLIENT_MODE_OPENTEST     40
#define SYNCLIENT_MODE_OPTEST       41

#define SYNCLIENT_MODE_ONLY        50
#define SYNCLIENT_MODE_UNTIL       51
#define SYNCLIENT_MODE_SLEEPUNTIL  52

#define SYNCLIENT_MODE_RANDOMSLEEP  61
#define SYNCLIENT_MODE_SLEEP        62




void parse_syn_options(vector<char*>& args);

class SyntheticClient {
  Client *client;

  pthread_t thread_id;

  Distribution op_dist;

  void init_op_dist();
  int get_op();

  
  filepath             cwd;
  map<string, inode_t> contents;
  set<string>          subdirs;
  bool                 did_readdir;
  set<int>             open_files;

  void up();

  void clear_dir() {
    contents.clear();
    subdirs.clear();
    did_readdir = false;
  }

  int get_random_fh() {
    int r = rand() % open_files.size();
    set<int>::iterator it = open_files.begin();
    while (r--) it++;
    return *it;
  }


  filepath n1;
  const char *get_random_subdir() {
    assert(!subdirs.empty());
    int r = ((rand() % subdirs.size()) + (rand() % subdirs.size())) / 2;  // non-uniform distn
    set<string>::iterator it = subdirs.begin();
    while (r--) it++;

    n1 = cwd;
    n1.add_dentry( *it );
    return n1.get_path().c_str();
  }
  filepath n2;
  const char *get_random_sub() {
    assert(!contents.empty());
    int r = ((rand() % contents.size()) + (rand() % contents.size())) / 2;  // non-uniform distn
    if (cwd.depth() && cwd.last_bit().length()) 
      r += cwd.last_bit().c_str()[0];                                         // slightly permuted
    r %= contents.size();

    map<string,inode_t>::iterator it = contents.begin();
    while (r--) it++;

    n2 = cwd;
    n2.add_dentry( it->first );
    return n2.get_path().c_str();
  }
  
  filepath sub;
  char sub_s[50];
  const char *make_sub(char *base) {
    sprintf(sub_s, "%s.%d", base, rand() % 100);
    string f = sub_s;
    sub = cwd;
    sub.add_dentry(f);
    return sub.c_str();
  }

 public:
  SyntheticClient(Client *client);

  int start_thread();
  int join_thread();

  int run();

  bool run_me() {
    if (run_only >= 0) {
      if (run_only == client->get_nodeid()) {
        run_only = -1;
        return true;
      }
      run_only = -1;
      return false;
    }
    return true;
  }

  // run() will do one of these things:
  list<int> modes;
  list<string> sargs;
  list<int> iargs;
  utime_t run_start;
  utime_t run_until;

  int     run_only;

  string get_sarg(int seq);

  bool time_to_stop() {
    utime_t now = g_clock.now();
    if (0) cout << "time_to_stop .. now " << now 
         << " until " << run_until 
         << " start " << run_start 
         << endl;
    if (run_until.sec() && now > run_until) 
      return true;
    else
      return false;
  }

  string compose_path(string& prefix, char *rest) {
    return prefix + rest;
  }

  int full_walk(string& fromdir);
  int random_walk(int n);

  int make_dirs(const char *basedir, int dirs, int files, int depth);
  int stat_dirs(const char *basedir, int dirs, int files, int depth);
  int read_dirs(const char *basedir, int dirs, int files, int depth);
  int make_files(int num, int count, int priv, bool more);

  int create_shared(int num);
  int open_shared(int num, int count);

  int ior2_bench(int num);
  int write_file(string& fn, int mb, int chunk);
  int write_batch(int nfile, int mb, int chunk);
  int read_file(string& fn, int mb, int chunk);

  int clean_dir(string& basedir);

  int play_trace(Trace& t, string& prefix);

  void make_dir_mess(const char *basedir, int n);

};

#endif
