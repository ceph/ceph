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


#ifndef CEPH_SYNTHETICCLIENT_H
#define CEPH_SYNTHETICCLIENT_H

#include <pthread.h>

#include "Client.h"
#include "include/Distribution.h"

#include "Trace.h"

#define SYNCLIENT_FIRST_POOL	0

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

#define SYNCLIENT_MODE_RMFILE      19
#define SYNCLIENT_MODE_WRITEFILE   20
#define SYNCLIENT_MODE_READFILE    21
#define SYNCLIENT_MODE_WRITEBATCH  22
#define SYNCLIENT_MODE_WRSHARED    23
#define SYNCLIENT_MODE_READSHARED    24
#define SYNCLIENT_MODE_RDWRRANDOM    25
#define SYNCLIENT_MODE_RDWRRANDOM_EX    26

#define SYNCLIENT_MODE_LINKTEST   27

#define SYNCLIENT_MODE_OVERLOAD_OSD_0 28 // two args

#define SYNCLIENT_MODE_DROPCACHE   29

#define SYNCLIENT_MODE_TRACE       30

#define SYNCLIENT_MODE_CREATEOBJECTS 35
#define SYNCLIENT_MODE_OBJECTRW 36

#define SYNCLIENT_MODE_OPENTEST     40
#define SYNCLIENT_MODE_OPTEST       41

#define SYNCLIENT_MODE_ONLY        50
#define SYNCLIENT_MODE_ONLYRANGE   51
#define SYNCLIENT_MODE_EXCLUDE     52
#define SYNCLIENT_MODE_EXCLUDERANGE  53

#define SYNCLIENT_MODE_UNTIL       55
#define SYNCLIENT_MODE_SLEEPUNTIL  56

#define SYNCLIENT_MODE_RANDOMSLEEP  61
#define SYNCLIENT_MODE_SLEEP        62

#define SYNCLIENT_MODE_DUMP 63

#define SYNCLIENT_MODE_LOOKUPHASH     70
#define SYNCLIENT_MODE_LOOKUPINO     71

#define SYNCLIENT_MODE_TRUNCATE     200

#define SYNCLIENT_MODE_FOO        100
#define SYNCLIENT_MODE_THRASHLINKS  101

#define SYNCLIENT_MODE_IMPORTFIND 300

#define SYNCLIENT_MODE_CHUNK    400

#define SYNCLIENT_MODE_MKSNAP 1000
#define SYNCLIENT_MODE_RMSNAP 1001

#define SYNCLIENT_MODE_MKSNAPFILE 1002



void parse_syn_options(vector<const char*>& args);

class SyntheticClient {
  Client *client;
  int whoami;

  pthread_t thread_id;

  Distribution op_dist;

  void init_op_dist();
  int get_op();

  
  filepath             cwd;
  map<string, struct stat*> contents;
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
    while (r--) ++it;
    return *it;
  }


  filepath n1;
  const char *get_random_subdir() {
    assert(!subdirs.empty());
    int r = ((rand() % subdirs.size()) + (rand() % subdirs.size())) / 2;  // non-uniform distn
    set<string>::iterator it = subdirs.begin();
    while (r--) ++it;

    n1 = cwd;
    n1.push_dentry( *it );
    return n1.get_path().c_str();
  }
  filepath n2;
  const char *get_random_sub() {
    assert(!contents.empty());
    int r = ((rand() % contents.size()) + (rand() % contents.size())) / 2;  // non-uniform distn
    if (cwd.depth() && cwd.last_dentry().length()) 
      r += cwd.last_dentry().c_str()[0];                                         // slightly permuted
    r %= contents.size();

    map<string,struct stat*>::iterator it = contents.begin();
    while (r--) ++it;

    n2 = cwd;
    n2.push_dentry( it->first );
    return n2.get_path().c_str();
  }
  
  filepath sub;
  char sub_s[50];
  const char *make_sub(const char *base) {
    snprintf(sub_s, sizeof(sub_s), "%s.%d", base, rand() % 100);
    string f = sub_s;
    sub = cwd;
    sub.push_dentry(f);
    return sub.c_str();
  }

 public:
  SyntheticClient(Client *client, int w = -1);

  int start_thread();
  int join_thread();

  int run();

  bool run_me() {
    if (run_only >= 0) {
      if (run_only == client->get_nodeid())
        return true;
      else
	return false;
    }
    return true;
  }
  void did_run_me() {
    run_only = -1;
    run_until = utime_t();
  }

  // run() will do one of these things:
  list<int> modes;
  list<string> sargs;
  list<int> iargs;
  utime_t run_start;
  utime_t run_until;

  client_t run_only;
  client_t exclude;

  string get_sarg(int seq);
  int get_iarg() {
    int i = iargs.front();
    iargs.pop_front();
    return i;
  }

  bool time_to_stop() {
    utime_t now = ceph_clock_now(client->cct);
    if (0) cout << "time_to_stop .. now " << now 
		<< " until " << run_until 
		<< " start " << run_start 
		<< std::endl;
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

  int dump_placement(string& fn);


  int make_dirs(const char *basedir, int dirs, int files, int depth);
  int stat_dirs(const char *basedir, int dirs, int files, int depth);
  int read_dirs(const char *basedir, int dirs, int files, int depth);
  int make_files(int num, int count, int priv, bool more);
  int link_test();

  int create_shared(int num);
  int open_shared(int num, int count);

  int rm_file(string& fn);
  int write_file(string& fn, int mb, loff_t chunk);
  int write_fd(int fd, int size, int wrsize);

  int write_batch(int nfile, int mb, int chunk);
  int read_file(const std::string& fn, int mb, int chunk, bool ignoreprint=false);

  int create_objects(int nobj, int osize, int inflight);
  int object_rw(int nobj, int osize, int wrpc, int overlap, 
		double rskew, double wskew);

  int read_random(string& fn, int mb, int chunk);
  int read_random_ex(string& fn, int mb, int chunk);
  
  int overload_osd_0(int n, int sie, int wrsize);
  int check_first_primary(int fd);

  int clean_dir(string& basedir);

  int play_trace(Trace& t, string& prefix, bool metadata_only=false);

  void make_dir_mess(const char *basedir, int n);
  void foo();

  int thrash_links(const char *basedir, int dirs, int files, int depth, int n);

  void import_find(const char *basedir, const char *find, bool writedata);

  int lookup_hash(inodeno_t ino, inodeno_t dirino, const char *name);
  int lookup_ino(inodeno_t ino);

  int chunk_file(string &filename);

  void mksnap(const char *base, const char *name);
  void rmsnap(const char *base, const char *name);
  void mksnapfile(const char *dir);

};

#endif
