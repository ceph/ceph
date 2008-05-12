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


#ifndef __MDISCOVERREPLY_H
#define __MDISCOVERREPLY_H

#include "msg/Message.h"
#include "mds/CDir.h"
#include "mds/CInode.h"
#include "include/filepath.h"

#include <vector>
#include <string>
using namespace std;

#define max(a,b)  ((a)>(b) ? (a):(b))


/**
 * MDiscoverReply - return new replicas (of inodes, dirs, dentries)
 *
 * we group returned items by (dir, dentry, inode).  each
 * item in each set shares an index (it's "depth").
 *
 * we can start and end with any type.
 *   no_base_dir    = true if the first group has an inode but no dir
 *   no_base_dentry = true if the first group has an inode but no dentry
 * they are false if there is no returned data, ie the first group is empty.
 *
 * we also return errors:
 *   error_flag_dn(string) - the specified dentry dne
 *   error_flag_dir        - the last item wasn't a dir, so we couldn't continue.
 *
 * and sometimes,
 *   dir_auth_hint         - where we think the dir auth is
 *
 * depth() gives us the number of depth units/indices for which we have 
 * information.  this INCLUDES those for which we have errors but no data.
 *
 * see MDCache::handle_discover, handle_discover_reply.
 *
 *
 * so basically, we get
 *
 *   dir den ino   i
 *            x    0
 *    x   x   x    1
 * or
 *        x   x    0
 *    x   x   x    1
 * or
 *    x   x   x    0
 *    x   x   x    1
 * ...and trail off however we want.    
 * 
 * 
 */

class MDiscoverReply : public Message {
  // info about original request
  inodeno_t base_ino;
  frag_t base_dir_frag;  
  bool wanted_base_dir;
  bool wanted_xlocked;
  inodeno_t wanted_ino;

  // and the response
  bool flag_error_dn;
  bool flag_error_ino;
  bool flag_error_dir;
  bool no_base_dir;     // no base dir (but IS dentry+inode)
  bool no_base_dentry;  // no base dentry (but IS inode)
  string error_dentry;   // dentry that was not found (to trigger waiters on asker)
  bool unsolicited;

  __s32 dir_auth_hint;

  vector<CDirDiscover*>    dirs;      // not inode-aligned if no_base_dir = true.
  vector<CDentryDiscover*> dentries;  // not inode-aligned if no_base_dentry = true
  vector<CInodeDiscover*>  inodes;


 public:
  // accessors
  inodeno_t get_base_ino() { return base_ino; }
  frag_t get_base_dir_frag() { return base_dir_frag; }
  bool get_wanted_base_dir() { return wanted_base_dir; }
  bool get_wanted_xlocked() { return wanted_xlocked; }
  inodeno_t get_wanted_ino() { return wanted_ino; }

  int       get_num_inodes() { return inodes.size(); }
  int       get_num_dentries() { return dentries.size(); }
  int       get_num_dirs() { return dirs.size(); }

  int       get_last_inode() { return inodes.size(); }
  int       get_last_dentry() { return dentries.size() + no_base_dentry; }
  int       get_last_dir() { return dirs.size() + no_base_dir; }

  int       get_depth() {   // return depth of deepest object (in dir/dentry/inode units)
    return max( inodes.size(),                                 // at least this many
           max( no_base_dentry + dentries.size() + flag_error_dn, // inode start + path + possible error
                dirs.size() + no_base_dir ));                  // dn/inode + dirs
  }

  bool has_base_dir() { return !no_base_dir && dirs.size(); }
  bool has_base_dentry() { return !no_base_dentry && dentries.size(); }
  bool has_base_inode() { return no_base_dir && no_base_dentry; }

  bool is_flag_error_dn() { return flag_error_dn; }
  bool is_flag_error_ino() { return flag_error_ino; }
  bool is_flag_error_dir() { return flag_error_dir; }
  string& get_error_dentry() { return error_dentry; }

  int get_dir_auth_hint() { return dir_auth_hint; }

  bool is_unsolicited() { return unsolicited; }
  void mark_unsolicited() { unsolicited = true; }

  // these index _arguments_ are aligned to each ([[dir, ] dentry, ] inode) set.
  CInodeDiscover& get_inode(int n) { return *(inodes[n]); }
  CDentryDiscover& get_dentry(int n) { return *(dentries[n - no_base_dentry]); }
  CDirDiscover& get_dir(int n) { return *(dirs[n - no_base_dir]); }
  inodeno_t get_ino(int n) { return inodes[n]->get_ino(); }

  // cons
  MDiscoverReply() {}
  MDiscoverReply(MDiscover *dis) :
    Message(MSG_MDS_DISCOVERREPLY),
    base_ino(dis->get_base_ino()),
    base_dir_frag(dis->get_base_dir_frag()),
    wanted_base_dir(dis->wants_base_dir()),
    wanted_xlocked(dis->wants_xlocked()),
    wanted_ino(dis->get_want_ino()),
    flag_error_dn(false),
    flag_error_ino(false),
    flag_error_dir(false),
    no_base_dir(false), no_base_dentry(false),
    dir_auth_hint(CDIR_AUTH_UNKNOWN) {
  }
  MDiscoverReply(dirfrag_t df) :
    Message(MSG_MDS_DISCOVERREPLY),
    base_ino(df.ino),
    base_dir_frag(df.frag),
    wanted_base_dir(false),
    wanted_xlocked(false),
    wanted_ino(inodeno_t()),
    flag_error_dn(false),
    flag_error_ino(false),
    flag_error_dir(false),
    no_base_dir(false), no_base_dentry(false),
    dir_auth_hint(CDIR_AUTH_UNKNOWN) {
  }
  ~MDiscoverReply() {
    for (vector<CDirDiscover*>::iterator it = dirs.begin();
         it != dirs.end();
         it++) 
      delete *it;
    for (vector<CDentryDiscover*>::iterator it = dentries.begin();
         it != dentries.end();
         it++) 
      delete *it;
    for (vector<CInodeDiscover*>::iterator it = inodes.begin();
         it != inodes.end();
         it++) 
      delete *it;
  }
  const char *get_type_name() { return "DisR"; }
  
  // builders
  bool is_empty() {
    return dirs.empty() && dentries.empty() && inodes.empty() && 
      !flag_error_dn &&
      !flag_error_ino &&
      !flag_error_dir &&
      dir_auth_hint == CDIR_AUTH_UNKNOWN;
  }
  void add_dentry(CDentryDiscover* ddis) {
    if (dentries.empty() && dirs.empty()) no_base_dir = true;
    dentries.push_back(ddis);
  }
  
  void add_inode(CInodeDiscover* din) {
    if (inodes.empty() && dentries.empty()) no_base_dir = no_base_dentry = true; 
    inodes.push_back( din );
  }

  void add_dir(CDirDiscover* dir) {
    dirs.push_back( dir );
  }


  //  void set_flag_forward() { flag_forward = true; }
  void set_flag_error_dn(const string& dn) { 
    flag_error_dn = true; 
    error_dentry = dn; 
  }
  void set_flag_error_ino() {
    flag_error_ino = true;
  }
  void set_flag_error_dir() { 
    flag_error_dir = true; 
  }
  void set_dir_auth_hint(int a) {
    dir_auth_hint = a;
  }
  void set_error_dentry(const string& dn) {
    error_dentry = dn;
  }


  // ...
  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(base_ino, p);
    ::decode(base_dir_frag, p);
    ::decode(wanted_base_dir, p);
    ::decode(wanted_xlocked, p);
    ::decode(flag_error_dn, p);
    ::decode(flag_error_ino, p);
    ::decode(flag_error_dir, p);
    ::decode(no_base_dir, p);
    ::decode(no_base_dentry, p);
    ::decode(error_dentry, p);
    ::decode(dir_auth_hint, p);
    ::decode(unsolicited, p);

    ::decode(dirs, p);
    ::decode(inodes, p);
    ::decode(dentries, p);
  }
  void encode_payload() {
    ::encode(base_ino, payload);
    ::encode(base_dir_frag, payload);
    ::encode(wanted_base_dir, payload);
    ::encode(wanted_xlocked, payload);
    ::encode(flag_error_dn, payload);
    ::encode(flag_error_ino, payload);
    ::encode(flag_error_dir, payload);
    ::encode(no_base_dir, payload);
    ::encode(no_base_dentry, payload);
    ::encode(error_dentry, payload);
    ::encode(dir_auth_hint, payload);
    ::encode(unsolicited, payload);

    ::encode(dirs, payload);
    ::encode(inodes, payload);
    ::encode(dentries, payload);
  }

};

#endif
