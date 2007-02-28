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
 * depth() gives us the number of depth units/indices for which we have 
 * information.  this INCLUDES those for which we have errors but no data.
 *
 * see MDCache::handle_discover, handle_discover_reply.
 *
  
 old crap, maybe not accurate:

  // dir [ + ... ]                 : discover want_base_dir=true
  
  // dentry [ + inode [ + ... ] ]  : discover want_base_dir=false
  //                                 no_base_dir=true
  //  -> we only exclude inode if dentry is null+xlock

  // inode [ + ... ], base_ino = 0 : discover base_ino=0, start w/ root ino,
  //                                 no_base_dir=no_base_dentry=true
  
 * 
 */

class MDiscoverReply : public Message {
  inodeno_t    base_ino;
  bool         no_base_dir;     // no base dir (but IS dentry+inode)
  bool         no_base_dentry;  // no base dentry (but IS inode)
  bool        flag_error_dn;
  bool        flag_error_dir;
  string      error_dentry;   // dentry that was not found (to trigger waiters on asker)

  
  vector<CDirDiscover*>    dirs;      // not inode-aligned if no_base_dir = true.
  vector<CDentryDiscover*> dentries;  // not inode-aligned if no_base_dentry = true
  vector<CInodeDiscover*>  inodes;

  string path;

 public:
  // accessors
  inodeno_t get_base_ino() { return base_ino; }
  int       get_num_inodes() { return inodes.size(); }
  int       get_num_dentries() { return dentries.size(); }
  int       get_num_dirs() { return dirs.size(); }

  int       get_depth() {   // return depth of deepest object (in dir/dentry/inode units)
    return max( inodes.size(),                                 // at least this many
           max( no_base_dentry + dentries.size() + flag_error_dn, // inode start + path + possible error
                dirs.size() + no_base_dir ));                  // dn/inode + dirs
  }

  bool      has_base_dir() { return !no_base_dir && dirs.size(); }
  bool      has_base_dentry() { return !no_base_dentry && dentries.size(); }
  bool has_root() {
    if (base_ino == 0) {
      assert(no_base_dir && no_base_dentry);
      return true;
    }
    return false;
  }

  const string& get_path() { return path; }

  //  bool is_flag_forward() { return flag_forward; }
  bool is_flag_error_dn() { return flag_error_dn; }
  bool is_flag_error_dir() { return flag_error_dir; }
  string& get_error_dentry() { return error_dentry; }

  // these index _arguments_ are aligned to each ([[dir, ] dentry, ] inode) set.
  CDirDiscover& get_dir(int n) { return *(dirs[n - no_base_dir]); }
  CDentryDiscover& get_dentry(int n) { return *(dentries[n - no_base_dentry]); }
  CInodeDiscover& get_inode(int n) { return *(inodes[n]); }
  inodeno_t get_ino(int n) { return inodes[n]->get_ino(); }

  // cons
  MDiscoverReply() {}
  MDiscoverReply(inodeno_t base_ino) :
    Message(MSG_MDS_DISCOVERREPLY) {
    this->base_ino = base_ino;
    flag_error_dn = false;
    flag_error_dir = false;
    no_base_dir = no_base_dentry = false;
  }
  ~MDiscoverReply() {
    for (vector<CDirDiscover*>::iterator it = dirs.begin();
         it != dirs.end();
         it++) 
      delete *it;
    for (vector<CInodeDiscover*>::iterator it = inodes.begin();
         it != inodes.end();
         it++) 
      delete *it;
  }
  virtual char *get_type_name() { return "DisR"; }
  
  // builders
  bool is_empty() {
    return dirs.empty() && dentries.empty() && inodes.empty() && 
      !flag_error_dn &&
      !flag_error_dir;
  }
  void add_dentry(CDentryDiscover* ddis) {
    if (dentries.empty() && dirs.empty()) no_base_dir = true;
    dentries.push_back(ddis);
    if (path.length()) path += "/";
    path += ddis->get_dname();
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
  void set_flag_error_dir() { 
    flag_error_dir = true; 
  }


  // ...
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(base_ino), (char*)&base_ino);
    off += sizeof(base_ino);
    payload.copy(off, sizeof(bool), (char*)&no_base_dir);
    off += sizeof(bool);
    payload.copy(off, sizeof(bool), (char*)&no_base_dentry);
    off += sizeof(bool);
    //    payload.copy(off, sizeof(bool), (char*)&flag_forward);
    //off += sizeof(bool);
    payload.copy(off, sizeof(bool), (char*)&flag_error_dn);
    off += sizeof(bool);
    
    _decode(error_dentry, payload, off);
    payload.copy(off, sizeof(bool), (char*)&flag_error_dir);
    off += sizeof(bool);
    
    // dirs
    int n;
    payload.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      dirs.push_back( new CDirDiscover() );
      dirs[i]->_decode(payload, off);
    }
    //dout(12) << n << " dirs out" << endl;

    // inodes
    payload.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      inodes.push_back( new CInodeDiscover() );
      inodes[i]->_decode(payload, off);
    }
    //dout(12) << n << " inodes out" << endl;

    // dentries
    payload.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      dentries.push_back( new CDentryDiscover() );
      dentries[i]->_decode(payload, off);
    }
  }
  void encode_payload() {
    payload.append((char*)&base_ino, sizeof(base_ino));
    payload.append((char*)&no_base_dir, sizeof(bool));
    payload.append((char*)&no_base_dentry, sizeof(bool));
    //    payload.append((char*)&flag_forward, sizeof(bool));
    payload.append((char*)&flag_error_dn, sizeof(bool));

    _encode(error_dentry, payload);
    payload.append((char*)&flag_error_dir, sizeof(bool));

    // dirs
    int n = dirs.size();
    payload.append((char*)&n, sizeof(int));
    for (vector<CDirDiscover*>::iterator it = dirs.begin();
         it != dirs.end();
         it++) 
      (*it)->_encode( payload );
    //dout(12) << n << " dirs in" << endl;
    
    // inodes
    n = inodes.size();
    payload.append((char*)&n, sizeof(int));
    for (vector<CInodeDiscover*>::iterator it = inodes.begin();
         it != inodes.end();
         it++) 
       (*it)->_encode( payload );
    //dout(12) << n << " inodes in" << endl;

    // dentries
    n = dentries.size();
    payload.append((char*)&n, sizeof(int));
    for (vector<CDentryDiscover*>::iterator it = dentries.begin();
         it != dentries.end();
         it++) 
       (*it)->_encode( payload );
    //dout(12) << n << " dentries in" << endl;
  }

};

#endif
