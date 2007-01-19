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


#ifndef __MCLIENTFILECAPS_H
#define __MCLIENTFILECAPS_H

#define CLIENT_FILECAP_RELEASE 1  // mds closed the cap
#define CLIENT_FILECAP_STALE   2  // mds has exported the cap
#define CLIENT_FILECAP_REAP    3  // mds has imported the cap from get_mds()

class MClientFileCaps : public Message {
 public:
  static const int FILECAP_RELEASE = 1;
  static const int FILECAP_STALE = 2;
  static const int FILECAP_REAP = 3;


 private:
  inode_t   inode;
  int       caps;
  long      seq;
  int       wanted;
  //int       client;
  
  int       special;   // stale || reap;  in conjunction w/ mds value
  int       mds;

 public:
  inodeno_t get_ino() { return inode.ino; }
  inode_t&  get_inode() { return inode; }
  int       get_caps() { return caps; }
  int       get_wanted() { return wanted; }
  long      get_seq() { return seq; }
  //int       get_client() { return client; }

  // for cap migration
  int       get_mds() { return mds; }
  int       get_special() { return special; }

  //void set_client(int c) { client = c; }
  void set_caps(int c) { caps = c; }
  void set_wanted(int w) { wanted = w; }

  void set_mds(int m) { mds = m; }
  void set_special(int s) { special = s; }

  MClientFileCaps() {}
  MClientFileCaps(inode_t& inode,
                  long seq,
                  int caps,
                  int wanted,
                  int special=0,
                  int mds=0) :
    Message(MSG_CLIENT_FILECAPS) {
    this->inode = inode;
    this->seq = seq;
    this->caps = caps;
    this->wanted = wanted;
    this->special = special;
    this->mds = mds;
  }
  virtual char *get_type_name() { return "Cfcap";}
  
  virtual void decode_payload(crope& s, int& off) {
    s.copy(off, sizeof(seq), (char*)&seq);
    off += sizeof(seq);
    s.copy(off, sizeof(inode), (char*)&inode);
    off += sizeof(inode);
    s.copy(off, sizeof(caps), (char*)&caps);
    off += sizeof(caps);
    s.copy(off, sizeof(wanted), (char*)&wanted);
    off += sizeof(wanted);
    //s.copy(off, sizeof(client), (char*)&client);
    //off += sizeof(client);
    s.copy(off, sizeof(mds), (char*)&mds);
    off += sizeof(mds);
    s.copy(off, sizeof(special), (char*)&special);
    off += sizeof(special);
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&seq, sizeof(seq));
    s.append((char*)&inode, sizeof(inode));
    s.append((char*)&caps, sizeof(caps));
    s.append((char*)&wanted, sizeof(wanted));
    //s.append((char*)&client, sizeof(client));
    s.append((char*)&mds,sizeof(mds));
    s.append((char*)&special,sizeof(special));
  }
};

#endif
