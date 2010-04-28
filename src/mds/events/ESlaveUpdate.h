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

#ifndef __MDS_ESLAVEUPDATE_H
#define __MDS_ESLAVEUPDATE_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

/*
 * rollback records, for remote/slave updates, which may need to be manually
 * rolled back during journal replay.  (or while active if master fails, but in 
 * that case these records aren't needed.)
 */ 
struct link_rollback {
  metareqid_t reqid;
  inodeno_t ino;
  bool was_inc;
  utime_t old_ctime;
  utime_t old_dir_mtime;
  utime_t old_dir_rctime;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(reqid, bl);
    ::encode(ino, bl);
    ::encode(was_inc, bl);
    ::encode(old_ctime, bl);
    ::encode(old_dir_mtime, bl);
    ::encode(old_dir_rctime, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(reqid, bl);
    ::decode(ino, bl);
    ::decode(was_inc, bl);
    ::decode(old_ctime, bl);
    ::decode(old_dir_mtime, bl);
    ::decode(old_dir_rctime, bl);
  }
};
WRITE_CLASS_ENCODER(link_rollback)

struct rename_rollback {
  struct drec {
    dirfrag_t dirfrag;
    utime_t dirfrag_old_mtime;
    utime_t dirfrag_old_rctime;
    inodeno_t ino, remote_ino;
    string dname;
    char remote_d_type;
    utime_t old_ctime;
    
    void encode(bufferlist &bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      ::encode(dirfrag, bl);
      ::encode(dirfrag_old_mtime, bl);
      ::encode(dirfrag_old_rctime, bl);
      ::encode(ino, bl);
      ::encode(remote_ino, bl);
      ::encode(dname, bl);
      ::encode(remote_d_type, bl);
      ::encode(old_ctime, bl);
   } 
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(dirfrag, bl);
      ::decode(dirfrag_old_mtime, bl);
      ::decode(dirfrag_old_rctime, bl);
      ::decode(ino, bl);
      ::decode(remote_ino, bl);
      ::decode(dname, bl);
      ::decode(remote_d_type, bl);
      ::decode(old_ctime, bl);
    }
  };
  WRITE_CLASS_ENCODER_MEMBER(drec)

  metareqid_t reqid;
  drec orig_src, orig_dest;
  drec stray; // we know this is null, but we want dname, old mtime/rctime
  utime_t ctime;

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(reqid, bl);
    encode(orig_src, bl);
    encode(orig_dest, bl);
    encode(stray, bl);
    ::encode(ctime, bl);
 }
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(reqid, bl);
    decode(orig_src, bl);
    decode(orig_dest, bl);
    decode(stray, bl);
    ::decode(ctime, bl);
  }
};
WRITE_CLASS_ENCODER(rename_rollback)


class ESlaveUpdate : public LogEvent {
public:
  const static int OP_PREPARE = 1;
  const static int OP_COMMIT = 2;
  const static int OP_ROLLBACK = 3;
  
  const static int LINK = 1;
  const static int RENAME = 2;
  /*
   * we journal a rollback metablob that contains the unmodified metadata
   * too, because we may be updating previously dirty metadata, which 
   * will allow old log segments to be trimmed.  if we end of rolling back,
   * those updates could be lost.. so we re-journal the unmodified metadata,
   * and replay will apply _either_ commit or rollback.
   */
  EMetaBlob commit;
  bufferlist rollback;
  string type;
  metareqid_t reqid;
  __s32 master;
  __u8 op;  // prepare, commit, abort
  __u8 origop; // link | rename

  ESlaveUpdate() : LogEvent(EVENT_SLAVEUPDATE) { }
  ESlaveUpdate(MDLog *mdlog, const char *s, metareqid_t ri, int mastermds, int o, int oo) : 
    LogEvent(EVENT_SLAVEUPDATE), commit(mdlog), 
    type(s),
    reqid(ri),
    master(mastermds),
    op(o), origop(oo) { }
  
  void print(ostream& out) {
    if (type.length())
      out << type << " ";
    out << " " << (int)op;
    if (origop == LINK) out << " link";
    if (origop == RENAME) out << " rename";
    out << " " << reqid;
    out << " for mds" << master;
    out << commit;
  }

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(type, bl);
    ::encode(reqid, bl);
    ::encode(master, bl);
    ::encode(op, bl);
    ::encode(origop, bl);
    ::encode(commit, bl);
    ::encode(rollback, bl);
  } 
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(type, bl);
    ::decode(reqid, bl);
    ::decode(master, bl);
    ::decode(op, bl);
    ::decode(origop, bl);
    ::decode(commit, bl);
    ::decode(rollback, bl);
  }

  void replay(MDS *mds);
};

#endif
