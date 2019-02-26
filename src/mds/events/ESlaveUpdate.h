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

#ifndef CEPH_MDS_ESLAVEUPDATE_H
#define CEPH_MDS_ESLAVEUPDATE_H

#include <string_view>

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
  bufferlist snapbl;

  link_rollback() : ino(0), was_inc(false) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<link_rollback*>& ls);
};
WRITE_CLASS_ENCODER(link_rollback)

/*
 * this is only used on an empty dir with a dirfrag on a remote node.
 * we are auth for nothing.  all we need to do is relink the directory
 * in the hierarchy properly during replay to avoid breaking the
 * subtree map.
 */
struct rmdir_rollback {
  metareqid_t reqid;
  dirfrag_t src_dir;
  string src_dname;
  dirfrag_t dest_dir;
  string dest_dname;
  bufferlist snapbl;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rmdir_rollback*>& ls);
};
WRITE_CLASS_ENCODER(rmdir_rollback)

struct rename_rollback {
  struct drec {
    dirfrag_t dirfrag;
    utime_t dirfrag_old_mtime;
    utime_t dirfrag_old_rctime;
    inodeno_t ino, remote_ino;
    string dname;
    char remote_d_type;
    utime_t old_ctime;
    
    drec() : remote_d_type((char)S_IFREG) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::const_iterator& bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<drec*>& ls);
  };
  WRITE_CLASS_MEMBER_ENCODER(drec)

  metareqid_t reqid;
  drec orig_src, orig_dest;
  drec stray; // we know this is null, but we want dname, old mtime/rctime
  utime_t ctime;
  bufferlist srci_snapbl;
  bufferlist desti_snapbl;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<rename_rollback*>& ls);
};
WRITE_CLASS_ENCODER(rename_rollback::drec)
WRITE_CLASS_ENCODER(rename_rollback)


class ESlaveUpdate : public LogEvent {
public:
  const static int OP_PREPARE = 1;
  const static int OP_COMMIT = 2;
  const static int OP_ROLLBACK = 3;
  
  const static int LINK = 1;
  const static int RENAME = 2;
  const static int RMDIR = 3;

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
  mds_rank_t master;
  __u8 op;  // prepare, commit, abort
  __u8 origop; // link | rename

  ESlaveUpdate() : LogEvent(EVENT_SLAVEUPDATE), master(0), op(0), origop(0) { }
  ESlaveUpdate(MDLog *mdlog, std::string_view s, metareqid_t ri, int mastermds, int o, int oo) :
    LogEvent(EVENT_SLAVEUPDATE),
    type(s),
    reqid(ri),
    master(mastermds),
    op(o), origop(oo) { }
  
  void print(ostream& out) const override {
    if (type.length())
      out << type << " ";
    out << " " << (int)op;
    if (origop == LINK) out << " link";
    if (origop == RENAME) out << " rename";
    out << " " << reqid;
    out << " for mds." << master;
    out << commit;
  }

  EMetaBlob *get_metablob() override { return &commit; }

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  static void generate_test_instances(list<ESlaveUpdate*>& ls);

  void replay(MDSRank *mds) override;
};
WRITE_CLASS_ENCODER_FEATURES(ESlaveUpdate)

#endif
