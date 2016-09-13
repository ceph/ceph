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

#ifndef CEPH_MDS_SERVER_H
#define CEPH_MDS_SERVER_H
#include "CObject.h"

class MDSRank;
class MDCache;
class Locker;
class Session;
class SimpleLock;
class MClientRequest;
class MClientReply;
class MClientReconnect;
class MClientSession;
class LogEvent;

struct MDRequestImpl;
typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;

class Server {
public:
  MDSRank* const mds;
  MDCache* const &mdcache;
  Locker* const &locker;

  Server(MDSRank *_mds);

  void dispatch(Message *m);
  void dispatch_client_request(const MDRequestRef& mdr);

  void find_idle_sessions();
  void trim_client_leases();
  void send_safe_reply(const MDRequestRef& mdr);
protected:

  friend class C_MDS_session_finish;
  void __session_logged(Session *session, uint64_t state_seq, bool open,
		  	version_t pv, interval_set<inodeno_t>& inos,version_t piv);
  void journal_close_session(Session *session, int state, Context *on_safe);
  void handle_client_session(MClientSession *m);
  void kill_session(Session *session, Context *on_safe);

  void handle_client_request(MClientRequest *m);

  void encode_null_lease(bufferlist& bl);
  void encode_empty_dirstat(bufferlist& bl);
  void lock_objects_for_trace(CInode *in, CDentry *dn, const MDRequestRef& mdr);
  void set_trace_dist(Session *session, MClientReply *reply,
		      CInode *in, CDentry *dn, const MDRequestRef& mdr);
  void respond_to_request(const MDRequestRef& mdr, int r);
  bool reply_client_request(const MDRequestRef& mdr, int r);
  void early_reply(const MDRequestRef& mdr);

  void journal_and_reply(const MDRequestRef& mdr, int tracei, int tracedn,
		  	 LogEvent *le, MDSLogContextBase *fin, bool flush=false);

  CInodeRef prepare_new_inode(const MDRequestRef& mdr, CDentryRef& dn, inodeno_t useino,
			      unsigned mode, file_layout_t *layout=NULL);
  void project_alloc_inos(const MDRequestRef& mdr, CInode *in, inodeno_t useino);
  void journal_allocated_inos(const MDRequestRef& mdr, LogEvent *le);
  void apply_allocated_inos(const MDRequestRef& mdr);

  CDentryRef prepare_stray_dentry(const MDRequestRef& mdr, CInode *in);

  int rdlock_path_pin_ref(const MDRequestRef& mdr, int n,
			  set<SimpleLock*> &rdlocks,
			  bool is_lookup);
  int rdlock_path_xlock_dentry(const MDRequestRef& mdr, int n,
			       set<SimpleLock*>& rdlocks,
			       set<SimpleLock*>& wrlocks,
			       set<SimpleLock*>& xlocks,
			       bool okexist, bool mustexist);
  bool directory_is_nonempty(CInodeRef& diri);

  void handle_client_getattr(const MDRequestRef& mdr, bool is_lookup);
  void handle_client_setattr(const MDRequestRef& mdr);
  void handle_client_mknod(const MDRequestRef& mdr);
  void handle_client_symlink(const MDRequestRef& mdr);
  void handle_client_mkdir(const MDRequestRef& mdr);
  void handle_client_readdir(const MDRequestRef& mdr);
  void handle_client_unlink(const MDRequestRef& mdr);
  void handle_client_link(const MDRequestRef& mdr);
  void handle_client_rename(const MDRequestRef& mdr);
  void do_open_truncate(const MDRequestRef& mdr, int cmode);
  void handle_client_open(const MDRequestRef& mdr);
  void handle_client_openc(const MDRequestRef& mdr);

  void __inode_update_finish(const MDRequestRef& mdr, bool truncate_smaller);
  void __mknod_finish(const MDRequestRef& mdr);
  void __unlink_finish(const MDRequestRef& mdr, version_t dnpv);
  void __link_finish(const MDRequestRef& mdr, version_t dnpv);
  void __rename_finish(const MDRequestRef& mdr, version_t srcdn_pv, version_t destdn_pv);
  friend class C_MDS_inode_update_finish;
  friend class C_MDS_mknod_finish;
  friend class C_MDS_unlink_finish;
  friend class C_MDS_link_finish;
  friend class C_MDS_rename_finish;

protected:
  utime_t reconnect_start;
  set<client_t> client_reconnect_gather;
  MDSContextBase *reconnect_done;

  void handle_client_reconnect(MClientReconnect *m);
  void reconnect_gather_finish(int failed=0);
public:
  void reconnect_clients(MDSContextBase *c);
  void reconnect_tick();
};

#endif
