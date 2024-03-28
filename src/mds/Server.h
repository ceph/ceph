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

#include <string_view>

using namespace std::literals::string_view_literals;

#include <common/DecayCounter.h>

#include "include/common_fwd.h"

#include "messages/MClientReconnect.h"
#include "messages/MClientReply.h"
#include "messages/MClientRequest.h"
#include "messages/MClientSession.h"
#include "messages/MClientSnap.h"
#include "messages/MClientReclaim.h"
#include "messages/MClientReclaimReply.h"
#include "messages/MLock.h"

#include "CInode.h"
#include "MDSRank.h"
#include "Mutation.h"
#include "MDSContext.h"

class OSDMap;
class LogEvent;
class EMetaBlob;
class EUpdate;
class MDLog;
struct SnapInfo;
class MetricsHandler;

enum {
  l_mdss_first = 1000,
  l_mdss_dispatch_client_request,
  l_mdss_dispatch_peer_request,
  l_mdss_handle_client_request,
  l_mdss_handle_client_session,
  l_mdss_handle_peer_request,
  l_mdss_req_create_latency,
  l_mdss_req_getattr_latency,
  l_mdss_req_getfilelock_latency,
  l_mdss_req_link_latency,
  l_mdss_req_lookup_latency,
  l_mdss_req_lookuphash_latency,
  l_mdss_req_lookupino_latency,
  l_mdss_req_lookupname_latency,
  l_mdss_req_lookupparent_latency,
  l_mdss_req_lookupsnap_latency,
  l_mdss_req_lssnap_latency,
  l_mdss_req_mkdir_latency,
  l_mdss_req_mknod_latency,
  l_mdss_req_mksnap_latency,
  l_mdss_req_open_latency,
  l_mdss_req_readdir_latency,
  l_mdss_req_rename_latency,
  l_mdss_req_renamesnap_latency,
  l_mdss_req_snapdiff_latency,
  l_mdss_req_rmdir_latency,
  l_mdss_req_rmsnap_latency,
  l_mdss_req_rmxattr_latency,
  l_mdss_req_setattr_latency,
  l_mdss_req_setdirlayout_latency,
  l_mdss_req_setfilelock_latency,
  l_mdss_req_setlayout_latency,
  l_mdss_req_setxattr_latency,
  l_mdss_req_symlink_latency,
  l_mdss_req_unlink_latency,
  l_mdss_cap_revoke_eviction,
  l_mdss_cap_acquisition_throttle,
  l_mdss_req_getvxattr_latency,
  l_mdss_last,
};

class Server {
public:
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  enum class RecallFlags : uint64_t {
    NONE = 0,
    STEADY = (1<<0),
    ENFORCE_MAX = (1<<1),
    TRIM = (1<<2),
    ENFORCE_LIVENESS = (1<<3),
  };

  explicit Server(MDSRank *m, MetricsHandler *metrics_handler);
  ~Server() {
    g_ceph_context->get_perfcounters_collection()->remove(logger);
    delete logger;
    delete reconnect_done;
  }

  void create_logger();

  // message handler
  void dispatch(const cref_t<Message> &m);

  void handle_osd_map();

  // -- sessions and recovery --
  bool waiting_for_reconnect(client_t c) const;
  void dump_reconnect_status(Formatter *f) const;

  time last_recalled() const {
    return last_recall_state;
  }

  void handle_client_session(const cref_t<MClientSession> &m);
  void _session_logged(Session *session, uint64_t state_seq, bool open, version_t pv,
		       const interval_set<inodeno_t>& inos_to_free, version_t piv,
		       const interval_set<inodeno_t>& inos_to_purge, LogSegment *ls);
  version_t prepare_force_open_sessions(std::map<client_t,entity_inst_t> &cm,
					std::map<client_t,client_metadata_t>& cmm,
					std::map<client_t,std::pair<Session*,uint64_t> >& smap);
  void finish_force_open_sessions(const std::map<client_t,std::pair<Session*,uint64_t> >& smap,
				  bool dec_import=true);
  void flush_client_sessions(std::set<client_t>& client_set, MDSGatherBuilder& gather);
  void finish_flush_session(Session *session, version_t seq);
  void terminate_sessions();
  void find_idle_sessions();

  void kill_session(Session *session, Context *on_safe);
  size_t apply_blocklist();
  void journal_close_session(Session *session, int state, Context *on_safe);

  size_t get_num_pending_reclaim() const { return client_reclaim_gather.size(); }
  Session *find_session_by_uuid(std::string_view uuid);
  void reclaim_session(Session *session, const cref_t<MClientReclaim> &m);
  void finish_reclaim_session(Session *session, const ref_t<MClientReclaimReply> &reply=nullptr);
  void handle_client_reclaim(const cref_t<MClientReclaim> &m);

  void reconnect_clients(MDSContext *reconnect_done_);
  void handle_client_reconnect(const cref_t<MClientReconnect> &m);
  void infer_supported_features(Session *session, client_metadata_t& client_metadata);
  void update_required_client_features();

  //void process_reconnect_cap(CInode *in, int from, ceph_mds_cap_reconnect& capinfo);
  void reconnect_gather_finish();
  void reconnect_tick();
  void recover_filelocks(CInode *in, bufferlist locks, int64_t client);

  std::pair<bool, uint64_t> recall_client_state(MDSGatherBuilder* gather, RecallFlags=RecallFlags::NONE);
  void force_clients_readonly();

  // -- requests --
  void trim_completed_request_list(ceph_tid_t tid, Session *session);
  void handle_client_request(const cref_t<MClientRequest> &m);
  void handle_client_reply(const cref_t<MClientReply> &m);

  void journal_and_reply(const MDRequestRef& mdr, CInode *tracei, CDentry *tracedn,
			 LogEvent *le, MDSLogContextBase *fin);
  void submit_mdlog_entry(LogEvent *le, MDSLogContextBase *fin,
                          const MDRequestRef& mdr, std::string_view event);
  void dispatch_client_request(const MDRequestRef& mdr);
  void perf_gather_op_latency(const cref_t<MClientRequest> &req, utime_t lat);
  void early_reply(const MDRequestRef& mdr, CInode *tracei, CDentry *tracedn);
  void respond_to_request(const MDRequestRef& mdr, int r = 0);
  void set_trace_dist(const ref_t<MClientReply> &reply, CInode *in, CDentry *dn,
		      const MDRequestRef& mdr);

  void handle_peer_request(const cref_t<MMDSPeerRequest> &m);
  void handle_peer_request_reply(const cref_t<MMDSPeerRequest> &m);
  void dispatch_peer_request(const MDRequestRef& mdr);
  void handle_peer_auth_pin(const MDRequestRef& mdr);
  void handle_peer_auth_pin_ack(const MDRequestRef& mdr, const cref_t<MMDSPeerRequest> &ack);

  // some helpers
  bool check_fragment_space(const MDRequestRef& mdr, CDir *in);
  bool check_dir_max_entries(const MDRequestRef& mdr, CDir *in);
  bool check_access(const MDRequestRef& mdr, CInode *in, unsigned mask);
  bool _check_access(Session *session, CInode *in, unsigned mask, int caller_uid, int caller_gid, int setattr_uid, int setattr_gid);
  CDentry *prepare_stray_dentry(const MDRequestRef& mdr, CInode *in);
  CInode* prepare_new_inode(const MDRequestRef& mdr, CDir *dir, inodeno_t useino, unsigned mode,
			    const file_layout_t *layout=nullptr);
  void journal_allocated_inos(const MDRequestRef& mdr, EMetaBlob *blob);
  void apply_allocated_inos(const MDRequestRef& mdr, Session *session);

  void _try_open_ino(const MDRequestRef& mdr, int r, inodeno_t ino);
  CInode* rdlock_path_pin_ref(const MDRequestRef& mdr, bool want_auth,
			      bool no_want_auth=false);
  CDentry* rdlock_path_xlock_dentry(const MDRequestRef& mdr, bool create,
				    bool okexist=false, bool authexist=false,
				    bool want_layout=false);
  std::pair<CDentry*, CDentry*>
	    rdlock_two_paths_xlock_destdn(const MDRequestRef& mdr, bool xlock_srcdn);

  CDir* try_open_auth_dirfrag(CInode *diri, frag_t fg, const MDRequestRef& mdr);

  // requests on existing inodes.
  void handle_client_getattr(const MDRequestRef& mdr, bool is_lookup);
  void handle_client_lookup_ino(const MDRequestRef& mdr,
				bool want_parent, bool want_dentry);
  void _lookup_snap_ino(const MDRequestRef& mdr);
  void _lookup_ino_2(const MDRequestRef& mdr, int r);
  void handle_client_readdir(const MDRequestRef& mdr);
  void handle_client_file_setlock(const MDRequestRef& mdr);
  void handle_client_file_readlock(const MDRequestRef& mdr);

  bool xlock_policylock(const MDRequestRef& mdr, CInode *in,
			bool want_layout=false, bool xlock_snaplock=false);
  CInode* try_get_auth_inode(const MDRequestRef& mdr, inodeno_t ino);
  void handle_client_setattr(const MDRequestRef& mdr);
  void handle_client_setlayout(const MDRequestRef& mdr);
  void handle_client_setdirlayout(const MDRequestRef& mdr);

  int parse_quota_vxattr(std::string name, std::string value, quota_info_t *quota);
  void create_quota_realm(CInode *in);
  int parse_layout_vxattr_json(std::string name, std::string value,
			       const OSDMap& osdmap, file_layout_t *layout);
  int parse_layout_vxattr_string(std::string name, std::string value, const OSDMap& osdmap,
				 file_layout_t *layout);
  int parse_layout_vxattr(std::string name, std::string value, const OSDMap& osdmap,
			  file_layout_t *layout, bool validate=true);
  int check_layout_vxattr(const MDRequestRef& mdr,
                          std::string name,
                          std::string value,
                          file_layout_t *layout);
  void handle_set_vxattr(const MDRequestRef& mdr, CInode *cur);
  void handle_remove_vxattr(const MDRequestRef& mdr, CInode *cur);
  void handle_client_getvxattr(const MDRequestRef& mdr);
  void handle_client_setxattr(const MDRequestRef& mdr);
  void handle_client_removexattr(const MDRequestRef& mdr);

  void handle_client_fsync(const MDRequestRef& mdr);
  
  // check layout
  bool is_valid_layout(file_layout_t *layout);

  // open
  void handle_client_open(const MDRequestRef& mdr);
  void handle_client_openc(const MDRequestRef& mdr);  // O_CREAT variant.
  void do_open_truncate(const MDRequestRef& mdr, int cmode);  // O_TRUNC variant.

  // namespace changes
  void handle_client_mknod(const MDRequestRef& mdr);
  void handle_client_mkdir(const MDRequestRef& mdr);
  void handle_client_symlink(const MDRequestRef& mdr);

  // link
  void handle_client_link(const MDRequestRef& mdr);
  void _link_local(const MDRequestRef& mdr, CDentry *dn, CInode *targeti, SnapRealm *target_realm);
  void _link_local_finish(const MDRequestRef& mdr, CDentry *dn, CInode *targeti,
			  version_t, version_t, bool);

  void _link_remote(const MDRequestRef& mdr, bool inc, CDentry *dn, CInode *targeti);
  void _link_remote_finish(const MDRequestRef& mdr, bool inc, CDentry *dn, CInode *targeti,
			   version_t);

  void handle_peer_link_prep(const MDRequestRef& mdr);
  void _logged_peer_link(const MDRequestRef& mdr, CInode *targeti, bool adjust_realm);
  void _commit_peer_link(const MDRequestRef& mdr, int r, CInode *targeti);
  void _committed_peer(const MDRequestRef& mdr);  // use for rename, too
  void handle_peer_link_prep_ack(const MDRequestRef& mdr, const cref_t<MMDSPeerRequest> &m);
  void do_link_rollback(bufferlist &rbl, mds_rank_t leader, const MDRequestRef& mdr);
  void _link_rollback_finish(MutationRef& mut, const MDRequestRef& mdr,
			     std::map<client_t,ref_t<MClientSnap>>& split);

  // unlink
  void handle_client_unlink(const MDRequestRef& mdr);
  bool _dir_is_nonempty_unlocked(const MDRequestRef& mdr, CInode *rmdiri);
  bool _dir_is_nonempty(const MDRequestRef& mdr, CInode *rmdiri);
  void _unlink_local(const MDRequestRef& mdr, CDentry *dn, CDentry *straydn);
  void _unlink_local_finish(const MDRequestRef& mdr,
			    CDentry *dn, CDentry *straydn,
			    version_t);
  bool _rmdir_prepare_witness(const MDRequestRef& mdr, mds_rank_t who, std::vector<CDentry*>& trace, CDentry *straydn);
  void handle_peer_rmdir_prep(const MDRequestRef& mdr);
  void _logged_peer_rmdir(const MDRequestRef& mdr, CDentry *srcdn, CDentry *straydn);
  void _commit_peer_rmdir(const MDRequestRef& mdr, int r, CDentry *straydn);
  void handle_peer_rmdir_prep_ack(const MDRequestRef& mdr, const cref_t<MMDSPeerRequest> &ack);
  void do_rmdir_rollback(bufferlist &rbl, mds_rank_t leader, const MDRequestRef& mdr);
  void _rmdir_rollback_finish(const MDRequestRef& mdr, metareqid_t reqid, CDentry *dn, CDentry *straydn);

  // rename
  void handle_client_rename(const MDRequestRef& mdr);
  void _rename_finish(const MDRequestRef& mdr,
		      CDentry *srcdn, CDentry *destdn, CDentry *straydn);

  void handle_client_lssnap(const MDRequestRef& mdr);
  void handle_client_mksnap(const MDRequestRef& mdr);
  void _mksnap_finish(const MDRequestRef& mdr, CInode *diri, SnapInfo &info);
  void handle_client_rmsnap(const MDRequestRef& mdr);
  void _rmsnap_finish(const MDRequestRef& mdr, CInode *diri, snapid_t snapid);
  void handle_client_renamesnap(const MDRequestRef& mdr);
  void _renamesnap_finish(const MDRequestRef& mdr, CInode *diri, snapid_t snapid);
  void handle_client_readdir_snapdiff(const MDRequestRef& mdr);

  // helpers
  bool _rename_prepare_witness(const MDRequestRef& mdr, mds_rank_t who, std::set<mds_rank_t> &witnesse,
			       std::vector<CDentry*>& srctrace, std::vector<CDentry*>& dsttrace, CDentry *straydn);
  version_t _rename_prepare_import(const MDRequestRef& mdr, CDentry *srcdn, bufferlist *client_map_bl);
  bool _need_force_journal(CInode *diri, bool empty);
  void _rename_prepare(const MDRequestRef& mdr,
		       EMetaBlob *metablob, bufferlist *client_map_bl,
		       CDentry *srcdn, CDentry *destdn, std::string_view alternate_name,
                       CDentry *straydn);
  /* set not_journaling=true if you're going to discard the results --
   * this bypasses the asserts to make sure we're journaling the right
   * things on the right nodes */
  void _rename_apply(const MDRequestRef& mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn);

  // slaving
  void handle_peer_rename_prep(const MDRequestRef& mdr);
  void handle_peer_rename_prep_ack(const MDRequestRef& mdr, const cref_t<MMDSPeerRequest> &m);
  void handle_peer_rename_notify_ack(const MDRequestRef& mdr, const cref_t<MMDSPeerRequest> &m);
  void _peer_rename_sessions_flushed(const MDRequestRef& mdr);
  void _logged_peer_rename(const MDRequestRef& mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  void _commit_peer_rename(const MDRequestRef& mdr, int r, CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  void do_rename_rollback(bufferlist &rbl, mds_rank_t leader, const MDRequestRef& mdr, bool finish_mdr=false);
  void _rename_rollback_finish(MutationRef& mut, const MDRequestRef& mdr, CDentry *srcdn, version_t srcdnpv,
			       CDentry *destdn, CDentry *staydn, std::map<client_t,ref_t<MClientSnap>> splits[2],
			       bool finish_mdr);

  void evict_cap_revoke_non_responders();
  void handle_conf_change(const std::set<std::string>& changed);

  bool terminating_sessions = false;

  std::set<client_t> client_reclaim_gather;

  std::set<client_t> get_laggy_clients() const {
    return laggy_clients;
  }
  void clear_laggy_clients() {
    laggy_clients.clear();
  }

  const bufferlist& get_snap_trace(Session *session, SnapRealm *realm) const;
  const bufferlist& get_snap_trace(client_t client, SnapRealm *realm) const;

private:
  friend class MDSContinuation;
  friend class ServerContext;
  friend class ServerLogContext;
  friend class Batch_Getattr_Lookup;

  // placeholder for validation handler to store xattr specific
  // data
  struct XattrInfo {
    virtual ~XattrInfo() {
    }
  };

  struct MirrorXattrInfo : XattrInfo {
    std::string cluster_id;
    std::string fs_id;

    static const std::string MIRROR_INFO_REGEX;
    static const std::string CLUSTER_ID;
    static const std::string FS_ID;

    MirrorXattrInfo(std::string_view cluster_id,
                    std::string_view fs_id)
      : cluster_id(cluster_id),
        fs_id(fs_id) {
    }
  };

  struct XattrOp {
    int op;
    std::string xattr_name;
    const bufferlist &xattr_value;
    int flags = 0;

    std::unique_ptr<XattrInfo> xinfo;

    XattrOp(int op, std::string_view xattr_name, const bufferlist &xattr_value, int flags)
      : op(op),
        xattr_name(xattr_name),
        xattr_value(xattr_value),
        flags (flags) {
    }
  };

  struct XattrHandler {
    const std::string xattr_name;
    const std::string description;

    // basic checks are to be done in this handler. return -errno to
    // reject xattr request (set or remove), zero to proceed. handlers
    // may parse xattr value for verification if needed and have an
    // option to store custom data in XattrOp::xinfo.
    int (Server::*validate)(CInode *cur, const InodeStoreBase::xattr_map_const_ptr xattrs,
                            XattrOp *xattr_op);

    // set xattr for an inode in xattr_map
    void (Server::*setxattr)(CInode *cur, InodeStoreBase::xattr_map_ptr xattrs,
                             const XattrOp &xattr_op);

    // remove xattr for an inode from xattr_map
    void (Server::*removexattr)(CInode *cur, InodeStoreBase::xattr_map_ptr xattrs,
                                const XattrOp &xattr_op);
  };

  inline static const std::string DEFAULT_HANDLER = "<default>";
  static const XattrHandler xattr_handlers[];

  const XattrHandler* get_xattr_or_default_handler(std::string_view xattr_name);

  // generic variant to set/remove xattr in/from xattr_map
  int xattr_validate(CInode *cur, const InodeStoreBase::xattr_map_const_ptr xattrs,
                     const std::string &xattr_name, int op, int flags);
  void xattr_set(InodeStoreBase::xattr_map_ptr xattrs, const std::string &xattr_name,
                 const bufferlist &xattr_value);
  void xattr_rm(InodeStoreBase::xattr_map_ptr xattrs, const std::string &xattr_name);

  // default xattr handlers
  int default_xattr_validate(CInode *cur, const InodeStoreBase::xattr_map_const_ptr xattrs,
                             XattrOp *xattr_op);
  void default_setxattr_handler(CInode *cur, InodeStoreBase::xattr_map_ptr xattrs,
                                const XattrOp &xattr_op);
  void default_removexattr_handler(CInode *cur, InodeStoreBase::xattr_map_ptr xattrs,
                                   const XattrOp &xattr_op);

  // mirror info xattr handler
  int parse_mirror_info_xattr(const std::string &name, const std::string &value,
                              std::string &cluster_id, std::string &fs_id);
  int mirror_info_xattr_validate(CInode *cur, const InodeStoreBase::xattr_map_const_ptr xattrs,
                                 XattrOp *xattr_op);
  void mirror_info_setxattr_handler(CInode *cur, InodeStoreBase::xattr_map_ptr xattrs,
                                    const XattrOp &xattr_op);
  void mirror_info_removexattr_handler(CInode *cur, InodeStoreBase::xattr_map_ptr xattrs,
                                       const XattrOp &xattr_op);

  static bool is_ceph_vxattr(std::string_view xattr_name) {
    return xattr_name.rfind("ceph.dir.layout", 0) == 0 ||
           xattr_name.rfind("ceph.file.layout", 0) == 0 ||
           xattr_name.rfind("ceph.quota", 0) == 0 ||
           xattr_name == "ceph.quiesce.block"sv ||
           xattr_name == "ceph.dir.subvolume" ||
           xattr_name == "ceph.dir.pin" ||
           xattr_name == "ceph.dir.pin.random" ||
           xattr_name == "ceph.dir.pin.distributed";
  }

  static bool is_ceph_dir_vxattr(std::string_view xattr_name) {
    return (xattr_name == "ceph.dir.layout" ||
	    xattr_name == "ceph.dir.layout.json" ||
	    xattr_name == "ceph.dir.layout.object_size" ||
	    xattr_name == "ceph.dir.layout.stripe_unit" ||
	    xattr_name == "ceph.dir.layout.stripe_count" ||
	    xattr_name == "ceph.dir.layout.pool" ||
	    xattr_name == "ceph.dir.layout.pool_name" ||
	    xattr_name == "ceph.dir.layout.pool_id" ||
	    xattr_name == "ceph.dir.layout.pool_namespace" ||
	    xattr_name == "ceph.dir.pin" ||
	    xattr_name == "ceph.dir.pin.random" ||
	    xattr_name == "ceph.dir.pin.distributed" ||
	    xattr_name == "ceph.dir.subvolume");
  }

  static bool is_ceph_file_vxattr(std::string_view xattr_name) {
    return (xattr_name == "ceph.file.layout" ||
	    xattr_name == "ceph.file.layout.json" ||
	    xattr_name == "ceph.file.layout.object_size" ||
	    xattr_name == "ceph.file.layout.stripe_unit" ||
	    xattr_name == "ceph.file.layout.stripe_count" ||
	    xattr_name == "ceph.file.layout.pool" ||
	    xattr_name == "ceph.file.layout.pool_name" ||
	    xattr_name == "ceph.file.layout.pool_id" ||
	    xattr_name == "ceph.file.layout.pool_namespace");
  }

  static bool is_allowed_ceph_xattr(std::string_view xattr_name) {
    // not a ceph xattr -- allow!
    if (xattr_name.rfind("ceph.", 0) != 0) {
      return true;
    }

    return xattr_name == "ceph.mirror.info" ||
           xattr_name == "ceph.mirror.dirty_snap_id";
  }

  void reply_client_request(const MDRequestRef& mdr, const ref_t<MClientReply> &reply);
  void flush_session(Session *session, MDSGatherBuilder& gather);

  void _finalize_readdir(const MDRequestRef& mdr,
                         CInode *diri,
                         CDir* dir,
                         bool start,
                         bool end,
                         __u16 flags,
                         __u32 numfiles,
                         bufferlist& dirbl,
                         bufferlist& dnbl);
  void _readdir_diff(
    utime_t now,
    const MDRequestRef& mdr,
    CInode* diri,
    CDir* dir,
    SnapRealm* realm,
    unsigned max_entries,
    int bytes_left,
    const std::string& offset_str,
    uint32_t offset_hash,
    unsigned req_flags,
    bufferlist& dirbl);
  bool build_snap_diff(
    const MDRequestRef& mdr,
    CDir* dir,
    int bytes_left,
    dentry_key_t* skip_key,
    snapid_t snapid_before,
    snapid_t snapid,
    const bufferlist& dnbl,
    std::function<bool(CDentry*, CInode*, bool)> add_result_cb);

  MDSRank *mds;
  MDCache *mdcache;
  MDLog *mdlog;
  PerfCounters *logger = nullptr;

  // OSDMap full status, used to generate CEPHFS_ENOSPC on some operations
  bool is_full = false;

  // State for while in reconnect
  MDSContext *reconnect_done = nullptr;
  int failed_reconnects = 0;
  bool reconnect_evicting = false;  // true if I am waiting for evictions to complete
                            // before proceeding to reconnect_gather_finish
  time reconnect_start = clock::zero();
  time reconnect_last_seen = clock::zero();
  std::set<client_t> client_reconnect_gather;  // clients i need a reconnect msg from.
  std::set<client_t> client_reconnect_denied;  // clients whose reconnect msg have been denied .

  feature_bitset_t supported_features;
  feature_bitset_t supported_metric_spec;
  feature_bitset_t required_client_features;

  bool forward_all_requests_to_auth = false;
  bool replay_unsafe_with_closed_session = false;
  double cap_revoke_eviction_timeout = 0;
  uint64_t max_snaps_per_dir = 100;
  // long snapshot names have the following format: "_<SNAPSHOT-NAME>_<INODE-NUMBER>"
  uint64_t snapshot_name_max = NAME_MAX - 1 - 1 - 13;
  unsigned delegate_inos_pct = 0;
  uint64_t dir_max_entries = 0;
  int64_t bal_fragment_size_max = 0;

  double inject_rename_corrupt_dentry_first = 0.0;

  DecayCounter recall_throttle;
  time last_recall_state;

  MetricsHandler *metrics_handler;

  // Cache cap acquisition throttle configs
  uint64_t max_caps_per_client;
  uint64_t cap_acquisition_throttle;
  double max_caps_throttle_ratio;
  double caps_throttle_retry_request_timeout;

  size_t alternate_name_max = g_conf().get_val<Option::size_t>("mds_alternate_name_max");
  size_t fscrypt_last_block_max_size = g_conf().get_val<Option::size_t>("mds_fscrypt_last_block_max_size");

  // record laggy clients due to laggy OSDs
  std::set<client_t> laggy_clients;
};

static inline constexpr auto operator|(Server::RecallFlags a, Server::RecallFlags b) {
  using T = std::underlying_type<Server::RecallFlags>::type;
  return static_cast<Server::RecallFlags>(static_cast<T>(a) | static_cast<T>(b));
}
static inline constexpr auto operator&(Server::RecallFlags a, Server::RecallFlags b) {
  using T = std::underlying_type<Server::RecallFlags>::type;
  return static_cast<Server::RecallFlags>(static_cast<T>(a) & static_cast<T>(b));
}
static inline std::ostream& operator<<(std::ostream& os, const Server::RecallFlags& f) {
  using T = std::underlying_type<Server::RecallFlags>::type;
  return os << "0x" << std::hex << static_cast<T>(f) << std::dec;
}
static inline constexpr bool operator!(const Server::RecallFlags& f) {
  using T = std::underlying_type<Server::RecallFlags>::type;
  return static_cast<T>(f) == static_cast<T>(0);
}
#endif
