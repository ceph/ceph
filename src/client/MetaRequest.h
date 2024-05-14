// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_METAREQUEST_H
#define CEPH_CLIENT_METAREQUEST_H


#include "include/types.h"
#include "include/xlist.h"
#include "include/filepath.h"
#include "mds/mdstypes.h"
#include "InodeRef.h"
#include "UserPerm.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

class Dentry;
class dir_result_t;

struct MetaRequest {
private:
  InodeRef _inode, _old_inode, _other_inode;
  Dentry *_dentry = NULL;     //associated with path
  Dentry *_old_dentry = NULL; //associated with path2
  int abort_rc = 0;
public:
  ceph::coarse_mono_time created = ceph::coarse_mono_clock::zero();
  uint64_t tid = 0;
  utime_t  op_stamp;
  ceph_mds_request_head head;
  filepath path, path2;
  std::string alternate_name;
  std::vector<uint8_t>	fscrypt_auth;
  std::vector<uint8_t>	fscrypt_file;
  bufferlist data;
  int inode_drop = 0;   //the inode caps this operation will drop
  int inode_unless = 0; //unless we have these caps already
  int old_inode_drop = 0, old_inode_unless = 0;
  int dentry_drop = 0, dentry_unless = 0;
  int old_dentry_drop = 0, old_dentry_unless = 0;
  int other_inode_drop = 0, other_inode_unless = 0;
  std::vector<MClientRequest::Release> cap_releases;

  int regetattr_mask = 0;          // getattr mask if i need to re-stat after a traceless reply
 
  utime_t  sent_stamp;
  mds_rank_t mds = -1;             // who i am asking
  mds_rank_t resend_mds = -1;      // someone wants you to (re)send the request here
  bool     send_to_auth = false;   // must send to auth mds
  __u32    sent_on_mseq = 0;       // mseq at last submission of this request
  int      num_fwd = 0;            // # of times i've been forwarded
  int      retry_attempt = 0;
  std::atomic<uint64_t> ref = { 1 };
  
  ceph::cref_t<MClientReply> reply = NULL;  // the reply
  bool kick = false;
  bool success = false;

  // readdir result
  dir_result_t *dirp = NULL;

  //possible responses
  bool got_unsafe = false;

  xlist<MetaRequest*>::item item;
  xlist<MetaRequest*>::item unsafe_item;
  xlist<MetaRequest*>::item unsafe_dir_item;
  xlist<MetaRequest*>::item unsafe_target_item;

  ceph::condition_variable *caller_cond = NULL;   // who to take up
  ceph::condition_variable *dispatch_cond = NULL; // who to kick back
  std::list<Context*> waitfor_safe;

  InodeRef target;
  UserPerm perms;

  explicit MetaRequest(int op) :
    item(this), unsafe_item(this), unsafe_dir_item(this),
    unsafe_target_item(this) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.owner_uid = -1;
    head.owner_gid = -1;
  }
  ~MetaRequest();

  /**
   * Prematurely terminate the request, such that callers
   * to make_request will receive `rc` as their result.
   */
  void abort(int rc)
  {
    ceph_assert(rc != 0);
    abort_rc = rc;
  }

  /**
   * Whether abort() has been called for this request
   */
  inline bool aborted() const
  {
    return abort_rc != 0;
  }

  /**
   * Given that abort() has been called for this request, what `rc` was
   * passed into it?
   */
  int get_abort_code() const
  {
    return abort_rc;
  }

  void set_inode(Inode *in) {
    _inode = in;
  }
  Inode *inode() {
    return _inode.get();
  }
  void take_inode(InodeRef *out) {
    out->swap(_inode);
  }
  void set_old_inode(Inode *in) {
    _old_inode = in;
  }
  Inode *old_inode() {
    return _old_inode.get();
  }
  void take_old_inode(InodeRef *out) {
    out->swap(_old_inode);
  }
  void set_other_inode(Inode *in) {
    _other_inode = in;
  }
  Inode *other_inode() {
    return _other_inode.get();
  }
  void take_other_inode(InodeRef *out) {
    out->swap(_other_inode);
  }
  void set_dentry(Dentry *d);
  Dentry *dentry();
  void set_old_dentry(Dentry *d);
  Dentry *old_dentry();

  MetaRequest* get() {
    ref++;
    return this;
  }

  /// psuedo-private put method; use Client::put_request()
  bool _put() {
    int v = --ref;
    return v == 0;
  }

  void set_inode_owner_uid_gid(unsigned u, unsigned g) {
    /* it makes sense to set owner_{u,g}id only for OPs which create inodes */
    ceph_assert(IS_CEPH_MDS_OP_NEWINODE(head.op));
    head.owner_uid = u;
    head.owner_gid = g;
  }

  // normal fields
  void set_tid(ceph_tid_t t) { tid = t; }
  void set_oldest_client_tid(ceph_tid_t t) { head.oldest_client_tid = t; }
  void inc_num_fwd() { head.ext_num_fwd = head.ext_num_fwd + 1; }
  void set_retry_attempt(int a) { head.ext_num_retry = a; }
  void set_filepath(const filepath& fp) { path = fp; }
  void set_filepath2(const filepath& fp) { path2 = fp; }
  void set_alternate_name(std::string an) { alternate_name = an; }
  void set_string2(const char *s) { path2.set_path(std::string_view(s), 0); }
  void set_caller_perms(const UserPerm& _perms) {
    perms = _perms;
    head.caller_uid = perms.uid();
    head.caller_gid = perms.gid();
  }
  uid_t get_uid() { return perms.uid(); }
  uid_t get_gid() { return perms.gid(); }
  void set_data(const bufferlist &d) { data = d; }
  void set_dentry_wanted() {
    head.flags = head.flags | CEPH_MDS_FLAG_WANT_DENTRY;
  }
  int get_op() { return head.op; }
  ceph_tid_t get_tid() { return tid; }
  filepath& get_filepath() { return path; }
  filepath& get_filepath2() { return path2; }

  bool is_write() {
    return
      (head.op & CEPH_MDS_OP_WRITE) || 
      (head.op == CEPH_MDS_OP_OPEN && (head.args.open.flags & (O_CREAT|O_TRUNC)));
  }
  bool can_forward() {
    if ((head.op & CEPH_MDS_OP_WRITE) ||
	head.op == CEPH_MDS_OP_OPEN)   // do not forward _any_ open request.
      return false;
    return true;
  }
  bool auth_is_best(int issued) {
    if (send_to_auth)
      return true;

    /* Any write op ? */
    if (head.op & CEPH_MDS_OP_WRITE)
      return true;

    switch (head.op) {
    case CEPH_MDS_OP_OPEN:
    case CEPH_MDS_OP_READDIR:
      return true;
    case CEPH_MDS_OP_GETATTR:
      /*
       * If any 'x' caps is issued we can just choose the auth MDS
       * instead of the random replica MDSes. Because only when the
       * Locker is in LOCK_EXEC state will the loner client could
       * get the 'x' caps. And if we send the getattr requests to
       * any replica MDS it must auth pin and tries to rdlock from
       * the auth MDS, and then the auth MDS need to do the Locker
       * state transition to LOCK_SYNC. And after that the lock state
       * will change back.
       *
       * This cost much when doing the Locker state transition and
       * usually will need to revoke caps from clients.
       *
       * And for the 'Xs' caps for getxattr we will also choose the
       * auth MDS, because the MDS side code is buggy due to setxattr
       * won't notify the replica MDSes when the values changed and
       * the replica MDS will return the old values. Though we will
       * fix it in MDS code, but this still makes sense for old ceph.
       */
      if (((head.args.getattr.mask & CEPH_CAP_ANY_SHARED) &&
	   (issued & CEPH_CAP_ANY_EXCL)) ||
          (head.args.getattr.mask & (CEPH_STAT_RSTAT | CEPH_STAT_CAP_XATTR)))
        return true;
    default:
      return false;
    }
  }

  void dump(Formatter *f) const;

};

#endif
