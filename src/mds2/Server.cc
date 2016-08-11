#include "MDSRank.h"
#include "Server.h"
#include "MDCache.h"
#include "Locker.h"

#include "messages/MClientSession.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientReconnect.h"
#include "common/Finisher.h"

#define dout_subsys ceph_subsys_mds

Server::Server(MDSRank *_mds)
  : mds(_mds), mdcache(_mds->mdcache), locker(_mds->locker)
{
}

void Server::dispatch(Message *m)
{
  switch (m->get_type()) {
    case CEPH_MSG_CLIENT_RECONNECT:
      handle_client_reconnect(static_cast<MClientReconnect*>(m));
      return;
    case CEPH_MSG_CLIENT_SESSION:
      handle_client_session(static_cast<MClientSession*>(m));
      return;
    case CEPH_MSG_CLIENT_REQUEST:
      handle_client_request(static_cast<MClientRequest*>(m));
      return;
    default:
      derr << "server unknown message " << m->get_type() << dendl;
      assert(0 == "server unknown message");
  }
}

Session *Server::get_session(Message *m)
{
  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
  if (session) {
    dout(20) << "get_session have " << session << " " << session->info.inst
	     << " state " << session->get_state_name() << dendl;
    session->put();  // not carry ref
  } else {
    dout(20) << "get_session dne for " << m->get_source_inst() << dendl;
  }
  return session;
}

/* This function DOES put the passed message before returning*/
void Server::handle_client_reconnect(MClientReconnect *m)
{
  dout(7) << "handle_client_reconnect " << m->get_source() << dendl;
  Session *session = get_session(m);
  assert(session);

  m->get_connection()->send_message(new MClientSession(CEPH_SESSION_CLOSE));
  m->put();
  return;
}

void Server::handle_client_session(MClientSession *m)
{
  Session *session = get_session(m);
  dout(3) << "handle_client_session " << *m << " from " << m->get_source() << dendl;

  switch (m->get_op()) {
    case CEPH_SESSION_REQUEST_OPEN:
      session->set_state(Session::STATE_OPEN);
      session->connection->send_message(new MClientSession(CEPH_SESSION_OPEN));
      break;
    case CEPH_SESSION_REQUEST_CLOSE:
      session->set_state(Session::STATE_CLOSED);
      session->clear();
      session->connection->send_message(new MClientSession(CEPH_SESSION_CLOSE));
      break;
    case CEPH_SESSION_REQUEST_RENEWCAPS:
      break;
    case CEPH_SESSION_FLUSHMSG_ACK:
      break;
    default:
      derr << "server unknown session op " << m->get_op() << dendl;
      assert(0 == "server unknown session op");
  }
  m->put();
}

void Server::handle_client_request(MClientRequest *req)
{
  dout(4) << "handle_client_request " << *req << dendl;

  MDRequestRef mdr = mdcache->request_start(req);
  if (!mdr)
    return;

  mdr->session = get_session(req);

  mds->op_wq.queue(mdr);
  //dispatch_client_request(mdr);
  return;
}

void Server::dispatch_client_request(MDRequestRef& mdr)
{
  MClientRequest *req = mdr->client_request;
  dout(7) << "dispatch_client_request " << *req << dendl;

  switch (req->get_op()) {
    case CEPH_MDS_OP_LOOKUP:
      handle_client_getattr(mdr, true);
      break;
    case CEPH_MDS_OP_GETATTR:
      handle_client_getattr(mdr, false);
      break;
    case CEPH_MDS_OP_SETATTR:
      handle_client_setattr(mdr);
      break;
    case CEPH_MDS_OP_MKNOD:
      handle_client_mknod(mdr);
      break;
    case CEPH_MDS_OP_SYMLINK:
      handle_client_symlink(mdr);
      break;
    case CEPH_MDS_OP_MKDIR:
      handle_client_mkdir(mdr);
      break;
    case CEPH_MDS_OP_READDIR:
      handle_client_readdir(mdr);
      break;
    case CEPH_MDS_OP_LINK:
      handle_client_link(mdr);
      break;
    case CEPH_MDS_OP_UNLINK:
    case CEPH_MDS_OP_RMDIR:
      handle_client_unlink(mdr);
      break;
    case CEPH_MDS_OP_RENAME:
      handle_client_rename(mdr);
      break;
    default:
      dout(1) << " unknown client op " << req->get_op() << dendl;
      respond_to_request(mdr, -EOPNOTSUPP);
  }
}

void Server::encode_empty_dirstat(bufferlist& bl)
{
  static DirStat empty;
  empty.encode(bl);
}

void Server::encode_null_lease(bufferlist& bl)
{
  LeaseStat e;
  e.seq = 0;
  e.mask = 0;
  e.duration_ms = 0;
  ::encode(e, bl);
  dout(20) << "encode_null_lease " << e << dendl;
}

void Server::set_trace_dist(Session *session, MClientReply *reply,
			    CInode *in, CDentry *dn, MDRequestRef& mdr)
{
  bufferlist bl;
  // dir + dentry?
  if (dn) {
    reply->head.is_dentry = 1;
    CDir *dir = dn->get_dir();
    CInode *diri = dir->get_inode();

    diri->encode_inodestat(bl, session, 0);
//    dout(20) << "set_trace_dist added diri " << *diri << dendl;

    encode_empty_dirstat(bl);
//    dout(20) << "set_trace_dist added dir  " << *dir << dendl;

    ::encode(dn->get_name(), bl);
     encode_null_lease(bl);
//    dout(20) << "set_trace_dist added dn   " << snapid << " " << *dn << dendl;
  } else
    reply->head.is_dentry = 0;

  if (in) {
    in->encode_inodestat(bl, session, 0);
//    dout(20) << "set_trace_dist added in  " << *in << dendl;
    reply->head.is_target = 1;
  } else
    reply->head.is_target = 0;

  reply->set_trace(bl);
}

void Server::respond_to_request(MDRequestRef& mdr, int r)
{
  if (mdr->client_request) {
    reply_client_request(mdr, new MClientReply(mdr->client_request, r));
  } else {
    assert(0); 
  }

  mdr->apply();
  mdcache->request_finish(mdr);
}

void Server::early_reply(MDRequestRef& mdr)
{
  if (!g_conf->mds_early_reply)
    return;

  MClientRequest *req = mdr->client_request;
  Session *session = get_session(req); 

  locker->set_xlocks_done(mdr.get(), req->get_op() == CEPH_MDS_OP_RENAME);

  MClientReply *reply = new MClientReply(req, 0);
  reply->set_unsafe();

  if (mdr->tracei >= 0 || mdr->tracedn >= 0) {
    CInode *in = NULL;
    if (mdr->tracei >= 0) {
      in = mdr->in[mdr->tracei].get();
      assert(mdr->is_object_locked(in));
    }
    CDentry *dn = NULL;
    if (mdr->tracedn >= 0) {
      dn = mdr->dn[mdr->tracedn].back().get();
      assert(mdr->is_object_locked(dn->get_dir_inode()));
    }

    set_trace_dist(session, reply, in, dn, mdr);
  }

  reply->set_extra_bl(mdr->reply_extra_bl);
  req->get_connection()->send_message(reply);

  mdr->did_early_reply = true;
}

void Server::reply_client_request(MDRequestRef& mdr, MClientReply *reply)
{
  MClientRequest *req = mdr->client_request;
  Session *session = get_session(req); 

  if (!mdr->did_early_reply) {

    if (mdr->tracei >= 0 || mdr->tracedn >= 0) {
      set<CObject*> objs;

      CInode *in = NULL;
      if (mdr->tracei >= 0) {
	in = mdr->in[mdr->tracei].get();
	assert(mdr->is_object_locked(in));
	objs.insert(in);
      }
      CDentry *dn = NULL;
      if (mdr->tracedn >= 0) {
	dn = mdr->dn[mdr->tracedn].back().get();
	assert(mdr->is_object_locked(dn->get_dir_inode()));
	objs.insert(dn);
	objs.insert(dn->get_dir_inode());
      }

      // drop non-rdlocks before replying, so that we can issue leases
      locker->drop_non_rdlocks(mdr.get(), objs);

      set_trace_dist(session, reply, in, dn, mdr);
    }

    reply->set_extra_bl(mdr->reply_extra_bl);
  }

  reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());
  req->get_connection()->send_message(reply);
}

int Server::rdlock_path_pin_ref(MDRequestRef& mdr, int n,
				set<SimpleLock*> &rdlocks,
				bool is_lookup)
{
  const filepath& refpath = n ? mdr->get_filepath2() : mdr->get_filepath();
  dout(10) << "rdlock_path_pin_ref " << *mdr << " " << refpath << dendl;

  int r = mdcache->path_traverse(mdr, refpath, &mdr->dn[n], &mdr->in[n]);
  if (r < 0) {
    /*
     * FIXME: 
    if (r == -ENOENT && n == 0 && mdr->dn[n].size()) {
      if (is_lookup)
	mdr->tracedn = n;
    }
    */
    respond_to_request(mdr, r);
    return r;
  }

  for (auto& p : mdr->dn[n])
    rdlocks.insert(&p->lock);

  return 0;
}

int Server::rdlock_path_xlock_dentry(MDRequestRef& mdr, int n,
				     set<SimpleLock*>& rdlocks,
				     set<SimpleLock*>& wrlocks,
				     set<SimpleLock*>& xlocks,
				     bool okexist, bool mustexist)
{
  const filepath& refpath = n ? mdr->get_filepath2() : mdr->get_filepath();
  dout(10) << "rdlock_path_xlock_dentry " << *mdr << " " << refpath << dendl;

  int err;
  if (refpath.depth() == 0) {
    err = -EINVAL;
    respond_to_request(mdr, err);
    return err;
  }

  filepath dirpath = refpath;
  string dname = dirpath.last_dentry();
  dirpath.pop_dentry();

  CInodeRef diri;
  err = mdcache->path_traverse(mdr, dirpath, &mdr->dn[n], &diri);
  if (err < 0) {
    respond_to_request(mdr, err);
    return err;
  }

  if (!diri->is_dir()) {
    err = -ENOTDIR;
    respond_to_request(mdr, err);
    return err; 
  }

  mdr->lock_object(diri.get());

  frag_t fg = diri->pick_dirfrag(dname);
  CDirRef dir = diri->get_or_open_dirfrag(fg);
  assert(dir);

  CDentryRef dn = dir->lookup(dname);
  const CDentry::linkage_t* dnl = dn ? dn->get_projected_linkage() : NULL;
  CInodeRef in;
  if (mustexist) {
    if (!dn || dnl->is_null()) {
      err = -ENOENT;
      respond_to_request(mdr, err);
      return err;
    }
  } else {
    if (dn) {
      if (!okexist && !dnl->is_null()) {
	err = -EEXIST;
	respond_to_request(mdr, err);
	return err;
      }
    } else {
      dn = dir->add_null_dentry(dname);
    }
  }
  if (dnl && !dnl->is_null()) {
    in = dnl->get_inode();
    if (dnl->is_remote() && !in) {
      in = mdcache->get_inode(dnl->get_remote_ino());
    }
    assert(in);
  }

  for (auto& p : mdr->dn[n])
    rdlocks.insert(&p->lock);

  wrlocks.insert(&diri->filelock); // also, wrlock on dir mtime
  wrlocks.insert(&diri->nestlock); // also, wrlock on dir mtime

  xlocks.insert(&dn->lock);

  mdr->unlock_object(diri.get());

  mdr->dn[n].push_back(dn);
  mdr->in[n] = in;
  return 0;
}

void Server::journal_and_reply(MDRequestRef& mdr, int tracei, int tracedn,
			       LogEvent *le, Context *fin)
{
  mdr->tracei = tracei;
  mdr->tracedn = tracedn;

  early_reply(mdr);

  // start journal
  mdcache->start_log_entry();
  // submit log

  // use finisher to simulate log flush
  mds->finisher->queue(fin);
 
  mdr->unlock_all_objects();

  mdcache->submit_log_entry();

  if (mdr->did_early_reply)
    locker->drop_rdlocks(mdr.get());

  mdr->start_committing();
}

void Server::handle_client_getattr(MDRequestRef& mdr, bool is_lookup)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_pin_ref(mdr, 0, rdlocks, is_lookup);
  if (r < 0)
    return;

  /*
  int mask = req->head.args.getattr.mask;
  if ((mask & CEPH_CAP_LINK_SHARED) && (issued & CEPH_CAP_LINK_EXCL) == 0) rdlocks.insert(&ref->linklock);
  if ((mask & CEPH_CAP_AUTH_SHARED) && (issued & CEPH_CAP_AUTH_EXCL) == 0) rdlocks.insert(&ref->authlock);
  if ((mask & CEPH_CAP_FILE_SHARED) && (issued & CEPH_CAP_FILE_EXCL) == 0) rdlocks.insert(&ref->filelock);
  if ((mask & CEPH_CAP_XATTR_SHARED) && (issued & CEPH_CAP_XATTR_EXCL) == 0) rdlocks.insert(&ref->xattrlock);
  */

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  if (is_lookup) {
    CDentryRef &dn = mdr->dn[0].back();
    mdr->lock_object(dn->get_dir_inode());
    mdr->tracedn = 0;
  }

  CInodeRef& in = mdr->in[0];
  mdr->lock_object(in.get());

  mdr->tracei = 0;
  respond_to_request(mdr, 0);
}


void Server::__inode_update_finish(MDRequestRef& mdr)
{
  mdr->wait_committing();

  CInodeRef& in = mdr->in[0]; 
  mdcache->lock_objects_for_update(mdr.get(), in.get(), true);
  mdr->early_apply();
  respond_to_request(mdr, 0);
}

class C_MDS_inode_update_finish : public Context {
  MDSRank *mds;
  MDRequestRef mdr;
public:
  C_MDS_inode_update_finish(MDSRank *m, MDRequestRef& r) : mds(m), mdr(r) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->__inode_update_finish(mdr);
  }
};

void Server::handle_client_setattr(MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_pin_ref(mdr, 0, rdlocks, false);
  if (r < 0)
    return;

  CInodeRef& in = mdr->in[0]; 

  MClientRequest *req = mdr->client_request;
  __u32 mask = req->head.args.setattr.mask;

  // xlock inode
  if (mask & (CEPH_SETATTR_MODE|CEPH_SETATTR_UID|CEPH_SETATTR_GID))
    xlocks.insert(&in->authlock);
  if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME|CEPH_SETATTR_SIZE))
    xlocks.insert(&in->filelock);
  if (mask & CEPH_SETATTR_CTIME)
    wrlocks.insert(&in->versionlock);

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  mdcache->lock_objects_for_update(mdr.get(), in.get(), false);

  mdr->add_projected_inode(in.get(), true);
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  pi->ctime = mdr->get_op_stamp();

  if (mask & CEPH_SETATTR_MODE)
    pi->mode = (pi->mode & ~07777) | (req->head.args.setattr.mode & 07777);
  if (mask & CEPH_SETATTR_UID)
    pi->uid = req->head.args.setattr.uid;
  if (mask & CEPH_SETATTR_GID)
    pi->gid = req->head.args.setattr.gid;

  if (mask & CEPH_SETATTR_MTIME)
    pi->mtime = req->head.args.setattr.mtime;
  if (mask & CEPH_SETATTR_ATIME)
    pi->atime = req->head.args.setattr.atime;
  if (mask & (CEPH_SETATTR_ATIME | CEPH_SETATTR_MTIME))
    pi->time_warp_seq++;   // maybe not a timewarp, but still a serialization point.
  if (mask & CEPH_SETATTR_SIZE) {
    // FIXME
  }

  mdcache->predirty_journal_parents(mdr.get(), NULL, in.get(), NULL, PREDIRTY_PRIMARY);
  // journal inode;

  CDentryRef null_dn; 
  journal_and_reply(mdr, 0, -1, NULL, new C_MDS_inode_update_finish(mds, mdr));
}

CInodeRef Server::prepare_new_inode(MDRequestRef& mdr, CDentryRef& dn, inodeno_t useino, unsigned mode,
				    file_layout_t *layout)
{
  CInodeRef in = new CInode(mdcache);

  inode_t *pi = in->__get_inode();
  pi->ino = mdcache->alloc_ino();

  pi->version = 1;
  pi->xattr_version = 1;
  pi->nlink = 1;
  pi->mode = mode;

  memset(&pi->dir_layout, 0, sizeof(pi->dir_layout));
  if (pi->is_dir()) {
    pi->dir_layout.dl_dir_hash = g_conf->mds_default_dir_hash;
  } else if (layout) {
    pi->layout = *layout;
  } else {
    pi->layout = mdcache->get_default_file_layout();
  }

  pi->truncate_size = -1ull;  // not truncated, yet!
  pi->truncate_seq = 1; /* starting with 1, 0 is kept for no-truncation logic */

  CInode *diri = dn->get_dir_inode();

  dout(10) << oct << " dir mode 0" << diri->get_inode()->mode << " new mode 0" << mode << dec << dendl;

  MClientRequest *req = mdr->client_request;
  pi->uid = req->get_caller_uid();

  if (diri->get_inode()->mode & S_ISGID) {
    dout(10) << " dir is sticky" << dendl;
    pi->gid = diri->get_inode()->gid;
    if (S_ISDIR(mode)) {
      dout(10) << " new dir also sticky" << dendl;
      pi->mode |= S_ISGID;
    }
  } else {
    pi->gid = req->get_caller_gid();
  }

  pi->ctime = pi->mtime = pi->atime = mdr->get_op_stamp();

  if (req->get_data().length()) {
    bufferlist::iterator p = req->get_data().begin();
    // xattrs on new inode?
    map<string,bufferptr> xattrs;
    try {
      ::decode(xattrs, p);
    } catch (buffer::error& e) {
    }
    for (map<string,bufferptr>::iterator p = xattrs.begin(); p != xattrs.end(); ++p) {
      dout(10) << "prepare_new_inode setting xattr " << p->first << dendl;
    }
    in->__get_xattrs()->swap(xattrs);
  }

  in->mutex_lock();
  mdcache->add_inode(in.get());  // add
  return in;
}

void Server::__mknod_finish(MDRequestRef& mdr)
{
  mdr->wait_committing();

  CInodeRef& in = mdr->in[0];
  CDentryRef& dn = mdr->dn[0].back();
  CInode *diri = dn->get_dir_inode();

  mdr->lock_object(diri);
  mdr->lock_object(in.get());

  dn->pop_projected_linkage();

  // be a bit hacky with the inode version, here.. we decrement it
  // just to keep mark_dirty() happen. (we didn't bother projecting
  // a new version of hte inode since it's just been created)
  in->__get_inode()->version--;
  in->mark_dirty(in->get_inode()->version + 1, mdr->ls);

  if (in->is_dir()) {
    CDirRef dir = in->get_dirfrag(frag_t());
    dir->__get_fnode()->version--;
    dir->mark_dirty(dir->get_fnode()->version + 1, mdr->ls);
  }

  // apply
  mdr->early_apply();
  respond_to_request(mdr, 0);
}

class C_MDS_mknod_finish : public Context {
  MDSRank *mds;
  MDRequestRef mdr;
public:
  C_MDS_mknod_finish(MDSRank *m, MDRequestRef& r) : mds(m), mdr(r) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->__mknod_finish(mdr);
  }
};

void Server::handle_client_mknod(MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false);
  if (r < 0)
    return;

  CDentryRef& dn = mdr->dn[0].back();
  CInode *diri = dn->get_dir_inode();

  rdlocks.insert(&diri->authlock);
  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  mdr->lock_object(diri);
  assert(dn->get_projected_linkage()->is_null());

  MClientRequest *req = mdr->client_request;
  unsigned mode = req->head.args.mknod.mode;
  if ((mode & S_IFMT) == 0)
    mode |= S_IFREG;

  CInodeRef newi = prepare_new_inode(mdr, dn, inodeno_t(req->head.ino), mode);
  assert(newi);
  mdr->add_locked_object(newi.get());

  inode_t *pi = newi->__get_inode();
  pi->version = dn->pre_dirty();
  pi->rdev = req->head.args.mknod.rdev;
  pi->rstat.rfiles = 1;

  dn->push_projected_linkage(newi.get());

  mdcache->predirty_journal_parents(mdr.get(), NULL, newi.get(), dn->get_dir(), PREDIRTY_PRIMARY, 1);

  mdr->in[0] = newi;
  journal_and_reply(mdr, 0, 0, NULL, new C_MDS_mknod_finish(mds, mdr));
};

void Server::handle_client_symlink(MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false);
  if (r < 0)
    return;

  CDentryRef& dn = mdr->dn[0].back();
  CInode *diri = dn->get_dir_inode();

  rdlocks.insert(&diri->authlock);
  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  mdr->lock_object(diri);
  assert(dn->get_projected_linkage()->is_null());

  MClientRequest *req = mdr->client_request;
  unsigned mode = S_IFLNK | 0777; 

  CInodeRef newi = prepare_new_inode(mdr, dn, inodeno_t(req->head.ino), mode);
  assert(newi);
  mdr->add_locked_object(newi.get());

  inode_t *pi = newi->__get_inode();
  pi->version = dn->pre_dirty();

  newi->__set_symlink(req->get_path2());
  pi->size = newi->get_symlink().length();
  pi->rstat.rbytes = pi->size;
  pi->rstat.rfiles = 1;

  dn->push_projected_linkage(newi.get());

  mdcache->predirty_journal_parents(mdr.get(), NULL, newi.get(), dn->get_dir(), PREDIRTY_PRIMARY, 1);

  mdr->in[0] = newi;
  journal_and_reply(mdr, 0, 0, NULL, new C_MDS_mknod_finish(mds, mdr));
};

void Server::handle_client_mkdir(MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false);
  if (r < 0)
    return;

  CDentryRef& dn = mdr->dn[0].back();
  CInode *diri = dn->get_dir_inode();

  rdlocks.insert(&diri->authlock);
  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  mdr->lock_object(diri);
  assert(dn->get_projected_linkage()->is_null());

  MClientRequest *req = mdr->client_request;
  unsigned mode = req->head.args.mkdir.mode;
  mode &= ~S_IFMT;
  mode |= S_IFDIR;

  CInodeRef newi = prepare_new_inode(mdr, dn, inodeno_t(req->head.ino), mode);
  assert(newi);
  mdr->add_locked_object(newi.get());

  dn->push_projected_linkage(newi.get());

  inode_t *pi = newi->__get_inode();
  pi->version = dn->pre_dirty();
  pi->rstat.rsubdirs = 1;

  CDirRef newdir = newi->get_or_open_dirfrag(frag_t());
  newdir->__get_fnode()->version = newdir->pre_dirty();

  mdcache->predirty_journal_parents(mdr.get(), NULL, newi.get(), dn->get_dir(), PREDIRTY_PRIMARY, 1);

  mdr->in[0] = newi;
  journal_and_reply(mdr, 0, 0, NULL, new C_MDS_mknod_finish(mds, mdr));
}

void Server::handle_client_readdir(MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_pin_ref(mdr, 0, rdlocks, false);
  if (r < 0)
    return;

  CInodeRef& diri = mdr->in[0];
  if (!diri->is_dir()) {
    respond_to_request(mdr, -ENOTDIR);
    return;
  }

  rdlocks.insert(&diri->filelock);
  rdlocks.insert(&diri->dirfragtreelock);

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  MClientRequest *req = mdr->client_request;
  string offset_str = req->get_path2();

  unsigned max = req->head.args.readdir.max_entries;
  if (!max)
    max = (unsigned)-1;
  unsigned max_bytes = req->head.args.readdir.max_bytes;
  if (!max_bytes)
    max_bytes = 512 << 10;  // 512 KB?


  mdr->lock_object(diri.get());

  CDirRef dir = diri->get_or_open_dirfrag(frag_t());

  bufferlist dirbl;
  encode_empty_dirstat(dirbl);

  int front_bytes = dirbl.length() + sizeof(__u32) + sizeof(__u8)*2;
  int bytes_left = max_bytes - front_bytes;

  // build dir contents
  bufferlist dnbl;
  __u32 num_entries = 0;
  auto it = dir->begin();
  while (num_entries < max_bytes && it != dir->end()) {
    CDentry *dn = it->second;
    ++it;

    const CDentry::linkage_t *dnl = dn->get_projected_linkage();
    if (dnl->is_null())
      continue;

    if (!offset_str.empty()) {
      dentry_key_t offset_key(dn->get_last(), offset_str.c_str());
      if (!(offset_key < dn->get_key()))
        continue;
    }

    CInodeRef in = dnl->get_inode();

    // remote link?
    if (dnl->is_remote() && !in) {
      in = mdcache->get_inode(dnl->get_remote_ino());
    }

    assert(in);

    if ((int)(dnbl.length() + dn->get_name().length() + sizeof(__u32) + sizeof(LeaseStat)) > bytes_left) {
      break;
    }

    unsigned start_len = dnbl.length();

    // dentry
    ::encode(dn->get_name(), dnbl);
    encode_null_lease(dnbl);

    in->mutex_lock();
    int r = in->encode_inodestat(dnbl, mdr->session, bytes_left - (int)dnbl.length());
    in->mutex_unlock();
    /*
     * FIXME:
     * After unlock the inode, Other thread may kick in, change inode's locks states and
     * send cap message before us. Need a mechanism to prevent this.
     */
    if (r < 0) {
      // chop off dn->name, lease
      bufferlist keep;
      keep.substr_of(dnbl, 0, start_len);
      dnbl.swap(keep);
      break;
    }

    assert(r >= 0);
    num_entries++;
  }

  bool complete = false;
  __u16 flags = 0;
  if (it == dir->end()) {
    flags = CEPH_READDIR_FRAG_END;
    complete = offset_str.empty(); // FIXME: what purpose does this serve
    if (complete)
      flags |= CEPH_READDIR_FRAG_COMPLETE;
  }

  // finish final blob
  ::encode(num_entries, dirbl);
  ::encode(flags, dirbl);
  dirbl.claim_append(dnbl);

  mdr->reply_extra_bl = dirbl;

  mdr->tracei = 0;
  respond_to_request(mdr, 0);
}

CDentryRef Server::prepare_stray_dentry(MDRequestRef& mdr, CInode *in)
{
  return mdcache->get_or_create_stray_dentry(in);
}

void Server::__unlink_finish(MDRequestRef& mdr, version_t dnpv)
{
  mdr->wait_committing();

  CInodeRef& in = mdr->in[0];
  CDentryRef& dn = mdr->dn[0].back();
  CDentryRef& straydn = mdr->straydn;

  mdcache->lock_parents_for_linkunlink(mdr, in.get(), dn.get(), true);

  if (straydn) {
    mdr->lock_object(straydn->get_dir_inode());
  }

  mdr->lock_object(in.get());

  dn->get_dir()->unlink_inode(dn.get());
  dn->pop_projected_linkage();
  
  if (straydn) {
    straydn->pop_projected_linkage();
  }

  dn->mark_dirty(dnpv, mdr->ls);
  mdr->early_apply();

  respond_to_request(mdr, 0);
}

bool Server::directory_is_nonempty(CInodeRef& diri)
{
  diri->mutex_assert_locked_by_me();
  dout(10) << "directory_is_nonempty" << *diri << dendl;
  assert(diri->filelock.can_read(-1));

  frag_info_t dirstat;
  version_t dirstat_version = diri->get_projected_inode()->dirstat.version;

  list<CDirRef> ls;
  diri->get_dirfrags(ls);
  for (auto p = ls.begin(); p != ls.end(); ++p) {
    CDirRef& dir = *p;
    const fnode_t *pf = dir->get_projected_fnode();
    if (pf->fragstat.size()) {
      dout(10) << "directory_is_nonempty has " << pf->fragstat.size()
	       << " items " << *dir << dendl;
      return true;
    }

    if (pf->accounted_fragstat.version == dirstat_version)
      dirstat.add(pf->accounted_fragstat);
    else
      dirstat.add(pf->fragstat);
  }
  return dirstat.size() != diri->get_projected_inode()->dirstat.size();
}

class C_MDS_unlink_finish : public Context {
  MDSRank *mds;
  MDRequestRef mdr;
  version_t dnpv;
public:
  C_MDS_unlink_finish(MDSRank *m, MDRequestRef& r, version_t pv)
    : mds(m), mdr(r), dnpv(pv) {}
  void finish(int r) {
    assert(r==0);
    mds->server->__unlink_finish(mdr, dnpv);
  }
};

void Server::handle_client_unlink(MDRequestRef& mdr)
{
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  int r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, true, true);
  if (r < 0)
    return;

  MClientRequest *req = mdr->client_request;
  // rmdir or unlink?
  bool rmdir = req->get_op() == CEPH_MDS_OP_RMDIR;

  CDentryRef& dn = mdr->dn[0].back();
  CInodeRef& in = mdr->in[0];

  mdr->lock_object(in.get());
  if (in->is_dir()) {               
    if (!rmdir) {                   
      respond_to_request(mdr, -EISDIR);
      return;                       
    }                               
  } else {                          
    if (rmdir) {                    
      respond_to_request(mdr, -ENOTDIR);
      return;                       
    }                               
  } 
  
  bool need_stray = (dn.get() == in->get_projected_parent_dn());
  mdr->unlock_object(in.get());

  CDentryRef straydn;
  if (need_stray) {
    straydn = prepare_stray_dentry(mdr, in.get());
    wrlocks.insert(&straydn->get_dir_inode()->filelock);
    wrlocks.insert(&straydn->get_dir_inode()->nestlock);
    xlocks.insert(&straydn->lock);
  }

  xlocks.insert(&in->linklock);
  if (rmdir)
    rdlocks.insert(&in->filelock);   // to verify it's empty
  
  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  mdcache->lock_parents_for_linkunlink(mdr, in.get(), dn.get(), false);

  const CDentry::linkage_t *dnl = dn->get_projected_linkage();

  if (straydn)
    assert(dnl->is_primary() && dnl->get_inode() == in.get());
  else
    assert((dnl->is_remote() && dnl->get_remote_ino() == in->ino()));

  if (straydn)
    mdr->lock_object(straydn->get_dir_inode());

  mdr->lock_object(in.get());

  if (rmdir && directory_is_nonempty(in)) {
    respond_to_request(mdr, -ENOTEMPTY);
    return;                       
  }

  if (straydn) {
    straydn->push_projected_linkage(in.get());
  }

  version_t dnpv = dn->pre_dirty();

  mdr->add_projected_inode(in.get(), true);
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  pi->ctime = mdr->get_op_stamp();
  pi->nlink--;

  dn->push_projected_linkage();

  if (dnl->is_primary()) {
    mdcache->predirty_journal_parents(mdr.get(), NULL, in.get(), straydn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
    mdcache->predirty_journal_parents(mdr.get(), NULL, in.get(), dn->get_dir(), PREDIRTY_PRIMARY|PREDIRTY_DIR, -1);
  } else {
    mdcache->predirty_journal_parents(mdr.get(), NULL, in.get(), NULL, PREDIRTY_PRIMARY);
    mdcache->predirty_journal_parents(mdr.get(), NULL, in.get(), dn->get_dir(), PREDIRTY_DIR, -1);
  }

  mdr->straydn = straydn;
  journal_and_reply(mdr, -1, 0, NULL, new C_MDS_unlink_finish(mds, mdr, dnpv));
}

void Server::__link_finish(MDRequestRef& mdr, version_t dnpv)
{ 
  mdr->wait_committing();

  CInodeRef& in = mdr->in[1];
  CDentryRef& dn = mdr->dn[0].back();

  mdcache->lock_parents_for_linkunlink(mdr, in.get(), dn.get(), true);

  mdr->lock_object(in.get());

  dn->pop_projected_linkage();

  dn->mark_dirty(dnpv, mdr->ls);
  mdr->early_apply();

  mds->server->respond_to_request(mdr, 0);
}

class C_MDS_link_finish : public Context {
  MDSRank *mds;
  MDRequestRef mdr;
  version_t dnpv;
  public:
  C_MDS_link_finish(MDSRank *m, MDRequestRef& r, version_t pv)
    : mds(m), mdr(r), dnpv(pv) {}
  void finish(int r) {
    assert(r==0);
    mds->server->__link_finish(mdr, dnpv);
  }
};

void Server::handle_client_link(MDRequestRef& mdr)
{
  int r;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false);
  if (r < 0)
    return;

  r = rdlock_path_pin_ref(mdr, 1, rdlocks, false);
  if (r < 0)
    return;

  CDentryRef& dn = mdr->dn[0].back();
  CInodeRef& in = mdr->in[1];

  if (in->is_dir()) {
    respond_to_request(mdr, -EINVAL);
    return;
  }

  xlocks.insert(&in->linklock);

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  mdcache->lock_parents_for_linkunlink(mdr, in.get(), dn.get(), false);
  assert(dn->get_projected_linkage()->is_null());

  mdr->lock_object(in.get());

  dn->push_projected_linkage(in->ino(), in->d_type());

  version_t dnpv = dn->pre_dirty();

  mdr->add_projected_inode(in.get(), true);
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();
  pi->ctime = mdr->get_op_stamp();
  pi->nlink++;

  mdcache->predirty_journal_parents(mdr.get(), NULL, in.get(), dn->get_dir(), PREDIRTY_DIR, 1);
  mdcache->predirty_journal_parents(mdr.get(), NULL, in.get(), 0, PREDIRTY_PRIMARY);

  journal_and_reply(mdr, 1, 0, NULL, new C_MDS_link_finish(mds, mdr, dnpv));
}

void Server::__rename_finish(MDRequestRef& mdr, version_t srcdn_pv, version_t destdn_pv)
{
  mdr->wait_committing();

  CDentryRef& srcdn = mdr->dn[1].back();
  CInodeRef& srci = mdr->in[1];
  CDentryRef& destdn = mdr->dn[0].back();
  CInodeRef& oldin = mdr->in[0];
  CDentryRef& straydn = mdr->straydn;

  mdcache->lock_parents_for_rename(mdr, srci.get(), oldin.get(),
				   srcdn.get(), destdn.get(), true);
  if (straydn) {
    mdr->lock_object(straydn->get_dir_inode());
  }

  if (oldin && oldin != srci) {
    if (srci->is_lt(oldin.get())) {
      srci->mutex_lock();
      oldin->mutex_lock();
    } else {
      oldin->mutex_lock();
      srci->mutex_lock();
    }
    mdr->add_locked_object(srci.get());
    mdr->add_locked_object(oldin.get());
  } else {
    mdr->lock_object(srci.get());
  }

  bool linkmerge = (srci == oldin) && srcdn->get_dir_inode()->is_stray();

  if (!linkmerge) {
    const CDentry::linkage_t *dest_dnl = destdn->get_linkage();
    if (dest_dnl->is_primary()) {
      assert(straydn);

      destdn->get_dir()->unlink_inode(destdn.get());

      straydn->pop_projected_linkage();
    } else if (dest_dnl->is_remote()) {
      destdn->get_dir()->unlink_inode(destdn.get());
    }
  }

  bool srcdn_was_remote = srcdn->get_linkage()->is_remote();
  srcdn->get_dir()->unlink_inode(srcdn.get());

  if (srcdn_was_remote) {
    if (!linkmerge) {
      destdn->pop_projected_linkage();
    }
  } else {
    if (linkmerge) {
      destdn->get_dir()->unlink_inode(destdn.get());
    }
    destdn->pop_projected_linkage();
  }

  srcdn->pop_projected_linkage();

  srcdn->mark_dirty(srcdn_pv, mdr->ls);
  if (!linkmerge && srcdn_was_remote)
    destdn->mark_dirty(destdn_pv, mdr->ls);

  if (mdr->hold_rename_dir_mutex) {
    mdcache->unlock_rename_dir_mutex();
    mdr->hold_rename_dir_mutex = false;
  }

  mdr->early_apply();
  respond_to_request(mdr, 0);
}

class C_MDS_rename_finish : public Context {
  MDSRank *mds;
  MDRequestRef mdr;
  version_t srcdn_pv;
  version_t destdn_pv;
  public:
  C_MDS_rename_finish(MDSRank *m, MDRequestRef& r, version_t spv, version_t dpv)
    : mds(m), mdr(r), srcdn_pv(spv), destdn_pv(dpv) {}
  void finish(int r) {
    assert(r==0);
    mds->server->__rename_finish(mdr, srcdn_pv, destdn_pv);
  }
};

void Server::handle_client_rename(MDRequestRef& mdr)
{
  int r;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  r = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, true, false);
  if (r < 0)
    return;

  r = rdlock_path_xlock_dentry(mdr, 1, rdlocks, wrlocks, xlocks, true, true);
  if (r < 0)
    return;

  CDentryRef& srcdn = mdr->dn[1].back();
  CInodeRef& srci = mdr->in[1];
  CDentryRef& destdn = mdr->dn[0].back();
  CInodeRef& oldin = mdr->in[0];

  bool need_stray = false;
  if (oldin) {
    mdr->lock_object(oldin.get());
    if (oldin->is_dir() && !srci->is_dir()) {
      respond_to_request(mdr, -EISDIR);
      return;
    }
    if (!oldin->is_dir() && srci->is_dir()) {
      respond_to_request(mdr, -ENOTDIR);
      return;
    }
    if (srci == oldin) {
      if (!srcdn->get_dir_inode()->is_stray()) {
	respond_to_request(mdr, 0);  // no-op.  POSIX makes no sense.
	return;
      }
    }
    need_stray = (srci != oldin && destdn.get() == oldin->get_projected_parent_dn());
    mdr->unlock_object(oldin.get());
  }

  CDentryRef straydn;
  if (need_stray) {
    straydn = prepare_stray_dentry(mdr, oldin.get());
    wrlocks.insert(&straydn->get_dir_inode()->filelock);
    wrlocks.insert(&straydn->get_dir_inode()->nestlock);
    xlocks.insert(&straydn->lock);
  }

  // we need to update srci's ctime.  xlock its least contended lock to do that...
  xlocks.insert(&srci->linklock);
  // xlock oldin (for nlink--)
  if (oldin) {
    xlocks.insert(&oldin->linklock);
    if (oldin->is_dir())
      rdlocks.insert(&oldin->filelock);
  }

  r = locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks);
  if (r <= 0) {
    if (r == -EAGAIN)
      dispatch_client_request(mdr); // revalidate path
    return;
  }

  r = mdcache->lock_parents_for_rename(mdr, srci.get(), oldin.get(),
		  		       srcdn.get(), destdn.get(), false);
  if (r < 0) {
    respond_to_request(mdr, r);
    return;
  }

  const CDentry::linkage_t *src_dnl = srcdn->get_projected_linkage();
  const CDentry::linkage_t *dest_dnl = destdn->get_projected_linkage();
  
  assert((src_dnl->is_primary() && src_dnl->get_inode() == srci.get()) ||
	 (src_dnl->is_remote() && src_dnl->get_remote_ino() == srci->ino()));
  assert((!oldin && dest_dnl->is_null()) ||
	 (need_stray && dest_dnl->is_primary() && dest_dnl->get_inode() == oldin.get()) ||
	 (!need_stray && dest_dnl->is_remote() && dest_dnl->get_remote_ino() == oldin->ino()));

  bool linkmerge = srci == oldin;
  if (linkmerge) {
    assert(srcdn->get_dir_inode()->is_stray());
    assert(src_dnl->is_primary());
  }

  if (straydn)
    mdr->lock_object(straydn->get_dir_inode());

  if (oldin && oldin != srci) {
    if (srci->is_lt(oldin.get())) {
      srci->mutex_lock();
      oldin->mutex_lock();
    } else {
      oldin->mutex_lock();
      srci->mutex_lock();
    }
    mdr->add_locked_object(srci.get());
    mdr->add_locked_object(oldin.get());
  } else {
    mdr->lock_object(srci.get());
  }

  if (oldin && oldin->is_dir() && directory_is_nonempty(oldin)) {
    if (mdr->hold_rename_dir_mutex) {
      mdcache->unlock_rename_dir_mutex();
      mdr->hold_rename_dir_mutex = false;
    }
    respond_to_request(mdr, -ENOTEMPTY);
  }

  inode_t *pi;
  inode_t *tpi = NULL;

  version_t srcdn_pv = srcdn->pre_dirty();
  version_t destdn_pv = 0;
  if (!linkmerge && src_dnl->is_remote())
    destdn_pv = destdn->pre_dirty();

  if (!linkmerge && !dest_dnl->is_null()) {
    mdr->add_projected_inode(oldin.get(), true);
    tpi = oldin->project_inode();
    if (dest_dnl->is_primary()) {
      straydn->push_projected_linkage(oldin.get());
      tpi->version = straydn->pre_dirty(tpi->version);
    } else {
      tpi->version = oldin->pre_dirty();
    }
  }

  mdr->add_projected_inode(srci.get(), true);
  if (src_dnl->is_remote()) {
    pi = srci->project_inode();
    pi->version = srci->pre_dirty();
    if (!linkmerge) {
      destdn->push_projected_linkage(src_dnl->get_remote_ino(), src_dnl->get_remote_d_type());
    }
  } else {
    destdn->push_projected_linkage(srci.get());
    pi = srci->project_inode();
    pi->version = destdn->pre_dirty(pi->version);
  }

  if (!linkmerge) {
    pi->ctime = mdr->get_op_stamp();
    if (tpi) {
      tpi->ctime = mdr->get_op_stamp();
      tpi->nlink--;
    }
  }

  srcdn->push_projected_linkage();

  // unlock rename_dir_mutex after projecting linkages
  if (mdr->hold_rename_dir_mutex) {
    mdcache->unlock_rename_dir_mutex();
    mdr->hold_rename_dir_mutex = false;
  }

  if (straydn) {
    mdcache->predirty_journal_parents(mdr.get(), NULL, oldin.get(), straydn->get_dir(),
				      PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  }
  int predirty_dir = linkmerge ? 0 : PREDIRTY_DIR;
  int predirty_primary;
  if (!dest_dnl->is_null()) {
    predirty_primary = dest_dnl->is_primary() ? PREDIRTY_PRIMARY : 0;
    mdcache->predirty_journal_parents(mdr.get(), NULL, oldin.get(), destdn->get_dir(),
				      predirty_dir|predirty_primary , -1);
  }

  predirty_primary = (src_dnl->is_primary() && srcdn->get_dir() != destdn->get_dir()) ? PREDIRTY_PRIMARY : 0;
  mdcache->predirty_journal_parents(mdr.get(), NULL, srci.get(), destdn->get_dir(),
				    predirty_dir|predirty_primary, 1);
  mdcache->predirty_journal_parents(mdr.get(), NULL, srci.get(), srcdn->get_dir(),
				    predirty_dir|predirty_primary, -1);

  if (!linkmerge) {
    if (dest_dnl->is_remote()) {
      mdcache->predirty_journal_parents(mdr.get(), NULL, oldin.get(), NULL, PREDIRTY_PRIMARY);
    }
    if (src_dnl->is_remote()) {
      mdcache->predirty_journal_parents(mdr.get(), NULL, srci.get(), NULL, PREDIRTY_PRIMARY);
    }
  }

  mdr->straydn = straydn;
  journal_and_reply(mdr, 1, 0, NULL, new C_MDS_rename_finish(mds, mdr, srcdn_pv, destdn_pv));
}
