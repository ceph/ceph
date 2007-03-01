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

#include "MDCache.h"
#include "MDStore.h"
#include "CInode.h"
#include "CDir.h"
#include "MDS.h"
#include "MDSMap.h"
#include "MDLog.h"
#include "AnchorClient.h"
#include "Migrator.h"
#include "Renamer.h"

#include "include/filepath.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "events/EString.h"
#include "events/EUnlink.h"

#include "messages/MRenameWarning.h"
#include "messages/MRenameNotify.h"
#include "messages/MRenameNotifyAck.h"
#include "messages/MRename.h"
#include "messages/MRenameAck.h"
#include "messages/MRenameReq.h"
#include "messages/MRenamePrep.h"



void Renamer::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_MDS_RENAMEWARNING:
    handle_rename_warning((MRenameWarning*)m);
    break;
  case MSG_MDS_RENAMENOTIFY:
    handle_rename_notify((MRenameNotify*)m);
    break;
  case MSG_MDS_RENAMENOTIFYACK:
    handle_rename_notify_ack((MRenameNotifyAck*)m);
    break;
  case MSG_MDS_RENAME:
    handle_rename((MRename*)m);
    break;
  case MSG_MDS_RENAMEREQ:
    handle_rename_req((MRenameReq*)m);
    break;
  case MSG_MDS_RENAMEPREP:
    handle_rename_prep((MRenamePrep*)m);
    break;
  case MSG_MDS_RENAMEACK:
    handle_rename_ack((MRenameAck*)m);
    break;

  default:
	assert(0);
  }
}


// renaming!


/*
 fix_renamed_dir():

 caller has already:
   - relinked inode in new location
   - fixed in->is_auth()
   - set dir_auth, if appropriate

 caller has not:
   - touched in->dir
   - updated import/export tables
*/
void Renamer::fix_renamed_dir(CDir *srcdir,
                              CInode *in,
                              CDir *destdir,
                              bool authchanged,   // _inode_ auth
                              int dir_auth)        // dir auth (for certain cases)
{
  dout(7) << "fix_renamed_dir on " << *in << endl;
  dout(7) << "fix_renamed_dir on " << *in->dir << endl;

  if (in->dir->is_auth()) {
    // dir ours
    dout(7) << "dir is auth" << endl;
    assert(!in->dir->is_export());

    if (in->is_auth()) {
      // inode now ours

      if (authchanged) {
        // inode _was_ replica, now ours
        dout(7) << "inode was replica, now ours.  removing from import list." << endl;
        assert(in->dir->is_import());
        
        // not import anymore!
        cache->imports.erase(in->dir);
        in->dir->state_clear(CDIR_STATE_IMPORT);
        in->dir->put(CDir::PIN_IMPORT);

        in->dir->set_dir_auth( CDIR_AUTH_PARENT );
        dout(7) << " fixing dir_auth to be " << in->dir->get_dir_auth() << endl;

        // move my nested imports to in's containing import
        CDir *con = cache->get_auth_container(in->dir);
        assert(con);
        for (set<CDir*>::iterator p = cache->nested_exports[in->dir].begin();
             p != cache->nested_exports[in->dir].end();
             p++) {
          dout(7) << "moving nested export under new container " << *con << endl;
          cache->nested_exports[con].insert(*p);
        }
        cache->nested_exports.erase(in->dir);
        
      } else {
        // inode was ours, still ours.
        dout(7) << "inode was ours, still ours." << endl;
        assert(!in->dir->is_import());
        assert(in->dir->get_dir_auth() == CDIR_AUTH_PARENT);
        
        // move any exports nested beneath me?
        CDir *newcon = cache->get_auth_container(in->dir);
        assert(newcon);
        CDir *oldcon = cache->get_auth_container(srcdir);
        assert(oldcon);
        if (newcon != oldcon) {
          dout(7) << "moving nested exports under new container" << endl;
          set<CDir*> nested;
          cache->find_nested_exports_under(oldcon, in->dir, nested);
          for (set<CDir*>::iterator it = nested.begin();
               it != nested.end();
               it++) {
            dout(7) << "moving nested export " << *it << " under new container" << endl;
            cache->nested_exports[oldcon].erase(*it);
            cache->nested_exports[newcon].insert(*it);
          }
        }
      }

    } else {
      // inode now replica

      if (authchanged) {
        // inode was ours, but now replica
        dout(7) << "inode was ours, now replica.  adding to import list." << endl;

        // i am now an import
        cache->imports.insert(in->dir);
        in->dir->state_set(CDIR_STATE_IMPORT);
        in->dir->get(CDir::PIN_IMPORT);

        in->dir->set_dir_auth( mds->get_nodeid() );
        dout(7) << " fixing dir_auth to be " << in->dir->get_dir_auth() << endl;

        // find old import
        CDir *oldcon = cache->get_auth_container(srcdir);
        assert(oldcon);
        dout(7) << " oldcon is " << *oldcon << endl;

        // move nested exports under me 
        set<CDir*> nested;
        cache->find_nested_exports_under(oldcon, in->dir, nested);  
        for (set<CDir*>::iterator it = nested.begin();
             it != nested.end();
             it++) {
          dout(7) << "moving nested export " << *it << " under me" << endl;
          cache->nested_exports[oldcon].erase(*it);
          cache->nested_exports[in->dir].insert(*it);
        }

      } else {
        // inode was replica, still replica
        dout(7) << "inode was replica, still replica.  doing nothing." << endl;
        assert(in->dir->is_import());

        // verify dir_auth
        assert(in->dir->get_dir_auth() == mds->get_nodeid()); // me, because i'm auth for dir.
        assert(in->authority() != in->dir->get_dir_auth());   // inode not me.
      }

      assert(in->dir->is_import());
    }

  } else {
    // dir is not ours
    dout(7) << "dir is not auth" << endl;

    if (in->is_auth()) {
      // inode now ours

      if (authchanged) {
        // inode was replica, now ours
        dout(7) << "inode was replica, now ours.  now an export." << endl;
        assert(!in->dir->is_export());
        
        // now export
        cache->exports.insert(in->dir);
        in->dir->state_set(CDIR_STATE_EXPORT);
        in->dir->get(CDir::PIN_EXPORT);
        
        assert(dir_auth >= 0);  // better be defined
        in->dir->set_dir_auth( dir_auth );
        dout(7) << " fixing dir_auth to be " << in->dir->get_dir_auth() << endl;
        
        CDir *newcon = cache->get_auth_container(in->dir);
        assert(newcon);
        cache->nested_exports[newcon].insert(in->dir);

      } else {
        // inode was ours, still ours
        dout(7) << "inode was ours, still ours.  did my import change?" << endl;

        // sanity
        assert(in->dir->is_export());
        assert(in->dir->get_dir_auth() >= 0);              
        assert(in->dir->get_dir_auth() != in->authority());

        // moved under new import?
        CDir *oldcon = cache->get_auth_container(srcdir);
        CDir *newcon = cache->get_auth_container(in->dir);
        if (oldcon != newcon) {
          dout(7) << "moving myself under new import " << *newcon << endl;
          cache->nested_exports[oldcon].erase(in->dir);
          cache->nested_exports[newcon].insert(in->dir);
        }
      }

      assert(in->dir->is_export());
    } else {
      // inode now replica

      if (authchanged) {
        // inode was ours, now replica
        dout(7) << "inode was ours, now replica.  removing from export list." << endl;
        assert(in->dir->is_export());

        // remove from export list
        cache->exports.erase(in->dir);
        in->dir->state_clear(CDIR_STATE_EXPORT);
        in->dir->put(CDir::PIN_EXPORT);
        
        CDir *oldcon = cache->get_auth_container(srcdir);
        assert(oldcon);
        assert(cache->nested_exports[oldcon].count(in->dir) == 1);
        cache->nested_exports[oldcon].erase(in->dir);

        // simplify dir_auth
        if (in->authority() == in->dir->authority()) {
          in->dir->set_dir_auth( CDIR_AUTH_PARENT );
          dout(7) << "simplified dir_auth to -1, inode auth is (also) " << in->authority() << endl;
        } else {
          assert(in->dir->get_dir_auth() >= 0);    // someone else's export,
        }

      } else {
        // inode was replica, still replica
        dout(7) << "inode was replica, still replica.  do nothing." << endl;
        
        // fix dir_auth?
        if (in->authority() == dir_auth)
          in->dir->set_dir_auth( CDIR_AUTH_PARENT );
        else
          in->dir->set_dir_auth( dir_auth );
        dout(7) << " fixing dir_auth to be " << dir_auth << endl;

        // do nothing.
      }
      
      assert(!in->dir->is_export());
    }  
  }

  cache->show_imports();
}

/*
 * when initiator gets an ack back for a foreign rename
 */

class C_MDC_RenameNotifyAck : public Context {
  Renamer *rn;
  CInode *in;
  int initiator;

public:
  C_MDC_RenameNotifyAck(Renamer *r, 
	CInode *i, int init) : rn(r), in(i), initiator(init) {}
  void finish(int r) {
    rn->file_rename_ack(in, initiator);
  }
};



/************** initiator ****************/

/*
 * when we get MRenameAck (and rename is done, notifies gone out+acked, etc.)
 */
class C_MDC_RenameAck : public Context {
  Renamer *mdc;
  CDir *srcdir;
  CInode *in;
  Context *c;
public:
  C_MDC_RenameAck(Renamer *mdc, CDir *srcdir, CInode *in, Context *c) {
    this->mdc = mdc;
    this->srcdir = srcdir;
    this->in = in;
    this->c = c;
  }
  void finish(int r) {
    mdc->file_rename_finish(srcdir, in, c);
  }
};


void Renamer::file_rename(CDentry *srcdn, CDentry *destdn, Context *onfinish)
{
  assert(srcdn->is_xlocked());  // by me
  assert(destdn->is_xlocked());  // by me

  CDir *srcdir = srcdn->dir;
  string srcname = srcdn->name;
  
  CDir *destdir = destdn->dir;
  string destname = destdn->name;

  CInode *in = srcdn->inode;
  //Message *req = srcdn->xlockedby;


  // determine the players
  int srcauth = srcdir->dentry_authority(srcdn->name);
  int destauth = destdir->dentry_authority(destname);


  // FOREIGN rename?
  if (srcauth != mds->get_nodeid() ||
      destauth != mds->get_nodeid()) {
    dout(7) << "foreign rename.  srcauth " << srcauth << ", destauth " << destauth << ", isdir " << srcdn->inode->is_dir() << endl;
    
    string destpath;
    destdn->make_path(destpath);

    if (destauth != mds->get_nodeid()) { 
      // make sure dest has dir open.
      dout(7) << "file_rename i'm not dest auth.  sending MRenamePrep to " << destauth << endl;
      
      // prep dest first, they must have the dir open!  rest will follow.
      string srcpath;
      srcdn->make_path(srcpath);
      
      MRenamePrep *m = new MRenamePrep(mds->get_nodeid(),  // i'm the initiator
                                       srcdir->ino(), srcname, srcpath, 
                                       destdir->ino(), destname, destpath,
                                       srcauth);  // tell dest who src is (maybe even me)
      mds->send_message_mds(m, destauth, MDS_PORT_CACHE);
      
      cache->show_imports();
      
    }
    
    else if (srcauth != mds->get_nodeid()) {
      if (destauth == mds->get_nodeid()) {
        dout(7) << "file_rename dest auth, not src auth.  sending MRenameReq" << endl;    
      } else {
        dout(7) << "file_rename neither src auth nor dest auth.  sending MRenameReq" << endl;    
      }
      
      // srcdn not important on destauth, just request
      MRenameReq *m = new MRenameReq(mds->get_nodeid(),  // i'm the initiator
                                     srcdir->ino(), srcname, 
                                     destdir->ino(), destname, destpath, destauth);  // tell src who dest is (they may not know)
      mds->send_message_mds(m, srcauth, MDS_PORT_CACHE);
    }
    
    else
      assert(0);

    // set waiter on the inode (is this the best place?)
    in->add_waiter(CINODE_WAIT_RENAMEACK, 
                   new C_MDC_RenameAck(this, 
                                       srcdir, in, onfinish));
    return;
  }

  // LOCAL rename!
  assert(srcauth == mds->get_nodeid() && destauth == mds->get_nodeid());
  dout(7) << "file_rename src and dest auth, renaming locally (easy!)" << endl;
  
  // update our cache
  if (destdn->inode && destdn->inode->is_dirty())
    destdn->inode->mark_clean();

  cache->rename_file(srcdn, destdn);
  
  // update imports/exports?
  if (in->is_dir() && in->dir) 
    fix_renamed_dir(srcdir, in, destdir, false);  // auth didnt change

  // mark dentries dirty
  srcdn->_mark_dirty(); // fixme
  destdn->_mark_dirty(); // fixme
  in->_mark_dirty();    // fixme
 
 
  // local, restrict notify to ppl with open dirs
  set<int> notify;
  for (map<int,int>::iterator it = srcdir->replicas_begin();
       it != srcdir->replicas_end();
       ++it)
    notify.insert(it->first);
  for (map<int,int>::iterator it = destdir->replicas_begin();
       it != destdir->replicas_end();
       it++)
    if (notify.count(it->first) == 0) notify.insert(it->first);
  
  if (notify.size()) {
    // warn + notify
    file_rename_warn(in, notify);
    file_rename_notify(in, srcdir, srcname, destdir, destname, notify, mds->get_nodeid());

    // wait for MRenameNotifyAck's
    in->add_waiter(CINODE_WAIT_RENAMENOTIFYACK,
                   new C_MDC_RenameNotifyAck(this, in, mds->get_nodeid()));  // i am initiator

    // wait for finish
    in->add_waiter(CINODE_WAIT_RENAMEACK,
                   new C_MDC_RenameAck(this, srcdir, in, onfinish));
  } else {
    // sweet, no notify necessary, we're done!
    file_rename_finish(srcdir, in, onfinish);
  }
}

void Renamer::handle_rename_ack(MRenameAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  
  dout(7) << "handle_rename_ack on " << *in << endl;

  // all done!
  in->finish_waiting(CINODE_WAIT_RENAMEACK);

  delete m;
}

void Renamer::file_rename_finish(CDir *srcdir, CInode *in, Context *c)
{
  dout(10) << "file_rename_finish on " << *in << endl;

  // did i empty out an imported dir?  FIXME this check should go somewhere else???
  if (srcdir->is_import() && !srcdir->inode->is_root() && srcdir->get_size() == 0) 
    cache->migrator->export_empty_import(srcdir);

  // finish our caller
  if (c) {
    c->finish(0);
    delete c;
  }
}


/************* src **************/


/** handle_rename_req
 * received by auth of src dentry (from init, or destauth if dir).  
 * src may not have dest dir open.
 * src will export inode, unlink|rename, and send MRename to dest.
 */
void Renamer::handle_rename_req(MRenameReq *m)
{
  // i am auth, i will have it.
  CInode *srcdiri = cache->get_inode(m->get_srcdirino());
  CDir *srcdir = srcdiri->dir;
  CDentry *srcdn = srcdir->lookup(m->get_srcname());
  assert(srcdn);
  
  // do it
  file_rename_foreign_src(srcdn, 
                          m->get_destdirino(), m->get_destname(), m->get_destpath(), m->get_destauth(), 
                          m->get_initiator());
  delete m;
}


void Renamer::file_rename_foreign_src(CDentry *srcdn, 
                                      inodeno_t destdirino, string& destname, string& destpath, int destauth, 
                                      int initiator)
{
  dout(7) << "file_rename_foreign_src " << *srcdn << endl;

  CDir *srcdir = srcdn->dir;
  string srcname = srcdn->name;

  // (we're basically exporting this inode)
  CInode *in = srcdn->inode;
  assert(in);
  assert(in->is_auth());

  if (in->is_dir()) cache->show_imports();

  // encode and export inode state
  bufferlist inode_state;
  cache->migrator->encode_export_inode(in, inode_state, destauth);

  // send
  MRename *m = new MRename(initiator,
                           srcdir->ino(), srcdn->name, destdirino, destname,
                           inode_state);
  mds->send_message_mds(m, destauth, MDS_PORT_CACHE);
  
  // have dest?
  CInode *destdiri = cache->get_inode(m->get_destdirino());
  CDir *destdir = 0;
  if (destdiri) destdir = destdiri->dir;
  CDentry *destdn = 0;
  if (destdir) destdn = destdir->lookup(m->get_destname());

  // discover src
  if (!destdn) {
    dout(7) << "file_rename_foreign_src doesn't have destdn, discovering " << destpath << endl;

    filepath destfilepath = destpath;
    vector<CDentry*> trace;
    int r = cache->path_traverse(destfilepath, trace, true,
								 m, new C_MDS_RetryMessage(mds, m), 
								 MDS_TRAVERSE_DISCOVER);
    assert(r>0);
    return;
  }

  assert(destdn);

  // update our cache
  cache->rename_file(srcdn, destdn);
  
  // update imports/exports?
  if (in->is_dir() && in->dir) 
    fix_renamed_dir(srcdir, in, destdir, true);  // auth changed

  srcdn->_mark_dirty(); // fixme

  // proxy!
  in->state_set(CInode::STATE_PROXY);
  in->get(CInode::PIN_PROXY);
  
  // generate notify list (everybody but src|dst) and send warnings
  set<int> notify;
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i != mds->get_nodeid() &&  // except the source
        i != destauth)             // and the dest
      notify.insert(i);
  }
  file_rename_warn(in, notify);


  // wait for MRenameNotifyAck's
  in->add_waiter(CINODE_WAIT_RENAMENOTIFYACK,
                 new C_MDC_RenameNotifyAck(this, in, initiator));
}

void Renamer::file_rename_warn(CInode *in,
                               set<int>& notify)
{
  // note gather list
  rename_waiting_for_ack[in->ino()] = notify;

  // send
  for (set<int>::iterator it = notify.begin();
       it != notify.end();
       it++) {
    dout(10) << "file_rename_warn to " << *it << " for " << *in << endl;
    mds->send_message_mds(new MRenameWarning(in->ino()), *it, MDS_PORT_CACHE);
  }
}


void Renamer::handle_rename_notify_ack(MRenameNotifyAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  dout(7) << "handle_rename_notify_ack on " << *in << endl;

  int source = m->get_source().num();
  rename_waiting_for_ack[in->ino()].erase(source);
  if (rename_waiting_for_ack[in->ino()].empty()) {
    // last one!
	rename_waiting_for_ack.erase(in->ino());
    in->finish_waiting(CINODE_WAIT_RENAMENOTIFYACK, 0);
  } else {
    dout(7) << "still waiting for " << rename_waiting_for_ack[in->ino()] << endl;
  }
}


void Renamer::file_rename_ack(CInode *in, int initiator) 
{
  // we got all our MNotifyAck's.

  // was i proxy (if not, it's cuz this was a local rename)
  if (in->state_test(CInode::STATE_PROXY)) {
    dout(10) << "file_rename_ack clearing proxy bit on " << *in << endl;
    in->state_clear(CInode::STATE_PROXY);
    in->put(CInode::PIN_PROXY);
  }

  // done!
  if (initiator == mds->get_nodeid()) {
    // it's me, finish
    dout(7) << "file_rename_ack i am initiator, finishing" << endl;
    in->finish_waiting(CINODE_WAIT_RENAMEACK);
  } else {
    // send ack
    dout(7) << "file_rename_ack sending MRenameAck to initiator " << initiator << endl;
    mds->send_message_mds(new MRenameAck(in->ino()), initiator, MDS_PORT_CACHE);
  }  
}




/************ dest *************/

/** handle_rename_prep
 * received by auth of dest dentry to make sure they have src + dir open.
 * this is so that when they get the inode and dir, they can update exports etc properly.
 * will send MRenameReq to src.
 */
void Renamer::handle_rename_prep(MRenamePrep *m)
{
  // open src
  filepath srcpath = m->get_srcpath();
  vector<CDentry*> trace;
  int r = cache->path_traverse(srcpath, trace, false,
							   m, new C_MDS_RetryMessage(mds, m), 
							   MDS_TRAVERSE_DISCOVER);

  if (r>0) return;

  // ok!
  CInode *srcin = trace[trace.size()-1]->inode;
  assert(srcin);
  
  dout(7) << "handle_rename_prep have srcin " << *srcin << endl;

  if (srcin->is_dir()) {
    if (!srcin->dir) {
      dout(7) << "handle_rename_prep need to open dir" << endl;
      cache->open_remote_dir(srcin,
							 new C_MDS_RetryMessage(mds,m));
      return;
    }

    dout(7) << "handle_rename_prep have dir " << *srcin->dir << endl;    
  }

  // pin
  srcin->get(CInode::PIN_RENAMESRC);

  // send rename request
  MRenameReq *req = new MRenameReq(m->get_initiator(),  // i'm the initiator
                                   m->get_srcdirino(), m->get_srcname(), 
                                   m->get_destdirino(), m->get_destname(), m->get_destpath(),
                                   mds->get_nodeid());  // i am dest
  mds->send_message_mds(req, m->get_srcauth(), MDS_PORT_CACHE);
  delete m;
  return;
}



/** handle_rename
 * received by auth of dest dentry.   includes exported inode info.
 * dest may not have srcdir open.
 */
void Renamer::handle_rename(MRename *m)
{
  // srcdn (required)
  CInode *srcdiri = cache->get_inode(m->get_srcdirino());
  CDir *srcdir = srcdiri->dir;
  CDentry *srcdn = srcdir->lookup(m->get_srcname());
  string srcname = srcdn->name;
  assert(srcdn && srcdn->inode);

  dout(7) << "handle_rename srcdn " << *srcdn << endl;

  // destdn (required).  i am auth, so i will have it.
  CInode *destdiri = cache->get_inode(m->get_destdirino());
  CDir *destdir = destdiri->dir;
  CDentry *destdn = destdir->lookup(m->get_destname());
  string destname = destdn->name;
  assert(destdn);
  
  dout(7) << "handle_rename destdn " << *destdn << endl;

  // note old dir auth
  int old_dir_auth = -1;
  if (srcdn->inode->dir) old_dir_auth = srcdn->inode->dir->authority();
    
  // rename replica into position
  if (destdn->inode && destdn->inode->is_dirty())
    destdn->inode->mark_clean();

  cache->rename_file(srcdn, destdn);

  // decode + import inode (into new location start)
  int off = 0;
  // HACK
  bufferlist bufstate;
  bufstate.claim_append(m->get_inode_state());
  cache->migrator->decode_import_inode(destdn, bufstate, off, m->get_source().num());

  CInode *in = destdn->inode;
  assert(in);

  // update imports/exports?
  if (in->is_dir()) {
    assert(in->dir);  // i had better already ahve it open.. see MRenamePrep
    fix_renamed_dir(srcdir, in, destdir, true,  // auth changed
                    old_dir_auth);              // src is possibly new dir auth.
  }
  
  // mark dirty
  destdn->_mark_dirty(); // fixme
  in->_mark_dirty(); // fixme

  // unpin
  in->put(CInode::PIN_RENAMESRC);

  // ok, send notifies.
  set<int> notify;
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i != m->get_source().num() &&  // except the source
        i != mds->get_nodeid())  // and the dest
      notify.insert(i);
  }
  file_rename_notify(in, srcdir, srcname, destdir, destname, notify, m->get_source().num());

  delete m;
}


void Renamer::file_rename_notify(CInode *in, 
                                 CDir *srcdir, string& srcname, CDir *destdir, string& destname,
                                 set<int>& notify,
                                 int srcauth)
{
  /* NOTE: notify list might include myself */
  
  // tell
  string destdirpath;
  destdir->inode->make_path(destdirpath);
  
  for (set<int>::iterator it = notify.begin();
       it != notify.end();
       it++) {
    dout(10) << "file_rename_notify to " << *it << " for " << *in << endl;
    mds->send_message_mds(new MRenameNotify(in->ino(),
					    srcdir->ino(),
					    srcname,
					    destdir->ino(),
					    destdirpath,
					    destname,
					    srcauth),
			  *it, MDS_PORT_CACHE);
  }
}



/************** bystanders ****************/

void Renamer::handle_rename_warning(MRenameWarning *m)
{
  // add to warning list
  stray_rename_warnings.insert( m->get_ino() );
  
  // did i already see the notify?
  if (stray_rename_notifies.count(m->get_ino())) {
    // i did, we're good.
    dout(7) << "handle_rename_warning on " << m->get_ino() << ".  already got notify." << endl;
    
    handle_rename_notify(stray_rename_notifies[m->get_ino()]);
    stray_rename_notifies.erase(m->get_ino());
  } else {
    dout(7) << "handle_rename_warning on " << m->get_ino() << ".  waiting for notify." << endl;
  }
  
  // done
  delete m;
}


void Renamer::handle_rename_notify(MRenameNotify *m)
{
  // FIXME: when we do hard links, i think we need to 
  // have srcdn and destdn both, or neither,  always!

  // did i see the warning yet?
  if (!stray_rename_warnings.count(m->get_ino())) {
    // wait for it.
    dout(7) << "handle_rename_notify on " << m->get_ino() << ", waiting for warning." << endl;
    stray_rename_notifies[m->get_ino()] = m;
    return;
  }

  dout(7) << "handle_rename_notify dir " << m->get_srcdirino() << " dn " << m->get_srcname() << " to dir " << m->get_destdirino() << " dname " << m->get_destname() << endl;
  
  // src
  CInode *srcdiri = cache->get_inode(m->get_srcdirino());
  CDir *srcdir = 0;
  if (srcdiri) srcdir = srcdiri->dir;
  CDentry *srcdn = 0;
  if (srcdir) srcdn = srcdir->lookup(m->get_srcname());

  // dest
  CInode *destdiri = cache->get_inode(m->get_destdirino());
  CDir *destdir = 0;
  if (destdiri) destdir = destdiri->dir;
  CDentry *destdn = 0;
  if (destdir) destdn = destdir->lookup(m->get_destname());

  // have both?
  list<Context*> finished;
  if (srcdn && destdir) {
    CInode *in = srcdn->inode;

    int old_dir_auth = -1;
    if (in && in->dir) old_dir_auth = in->dir->authority();

    if (!destdn) {
      destdn = destdir->add_dentry(m->get_destname());  // create null dentry
      destdn->lockstate = DN_LOCK_XLOCK;                // that's xlocked!
    }

    dout(7) << "handle_rename_notify renaming " << *srcdn << " to " << *destdn << endl;
    
    if (in) {
      cache->rename_file(srcdn, destdn);

      // update imports/exports?
      if (in && in->is_dir() && in->dir) {
        fix_renamed_dir(srcdir, in, destdir, false, old_dir_auth);  // auth didnt change
      }
    } else {
      dout(7) << " i don't have the inode (just null dentries)" << endl;
    }
    
  }

  else if (srcdn) {
    dout(7) << "handle_rename_notify no dest, but have src" << endl;
    dout(7) << "srcdn is " << *srcdn << endl;

    if (destdiri) {
      dout(7) << "have destdiri, opening dir " << *destdiri << endl;
      cache->open_remote_dir(destdiri,
							 new C_MDS_RetryMessage(mds,m));
    } else {
      filepath destdirpath = m->get_destdirpath();
      dout(7) << "don't have destdiri even, doing traverse+discover on " << destdirpath << endl;
      
      vector<CDentry*> trace;
      int r = cache->path_traverse(destdirpath, trace, true,
								   m, new C_MDS_RetryMessage(mds, m), 
								   MDS_TRAVERSE_DISCOVER);
      assert(r>0);
    }
    return;
  }

  else if (destdn) {
    dout(7) << "handle_rename_notify unlinking dst only " << *destdn << endl;
    if (destdn->inode) {
      destdir->unlink_inode(destdn);
    }
  }
  
  else {
    dout(7) << "handle_rename_notify didn't have srcdn or destdn" << endl;
    assert(srcdn == 0 && destdn == 0);
  }
  
  mds->queue_finished(finished);


  // ack
  dout(10) << "sending RenameNotifyAck back to srcauth " << m->get_srcauth() << endl;
  MRenameNotifyAck *ack = new MRenameNotifyAck(m->get_ino());
  mds->send_message_mds(ack, m->get_srcauth(), MDS_PORT_CACHE);
  

  stray_rename_warnings.erase( m->get_ino() );
  delete m;
}




