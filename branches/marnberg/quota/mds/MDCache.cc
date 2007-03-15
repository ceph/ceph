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
#include "MDS.h"
#include "Server.h"
#include "Locker.h"
#include "MDLog.h"
#include "MDBalancer.h"
#include "AnchorClient.h"
#include "Migrator.h"
#include "Renamer.h"

#include "MDSMap.h"

#include "CInode.h"
#include "CDir.h"

#include "include/filepath.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "common/Logger.h"

#include "osdc/Filer.h"

#include "events/EImportMap.h"
#include "events/EString.h"
#include "events/EUnlink.h"
#include "events/EPurgeFinish.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSImportMap.h"
#include "messages/MMDSCacheRejoin.h"
#include "messages/MMDSCacheRejoinAck.h"

#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MDirUpdate.h"
#include "messages/MCacheExpire.h"

#include "messages/MInodeFileCaps.h"

#include "messages/MInodeLink.h"
#include "messages/MInodeLinkAck.h"
#include "messages/MInodeUnlink.h"
#include "messages/MInodeUnlinkAck.h"

#include "messages/MLock.h"
#include "messages/MDentryUnlink.h"

#include "messages/MClientRequest.h"
#include "messages/MClientFileCaps.h"

#include "IdAllocator.h"

#include "common/Timer.h"

#include <assert.h>
#include <errno.h>
#include <iostream>
#include <string>
#include <map>
using namespace std;

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".cache "




MDCache::MDCache(MDS *m)
{
  mds = m;
  migrator = new Migrator(mds, this);
  renamer = new Renamer(mds, this);
  root = NULL;
  lru.lru_set_max(g_conf.mds_cache_size);
  lru.lru_set_midpoint(g_conf.mds_cache_mid);

  did_shutdown_exports = false;
  did_shutdown_log_cap = false;
  shutdown_commits = 0;
}

MDCache::~MDCache() 
{
  delete migrator;
  delete renamer;
}



void MDCache::log_stat(Logger *logger)
{
  if (get_root()) {
    logger->set("popanyd", (int)get_root()->popularity[MDS_POP_ANYDOM].meta_load());
    logger->set("popnest", (int)get_root()->popularity[MDS_POP_NESTED].meta_load());
  }
  logger->set("c", lru.lru_get_size());
  logger->set("cpin", lru.lru_get_num_pinned());
  logger->set("ctop", lru.lru_get_top());
  logger->set("cbot", lru.lru_get_bot());
  logger->set("cptail", lru.lru_get_pintail());
}


// 

bool MDCache::shutdown()
{
  if (lru.lru_get_size() > 0) {
    dout(7) << "WARNING: mdcache shutodwn with non-empty cache" << endl;
    //show_cache();
    show_imports();
    //dump();
  }
  return true;
}


// MDCache

CInode *MDCache::create_inode()
{
  CInode *in = new CInode(this);

  // zero
  memset(&in->inode, 0, sizeof(inode_t));
  
  // assign ino
  in->inode.ino = mds->idalloc->alloc_id();

  in->inode.nlink = 1;   // FIXME

  in->inode.layout = g_OSD_FileLayout;

  add_inode(in);  // add
  return in;
}

void MDCache::destroy_inode(CInode *in)
{
  mds->idalloc->reclaim_id(in->ino());
  remove_inode(in);
}


void MDCache::add_inode(CInode *in) 
{
  // add to lru, inode map
  assert(inode_map.count(in->ino()) == 0);  // should be no dup inos!
  inode_map[ in->ino() ] = in;
}

void MDCache::remove_inode(CInode *o) 
{ 
  dout(14) << "remove_inode " << *o << endl;
  if (o->get_parent_dn()) {
    // FIXME: multiple parents?
    CDentry *dn = o->get_parent_dn();
    assert(!dn->is_dirty());
    if (dn->is_sync())
      dn->dir->remove_dentry(dn);  // unlink inode AND hose dentry
    else
      dn->dir->unlink_inode(dn);   // leave dentry
  }
  inode_map.erase(o->ino());    // remove from map
}


/*
 * take note of where we write import_maps in the log, as we need
 * to take care not to expire them until an updated map is safely flushed.
 */
class C_MDS_WroteImportMap : public Context {
  MDLog *mdlog;
  off_t end_off;
public:
  C_MDS_WroteImportMap(MDLog *ml, off_t eo) : mdlog(ml), end_off(eo) { }
  void finish(int r) {
    //    cout << "WroteImportMap at " << end_off << endl;
    if (r >= 0)
      mdlog->last_import_map = end_off;
    mdlog->writing_import_map = false;
  }
};



void MDCache::log_import_map(Context *onsync)
{
  dout(10) << "log_import_map " << imports.size() << " imports, "
	   << exports.size() << " exports" << endl;
  
  EImportMap *le = new EImportMap;

  // include import/export inodes,
  // and a spanning tree to tie it to the root of the fs
  for (set<CDir*>::iterator p = imports.begin();
       p != imports.end();
       p++) {
    CDir *im = *p;
    le->imports.insert(im->ino());
    le->metablob.add_dir_context(im, true);
    le->metablob.add_dir(im, false);

    if (nested_exports.count(im)) {
      for (set<CDir*>::iterator q = nested_exports[im].begin();
	   q != nested_exports[im].end();
	   ++q) {
	CDir *ex = *q;
	le->nested_exports[im->ino()].insert(ex->ino());
	le->exports.insert(ex->ino());
	le->metablob.add_dir_context(ex);
	le->metablob.add_dir(ex, false);
      }
    }
  }

  mds->mdlog->writing_import_map = true;
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_sync(new C_MDS_WroteImportMap(mds->mdlog, mds->mdlog->get_write_pos()));
  if (onsync)
    mds->mdlog->wait_for_sync(onsync);
}





// =====================
// recovery stuff

void MDCache::send_pending_import_maps()
{
  if (wants_import_map.empty())
    return;  // nothing to send.

  // only if it's appropriate!
  if (migrator->is_exporting()) {
    dout(7) << "send_pending_import_maps waiting, exports still in progress" << endl;
    return;  // not now
  }

  // ok, send them.
  for (set<int>::iterator p = wants_import_map.begin();
       p != wants_import_map.end();
       p++) 
    send_import_map_now(*p);
  wants_import_map.clear();
}

void MDCache::send_import_map(int who)
{
  if (migrator->is_exporting())
    send_import_map_later(who);
  else
    send_import_map_now(who);
}

void MDCache::send_import_map_now(int who)
{
  dout(10) << "send_import_map to mds" << who << endl;

  MMDSImportMap *m = new MMDSImportMap;

  // known
  for (set<CDir*>::iterator p = imports.begin();
       p != imports.end();
       p++) {
    CDir *im = *p;

    if (migrator->is_importing(im->ino())) {
      // ambiguous (mid-import)
      m->add_ambiguous_import(im->ino(), 
			      migrator->get_import_bounds(im->ino()));
    } else {
      // not ambiguous.
      m->add_import(im->ino());
      
      if (nested_exports.count(im)) {
	for (set<CDir*>::iterator q = nested_exports[im].begin();
	     q != nested_exports[im].end();
	     ++q) {
	  CDir *ex = *q;
	  m->add_import_export(im->ino(), ex->ino());
	}
      }
    }
  }

  // ambiguous
  for (map<inodeno_t, set<inodeno_t> >::iterator p = my_ambiguous_imports.begin();
       p != my_ambiguous_imports.end();
       ++p) 
    m->add_ambiguous_import(p->first, p->second);
  
  // second
  mds->send_message_mds(m, who, MDS_PORT_CACHE);
}



/*
 * during resolve state, we share import_maps to determine who
 * is authoritative for which trees.  we expect to get an import_map
 * from _everyone_ in the recovery_set (the mds cluster at the time of
 * the first failure).
 */
void MDCache::handle_import_map(MMDSImportMap *m)
{
  dout(7) << "handle_import_map from " << m->get_source() << endl;
  int from = m->get_source().num();

  // FIXME: check if we are a surviving ambiguous importer

  // update my dir_auth values
  for (map<inodeno_t, set<inodeno_t> >::iterator pi = m->imap.begin();
       pi != m->imap.end();
       ++pi) {
    CInode *imi = get_inode(pi->first);
    if (!imi) continue;
    CDir *im = imi->dir;
    if (!im) continue;
    
    im->set_dir_auth(from);
    
    for (set<inodeno_t>::iterator pe = pi->second.begin();
	 pe != pi->second.end();
	 ++pe) {
      CInode *exi = get_inode(*pe);
      if (!exi) continue;
      CDir *ex = exi->dir;
      if (!ex) continue;
      
      if (ex->get_dir_auth() == CDIR_AUTH_PARENT)
	ex->set_dir_auth(CDIR_AUTH_UNKNOWN);
    }
  }

  // note ambiguous imports too
  for (map<inodeno_t, set<inodeno_t> >::iterator pi = m->ambiguous_imap.begin();
       pi != m->ambiguous_imap.end();
       ++pi)
    mds->mdcache->other_ambiguous_imports[from][pi->first].swap( pi->second );

  // did i get them all?
  got_import_map.insert(from);
  
  if (got_import_map == recovery_set) {
    dout(10) << "got all import maps, ready to rejoin" << endl;
    disambiguate_imports();
    recalc_auth_bits();
    trim_non_auth(); 

    // move to rejoin state
    mds->set_want_state(MDSMap::STATE_REJOIN);
  
  } else {
    dout(10) << "still waiting for more importmaps, got " << got_import_map 
	     << ", need " << recovery_set << endl;
  }
  
  delete m;
}


void MDCache::disambiguate_imports()
{
  dout(10) << "disambiguate_imports" << endl;

  // other nodes' ambiguous imports
  for (map<int, map<inodeno_t, set<inodeno_t> > >::iterator p = other_ambiguous_imports.begin();
       p != other_ambiguous_imports.begin();
       ++p) {
    int who = p->first;

    for (map<inodeno_t, set<inodeno_t> >::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      CInode *diri = get_inode(q->first);
      if (!diri) continue;
      CDir *dir = diri->dir;
      if (!dir) continue;
      
      if (dir->authority() >= CDIR_AUTH_UNKNOWN) {
	dout(10) << "mds" << who << " did not import " << *dir << endl;
      } else {
	dout(10) << "mds" << who << " did import " << *dir << endl;
	int was = dir->authority();
	dir->set_dir_auth(who);
	
	for (set<inodeno_t>::iterator r = q->second.begin();
	     r != q->second.end();
	     ++r) {
	  CInode *exi = get_inode(q->first);
	  if (!exi) continue;
	  CDir *ex = exi->dir;
	  if (!ex) continue;
	  if (ex->get_dir_auth() == CDIR_AUTH_PARENT)
	    ex->set_dir_auth(was);
	  dout(10) << "   bound " << *ex << endl;
	}
      }
    }
  }
  other_ambiguous_imports.clear();

  // my ambiguous imports
  while (!my_ambiguous_imports.empty()) {
    map<inodeno_t, set<inodeno_t> >::iterator q = my_ambiguous_imports.begin();

    CInode *diri = get_inode(q->first);
    if (!diri) continue;
    CDir *dir = diri->dir;
    if (!dir) continue;
    
    if (dir->authority() != CDIR_AUTH_UNKNOWN) {
      dout(10) << "ambiguous import auth known, must not be me " << *dir << endl;
      cancel_ambiguous_import(q->first);
    } else {
      dout(10) << "ambiguous import auth unknown, must be me " << *dir << endl;
      finish_ambiguous_import(q->first);
    }
  }
  assert(my_ambiguous_imports.empty());

  show_imports();
}

void MDCache::cancel_ambiguous_import(inodeno_t dirino)
{
  assert(my_ambiguous_imports.count(dirino));
  dout(10) << "cancel_ambiguous_import " << dirino
	   << " bounds " << my_ambiguous_imports[dirino]
	   << endl;
  my_ambiguous_imports.erase(dirino);
}

void MDCache::finish_ambiguous_import(inodeno_t dirino)
{
  assert(my_ambiguous_imports.count(dirino));
  set<inodeno_t> bounds;
  bounds.swap(my_ambiguous_imports[dirino]);
  my_ambiguous_imports.erase(dirino);

  dout(10) << "finish_ambiguous_import " << dirino
	   << " bounds " << bounds
	   << endl;

  CInode *diri = get_inode(dirino);
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);
    
  // adjust dir_auth
  CDir *im = dir;
  if (dir->get_inode()->authority() == mds->get_nodeid()) {
    // parent is already me.  adding to existing import.
    im = get_auth_container(dir);
    if (!im) im = dir;
    nested_exports[im].erase(dir);
    exports.erase(dir);
    dir->set_dir_auth( CDIR_AUTH_PARENT );     
    dir->state_clear(CDIR_STATE_EXPORT);
    dir->put(CDir::PIN_EXPORT);
  } else {
    // parent isn't me.  new import.
    imports.insert(dir);
    dir->set_dir_auth( mds->get_nodeid() );               
    dir->state_set(CDIR_STATE_IMPORT);
    dir->get(CDir::PIN_IMPORT);
  }

  dout(10) << "  base " << *dir << endl;
  if (dir != im)
    dout(10) << "  under " << *im << endl;

  // bounds (exports, before)
  for (set<inodeno_t>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CInode *bi = get_inode(*p);
    assert(bi);
    CDir *bd = bi->dir;
    assert(bd);
    
    if (bd->get_dir_auth() == mds->get_nodeid()) {
      // still me.  was an import. 
      imports.erase(bd);
      bd->set_dir_auth( CDIR_AUTH_PARENT );   
      bd->state_clear(CDIR_STATE_IMPORT);
      bd->put(CDir::PIN_IMPORT);
      // move nested exports.
      for (set<CDir*>::iterator q = nested_exports[bd].begin();
	   q != nested_exports[bd].end();
	   ++q) 
	nested_exports[im].insert(*q);
      nested_exports.erase(bd);	

    } else {
      // not me anymore.  now an export.
      exports.insert(bd);
      nested_exports[im].insert(bd);
      assert(bd->get_dir_auth() != CDIR_AUTH_PARENT);
      bd->set_dir_auth( CDIR_AUTH_UNKNOWN );
      bd->state_set(CDIR_STATE_EXPORT);
      bd->get(CDir::PIN_EXPORT);
    }
    
    dout(10) << "  bound " << *bd << endl;
  }
}

void MDCache::finish_ambiguous_export(inodeno_t dirino, set<inodeno_t>& bounds)
{
  CInode *diri = get_inode(dirino);
  assert(diri);
  CDir *dir = diri->dir;
  assert(dir);
  
  dout(10) << "finish_ambiguous_export " << dirino 
	   << " bounds " << bounds
	   << endl;
  
  // adjust dir_auth
  CDir *im = get_auth_container(dir);
  if (dir->get_inode()->authority() == CDIR_AUTH_UNKNOWN) { 
    // was an import, hose it
    assert(im == dir);
    assert(imports.count(dir));
    imports.erase(dir);
    dir->set_dir_auth( CDIR_AUTH_PARENT );
    dir->state_clear(CDIR_STATE_IMPORT);
    dir->put(CDir::PIN_IMPORT);
  } else {
    // i'm now an export
    exports.insert(dir);
    nested_exports[im].insert(dir);
    dir->set_dir_auth( CDIR_AUTH_UNKNOWN );  // not me
    dir->state_set(CDIR_STATE_EXPORT);
    dir->get(CDir::PIN_EXPORT);
  }
  dout(10) << "  base " << *dir << endl;
  if (dir != im)
    dout(10) << "  under " << *im << endl;
    
  // bounds (there were exports, before)
  for (set<inodeno_t>::iterator p = bounds.begin();
       p != bounds.end();
       ++p) {
    CInode *bi = get_inode(*p);
    assert(bi);
    CDir *bd = bi->dir;
    assert(bd);
    
    // hose export
    assert(exports.count(bd));
    exports.erase(bd);
    nested_exports[im].erase(bd);
    
    // fix dir_auth
    assert(bd->get_dir_auth() != CDIR_AUTH_PARENT);
    bd->set_dir_auth( CDIR_AUTH_PARENT );  // not me

    bd->state_clear(CDIR_STATE_EXPORT);
    bd->put(CDir::PIN_EXPORT);

    dout(10) << "  bound " << *bd << endl;
  }
  
  show_imports();
}




/*
 * rejoin phase!
 * we start out by sending rejoins to everyone in the recovery set.
 *
 * if _were_ are rejoining, send for all regions in our cache.
 * if we are active|stopping, send only to nodes that are are rejoining.
 */
void MDCache::send_cache_rejoins()
{
  dout(10) << "send_cache_rejoins " << endl;

  map<int, MMDSCacheRejoin*> rejoins;

  // if i am rejoining, send a rejoin to everyone.
  // otherwise, just send to others who are rejoining.
  for (set<int>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) {
    if (*p == mds->get_nodeid())  continue;  // nothing to myself!
    if (mds->is_rejoin() ||
	mds->mdsmap->is_rejoin(*p))
      rejoins[*p] = new MMDSCacheRejoin;
  }	

  // build list of dir_auth regions
  list<CDir*> dir_auth_regions;
  for (hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       ++p) {
    if (!p->second->is_dir()) continue;
    if (!p->second->dir) continue;
    if (p->second->dir->get_dir_auth() == CDIR_AUTH_PARENT) continue;

    int auth = p->second->dir->get_dir_auth();
    assert(auth >= 0);

    if (auth == mds->get_nodeid()) continue; // skip my own regions!
    
    if (rejoins.count(auth) == 0) 
      continue;   // don't care about this node's regions
    
    // add to list
    dout(10) << " on mds" << auth << " region " << *p->second << endl;
    dir_auth_regions.push_back(p->second->dir);
  }

  // walk the regions
  for (list<CDir*>::iterator p = dir_auth_regions.begin();
       p != dir_auth_regions.end();
       ++p) {
    CDir *dir = *p;
    int to = dir->authority();
    cache_rejoin_walk(dir, rejoins[to]);
  }

  // send the messages
  assert(rejoin_ack_gather.empty());
  for (map<int,MMDSCacheRejoin*>::iterator p = rejoins.begin();
       p != rejoins.end();
       ++p) {
    mds->send_message_mds(p->second, p->first, MDS_PORT_CACHE);
    rejoin_ack_gather.insert(p->first);
  }

  // nothing?
  if (rejoins.empty()) {
    dout(10) << "nothing to rejoin, going active" << endl;
    mds->set_want_state(MDSMap::STATE_ACTIVE);
  }
}



void MDCache::cache_rejoin_walk(CDir *dir, MMDSCacheRejoin *rejoin)
{
  dout(10) << "cache_rejoin_walk " << *dir << endl;
  rejoin->add_dir(dir->ino());

  list<CDir*> nested;  // finish this dir, then do nested items
  
  // walk dentries
  for (map<string,CDentry*>::iterator p = dir->items.begin();
       p != dir->items.end();
       ++p) {
    // dentry
    rejoin->add_dentry(dir->ino(), p->first);
    
    // inode?
    if (p->second->is_primary() && p->second->get_inode()) {
      CInode *in = p->second->get_inode();
      rejoin->add_inode(in->ino(), 
			in->get_caps_wanted());
      
      // dir?
      if (in->dir &&
	  in->dir->get_dir_auth() == CDIR_AUTH_PARENT)
	nested.push_back(in->dir);
    }
  }

  // recurse into nested dirs
  for (list<CDir*>::iterator p = nested.begin(); 
       p != nested.end();
       ++p)
    cache_rejoin_walk(*p, rejoin);
}


/*
 * i got a rejoin.
 * 
 *  - reply with the lockstate
 *
 * if i am active|stopping, 
 *  - remove source from replica list for everything not referenced here.
 */
void MDCache::handle_cache_rejoin(MMDSCacheRejoin *m)
{
  dout(7) << "handle_cache_rejoin from " << m->get_source() << endl;
  int from = m->get_source().num();

  MMDSCacheRejoinAck *ack = new MMDSCacheRejoinAck;

  if (mds->is_active() || mds->is_stopping()) {
    dout(10) << "removing stale cache replicas" << endl;
    // first, scour cache of replica references
    for (hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
	 p != inode_map.end();
	 ++p) {
      // inode
      CInode *in = p->second;
      if (in->is_replica(from) && m->inodes.count(p->first) == 0) {
	inode_remove_replica(in, from);
	dout(10) << " rem " << *in << endl;
      }

      // dentry
      if (in->parent) {
	CDentry *dn = in->parent;
	if (dn->is_replica(from) &&
	    (m->dentries.count(dn->get_dir()->ino()) == 0 ||
	     m->dentries[dn->get_dir()->ino()].count(dn->get_name()) == 0)) {
	  dn->remove_replica(from);
	  dout(10) << " rem " << *dn << endl;
	}
      }

      // dir
      if (in->dir) {
	CDir *dir = in->dir;
	if (dir->is_replica(from) && m->dirs.count(p->first) == 0) {
	  dir->remove_replica(from);
	  dout(10) << " rem " << *dir << endl;
	}
      }
    }
  } else {
    assert(mds->is_rejoin());
  }

  // dirs
  for (set<inodeno_t>::iterator p = m->dirs.begin();
       p != m->dirs.end();
       ++p) {
    CInode *diri = get_inode(*p);
    assert(diri);
    CDir *dir = diri->dir;
    assert(dir);
    int nonce = dir->add_replica(from);
    dout(10) << " has " << *dir << endl;
    ack->add_dir(*p, nonce);
    
    // dentries
    for (set<string>::iterator q = m->dentries[*p].begin();
	 q != m->dentries[*p].end();
	 ++q) {
      CDentry *dn = dir->lookup(*q);
      assert(dn);
      int nonce = dn->add_replica(from);
      dout(10) << " has " << *dn << endl;
      ack->add_dentry(*p, *q, dn->get_lockstate(), nonce);
    }
  }

  // inodes
  for (map<inodeno_t,int>::iterator p = m->inodes.begin();
       p != m->inodes.end();
       ++p) {
    CInode *in = get_inode(p->first);
    assert(in);
    int nonce = in->add_replica(from);
    if (p->second)
      in->mds_caps_wanted[from] = p->second;
    else
      in->mds_caps_wanted.erase(from);
    in->hardlock.gather_set.erase(from);  // just in case
    in->filelock.gather_set.erase(from);  // just in case
    dout(10) << " has " << *in << endl;
    ack->add_inode(p->first, 
		   in->hardlock.get_replica_state(), in->filelock.get_replica_state(), 
		   nonce);
  }

  // send ack
  mds->send_message_mds(ack, from, MDS_PORT_CACHE);
  
  delete m;
}


void MDCache::handle_cache_rejoin_ack(MMDSCacheRejoinAck *m)
{
  dout(7) << "handle_cache_rejoin from " << m->get_source() << endl;
  int from = m->get_source().num();
  
  // dirs
  for (list<MMDSCacheRejoinAck::dirinfo>::iterator p = m->dirs.begin();
       p != m->dirs.end();
       ++p) {
    CInode *diri = get_inode(p->dirino);
    CDir *dir = diri->dir;
    assert(dir);

    dir->set_replica_nonce(p->nonce);
    dout(10) << " got " << *dir << endl;

    // dentries
    for (map<string,MMDSCacheRejoinAck::dninfo>::iterator q = m->dentries[p->dirino].begin();
	 q != m->dentries[p->dirino].end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first);
      assert(dn);
      dn->set_replica_nonce(q->second.nonce);
      dn->set_lockstate(q->second.lock);
      dout(10) << " got " << *dn << endl;
    }
  }

  // inodes
  for (list<MMDSCacheRejoinAck::inodeinfo>::iterator p = m->inodes.begin();
       p != m->inodes.end();
       ++p) {
    CInode *in = get_inode(p->ino);
    assert(in);
    in->set_replica_nonce(p->nonce);
    in->hardlock.set_state(p->hardlock);
    in->filelock.set_state(p->filelock);
    dout(10) << " got " << *in << endl;
  }

  delete m;

  // done?
  rejoin_ack_gather.erase(from);
  if (rejoin_ack_gather.empty()) {
    dout(7) << "all done, going active!" << endl;
    show_imports();
    show_cache();
    mds->set_want_state(MDSMap::STATE_ACTIVE);
  } else {
    dout(7) << "still need rejoin_ack from " << rejoin_ack_gather << endl;
  }

}





// ===============================================================================

void MDCache::rename_file(CDentry *srcdn, 
                          CDentry *destdn)
{
  CInode *in = srcdn->inode;

  // unlink src
  srcdn->dir->unlink_inode(srcdn);
  
  // unlink old inode?
  if (destdn->inode) destdn->dir->unlink_inode(destdn);
  
  // link inode w/ dentry
  destdn->dir->link_inode( destdn, in );
}



void MDCache::set_root(CInode *in)
{
  assert(root == 0);
  root = in;
  root->state_set(CInode::STATE_ROOT);
}

void MDCache::add_import(CDir *dir)
{
  imports.insert(dir);
  dir->state_set(CDIR_STATE_IMPORT);
  dir->get(CDir::PIN_IMPORT);
}


void MDCache::recalc_auth_bits()
{
  dout(7) << "recalc_auth_bits" << endl;

  for (hash_map<inodeno_t,CInode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       ++p) {
    CInode *in = p->second;
    if (in->authority() == mds->get_nodeid())
      in->state_set(CInode::STATE_AUTH);
    else {
      in->state_clear(CInode::STATE_AUTH);
      if (in->is_dirty())
	in->mark_clean();
    }

    if (in->parent) {
      if (in->parent->authority() == mds->get_nodeid())
	in->parent->state_set(CDentry::STATE_AUTH);
      else {
	in->parent->state_clear(CDentry::STATE_AUTH);
	if (in->parent->is_dirty()) 
	  in->parent->mark_clean();
      }
    }

    if (in->dir) {
      if (in->dir->authority() == mds->get_nodeid())
	in->dir->state_set(CDIR_STATE_AUTH);
      else {
	in->dir->state_clear(CDIR_STATE_AUTH);
	if (in->dir->is_dirty()) 
	  in->dir->mark_clean();
      }
    }
  }
  show_imports();
  show_cache();
}





// **************
// Inode purging -- reliably removing deleted file's objects

class C_MDC_PurgeFinish : public Context {
  MDCache *mdc;
  inodeno_t ino;
public:
  C_MDC_PurgeFinish(MDCache *c, inodeno_t i) : mdc(c), ino(i) {}
  void finish(int r) {
    mdc->purge_inode_finish(ino);
  }
};
class C_MDC_PurgeFinish2 : public Context {
  MDCache *mdc;
  inodeno_t ino;
public:
  C_MDC_PurgeFinish2(MDCache *c, inodeno_t i) : mdc(c), ino(i) {}
  void finish(int r) {
    mdc->purge_inode_finish_2(ino);
  }
};

/* purge_inode in
 * will be called by on unlink or rmdir
 * caller responsible for journaling an appropriate EUnlink or ERmdir
 */
void MDCache::purge_inode(inode_t &inode)
{
  dout(10) << "purge_inode " << inode.ino << " size " << inode.size << endl;

  // take note
  assert(purging.count(inode.ino) == 0);
  purging[inode.ino] = inode;

  // remove
  mds->filer->remove(inode, 0, inode.size,
		     0, new C_MDC_PurgeFinish(this, inode.ino));
}

void MDCache::purge_inode_finish(inodeno_t ino)
{
  dout(10) << "purge_inode_finish " << ino << " - logging our completion" << endl;

  // log completion
  mds->mdlog->submit_entry(new EPurgeFinish(ino),
			   new C_MDC_PurgeFinish2(this, ino));
}

void MDCache::purge_inode_finish_2(inodeno_t ino)
{
  dout(10) << "purge_inode_finish_2 " << ino << endl;

  // remove from purging list
  purging.erase(ino);

  // tell anyone who cares (log flusher?)
  list<Context*> ls;
  ls.swap(waiting_for_purge[ino]);
  waiting_for_purge.erase(ino);
  finish_contexts(ls, 0);

  // reclaim ino?
  
}

void MDCache::start_recovered_purges()
{
  for (map<inodeno_t,inode_t>::iterator p = purging.begin();
       p != purging.end();
       ++p) {
    dout(10) << "start_recovered_purges " << p->first << " size " << p->second.size << endl;
    mds->filer->remove(p->second, 0, p->second.size,
		       0, new C_MDC_PurgeFinish(this, p->first));
  }
}



bool MDCache::trim(int max) 
{
  // trim LRU
  if (max < 0) {
    max = lru.lru_get_max();
    if (!max) return false;
  }
  dout(7) << "trim max=" << max << "  cur=" << lru.lru_get_size() << endl;

  map<int, MCacheExpire*> expiremap;

  while (lru.lru_get_size() > (unsigned)max) {
    CDentry *dn = (CDentry*)lru.lru_expire();
    if (!dn) break;
    
    CDir *dir = dn->get_dir();
    assert(dir);
    
    // notify dentry authority?
    if (!dn->is_auth()) {
      int auth = dn->authority();
      dout(17) << "sending expire to mds" << auth << " on " << *dn << endl;
      if (expiremap.count(auth) == 0) 
	expiremap[auth] = new MCacheExpire(mds->get_nodeid());
      expiremap[auth]->add_dentry(dir->ino(), dn->get_name(), dn->get_replica_nonce());
    }
    
    // unlink the dentry
    dout(15) << "trim removing " << *dn << endl;
    if (!dn->is_null()) 
      dir->unlink_inode(dn);
    dir->remove_dentry(dn);

    // adjust the dir state
    CInode *diri = dir->get_inode();
    diri->dir->state_clear(CDIR_STATE_COMPLETE);  // dir incomplete!

    // reexport?
    if (diri->dir->is_import() &&             // import
	diri->dir->get_size() == 0 &&         // no children
	!diri->is_root())                   // not root
      migrator->export_empty_import(diri->dir);
    
    if (mds->logger) mds->logger->inc("cex");
  }

  // inode expire_queue
  while (!inode_expire_queue.empty()) {
    CInode *in = inode_expire_queue.front();
    inode_expire_queue.pop_front();

    assert(in->get_num_ref() == 0);
    
    int dirauth = -2;
    if (in->dir) {
      // notify dir authority?
      dirauth = in->dir->authority();
      if (dirauth != mds->get_nodeid()) {
	dout(17) << "sending expire to mds" << dirauth << " on   " << *in->dir << endl;
	if (expiremap.count(dirauth) == 0) 
	  expiremap[dirauth] = new MCacheExpire(mds->get_nodeid());
	expiremap[dirauth]->add_dir(in->ino(), in->dir->replica_nonce);
      }

      in->close_dir();
    }
  
    // notify inode authority
    int auth = in->authority();
    if (auth == CDIR_AUTH_UNKNOWN) {
      assert(in->ino() == 1);
      assert(dirauth >= 0);
      auth = dirauth;
    }
    if (auth != mds->get_nodeid()) {
      assert(!in->is_auth());
      dout(17) << "sending expire to mds" << auth << " on " << *in << endl;
      if (expiremap.count(auth) == 0) 
	expiremap[auth] = new MCacheExpire(mds->get_nodeid());
      expiremap[auth]->add_inode(in->ino(), in->get_replica_nonce());
    } else {
      assert(in->is_auth());
    }

    dout(15) << "trim removing " << *in << endl;
    if (in == root) root = 0;
    remove_inode(in);
  }

  // send expires
  for (map<int, MCacheExpire*>::iterator it = expiremap.begin();
       it != expiremap.end();
       it++) {
    dout(7) << "sending cache_expire to " << it->first << endl;
    mds->send_message_mds(it->second, it->first, MDS_PORT_CACHE);
  }


  return true;
}


void MDCache::trim_non_auth()
{
  dout(7) << "trim_non_auth" << endl;
  
  CDentry *first_auth = 0;
  
  // trim non-auth items from the lru
  while (lru.lru_get_size() > 0) {
    CDentry *dn = (CDentry*)lru.lru_expire();
    if (!dn) break;

    if (dn->is_auth()) {
      // add back into lru (at the top)
      lru.lru_insert_top(dn);

      if (!first_auth) {
	first_auth = dn;
      } else {
	if (first_auth == dn) 
	  break;
      }
    } else {
      // non-auth.  expire.
      CDir *dir = dn->get_dir();
      assert(dir);

      // unlink the dentry
      dout(15) << "trim_non_auth removing " << *dn << endl;
      if (!dn->is_null()) 
	dir->unlink_inode(dn);
      dir->remove_dentry(dn);
      
      // adjust the dir state
      CInode *diri = dir->get_inode();
      diri->dir->state_clear(CDIR_STATE_COMPLETE);  // dir incomplete!
    }
  }

  // inode expire queue
  while (!inode_expire_queue.empty()) {
    CInode *in = inode_expire_queue.front();
    inode_expire_queue.pop_front();
    dout(15) << "trim_non_auth removing " << *in << endl;
    if (in == root) root = 0;
    remove_inode(in);
  }
}



class C_MDC_ShutdownCommit : public Context {
  MDCache *mdc;
public:
  C_MDC_ShutdownCommit(MDCache *mdc) {
    this->mdc = mdc;
  }
  void finish(int r) {
    mdc->shutdown_commits--;
  }
};

class C_MDC_ShutdownCheck : public Context {
  MDCache *mdc;
public:
  C_MDC_ShutdownCheck(MDCache *m) : mdc(m) {}
  void finish(int) {
    mdc->shutdown_check();
  }
};

void MDCache::shutdown_check()
{
  dout(0) << "shutdown_check at " << g_clock.now() << endl;

  // cache
  int o = g_conf.debug_mds;
  g_conf.debug_mds = 10;
  show_cache();
  g_conf.debug_mds = o;
  mds->timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  // this
  dout(0) << "lru size now " << lru.lru_get_size() << endl;
  dout(0) << "log len " << mds->mdlog->get_num_events() << endl;


  if (exports.size()) 
    dout(0) << "still have " << exports.size() << " exports" << endl;

  if (mds->filer->is_active()) 
    dout(0) << "filer still active" << endl;
}

void MDCache::shutdown_start()
{
  dout(1) << "shutdown_start" << endl;

  if (g_conf.mds_shutdown_check)
    mds->timer.add_event_after(g_conf.mds_shutdown_check, new C_MDC_ShutdownCheck(this));
}



bool MDCache::shutdown_pass()
{
  dout(7) << "shutdown_pass" << endl;
  //assert(mds->is_shutting_down());
  if (mds->is_out()) {
    dout(7) << " already shut down" << endl;
    show_cache();
    show_imports();
    return true;
  }

  // unhash dirs?
  if (!hashdirs.empty()) {
    // unhash any of my dirs?
    for (set<CDir*>::iterator it = hashdirs.begin();
         it != hashdirs.end();
         it++) {
      CDir *dir = *it;
      if (!dir->is_auth()) continue;
      if (dir->is_unhashing()) continue;
      migrator->unhash_dir(dir);
    }

    dout(7) << "waiting for dirs to unhash" << endl;
    return false;
  }

  // commit dirs?
  if (g_conf.mds_commit_on_shutdown) {
    
    if (shutdown_commits < 0) {
      dout(1) << "shutdown_pass committing all dirty dirs" << endl;
      shutdown_commits = 0;
      
      for (hash_map<inodeno_t, CInode*>::iterator it = inode_map.begin();
           it != inode_map.end();
           it++) {
        CInode *in = it->second;
        
        // commit any dirty dir that's ours
        if (in->is_dir() && in->dir && in->dir->is_auth() && in->dir->is_dirty()) {
          mds->mdstore->commit_dir(in->dir, new C_MDC_ShutdownCommit(this));
          shutdown_commits++;
        }
      }
    }

    // commits?
    if (shutdown_commits > 0) {
      dout(7) << "shutdown_commits still waiting for " << shutdown_commits << endl;
      return false;
    }
  }

  // flush anything we can from the cache
  trim(0);
  dout(5) << "lru size now " << lru.lru_get_size() << endl;

  mds->mdlog->trim(0);

  // (wait for) flush log?
  if (g_conf.mds_log_flush_on_shutdown) {
    if (mds->mdlog->get_non_importmap_events()) {
      dout(7) << "waiting for log to flush .. " << mds->mdlog->get_num_events() 
	      << " (" << mds->mdlog->get_non_importmap_events() << ")" << endl;
      return false;
    } 
  }


  // send all imports back to 0.
  if (mds->get_nodeid() != 0 && !did_shutdown_exports) {
    // flush what i can from the cache first..
    trim(0);

    // export to root
    for (set<CDir*>::iterator it = imports.begin();
         it != imports.end();
         ) {
      CDir *im = *it;
      it++;
      if (im->inode->is_root()) continue;
      if (im->is_frozen() || im->is_freezing()) continue;
      
      dout(7) << "sending " << *im << " back to mds0" << endl;
      migrator->export_dir(im,0);
    }
    did_shutdown_exports = true;
  } 


  // waiting for imports?  (e.g. root?)
  if (exports.size()) {
    dout(7) << "still have " << exports.size() << " exports" << endl;
    //show_cache();
    return false;
  }

  
  // close root?
  if (mds->get_nodeid() == 0 &&
      lru.lru_get_size() == 0 &&
      root && 
      root->dir && 
      root->dir->is_import() &&
      root->dir->get_num_ref() == 1) {  // 1 is the import!
    // un-import
    dout(7) << "removing root import" << endl;
    imports.erase(root->dir);
    root->dir->state_clear(CDIR_STATE_IMPORT);
    root->dir->put(CDir::PIN_IMPORT);

    if (root->is_pinned_by(CInode::PIN_DIRTY)) {
      dout(7) << "clearing root inode dirty flag" << endl;
      root->put(CInode::PIN_DIRTY);
    }

    trim(0);
  }
  
  // imports?
  if (!imports.empty() || migrator->is_exporting()) {
    dout(7) << "still have " << imports.size() << " imports, or still exporting" << endl;
    show_cache();
    return false;
  }
  
  // cap log?
  if (g_conf.mds_log_flush_on_shutdown) {

    if (imports.empty() && exports.empty()) {
      // (only do this once!)
      if (!mds->mdlog->is_capped()) {
	dout(7) << "capping the log" << endl;
	mds->mdlog->cap();
	// note that this won't flush right away, so we'll make at least one more pass
      }
    }

    if (mds->mdlog->get_num_events()) {
      dout(7) << "waiting for log to flush (including import_map, now) .. " << mds->mdlog->get_num_events() 
	      << " (" << mds->mdlog->get_non_importmap_events() << ")" << endl;
      return false;
    }

    if (!did_shutdown_log_cap) {
      // flush journal header
      dout(7) << "writing header for (now-empty) journal" << endl;
      assert(mds->mdlog->empty());
      mds->mdlog->write_head(0);  
      // NOTE: filer active checker below will block us until this completes.
      did_shutdown_log_cap = true;
      return false;
    }
  }

  // filer active?
  if (mds->filer->is_active()) {
    dout(7) << "filer still active" << endl;
    return false;
  }


  // done?
  if (lru.lru_get_size() > 0) {
    dout(7) << "there's still stuff in the cache: " << lru.lru_get_size() << endl;
    show_cache();
    //dump();
    return false;
  } 
  
  // done!
  dout(1) << "shutdown done." << endl;
  return true;
}





CInode *MDCache::create_root_inode()
{
  CInode *root = new CInode(this);
  memset(&root->inode, 0, sizeof(inode_t));
  root->inode.ino = 1;
  root->inode.hash_seed = 0;   // not hashed!
  
  // make it up (FIXME)
  root->inode.mode = 0755 | INODE_MODE_DIR;
  root->inode.size = 0;
  root->inode.ctime = 0;
  root->inode.mtime = g_clock.gettime();
  
  root->inode.nlink = 1;
  root->inode.layout = g_OSD_MDDirLayout;
  
  set_root( root );
  add_inode( root );

  return root;
}


int MDCache::open_root(Context *c)
{
  int whoami = mds->get_nodeid();

  // open root inode
  if (whoami == 0) { 
    // i am root inode
    CInode *root = create_root_inode();

    // root directory too
    assert(root->dir == NULL);
    root->set_dir( new CDir(root, this, true) );
    root->dir->set_dir_auth( 0 );  // me!
    root->dir->dir_rep = CDIR_REP_ALL;   //NONE;

    // root is sort of technically an import (from a vacuum)
    imports.insert( root->dir );
    root->dir->state_set(CDIR_STATE_IMPORT);
    root->dir->get(CDir::PIN_IMPORT);

    if (c) {
      c->finish(0);
      delete c;
    }
  } else {
    // request inode from root mds
    if (waiting_for_root.empty()) {
      dout(7) << "discovering root" << endl;

      filepath want;
      MDiscover *req = new MDiscover(whoami,
                                     0,
                                     want,
                                     false);  // there _is_ no base dir for the root inode
      mds->send_message_mds(req, 0, MDS_PORT_CACHE);
    } else {
      dout(7) << "waiting for root" << endl;
    }    

    // wait
    waiting_for_root.push_back(c);

  }

  return 0;
}








// ========= messaging ==============


void MDCache::dispatch(Message *m)
{
  switch (m->get_type()) {

  case MSG_MDS_IMPORTMAP:
    handle_import_map((MMDSImportMap*)m);
    break;

  case MSG_MDS_CACHEREJOIN:
    handle_cache_rejoin((MMDSCacheRejoin*)m);
    break;
  case MSG_MDS_CACHEREJOINACK:
    handle_cache_rejoin_ack((MMDSCacheRejoinAck*)m);
    break;


  case MSG_MDS_DISCOVER:
    handle_discover((MDiscover*)m);
    break;
  case MSG_MDS_DISCOVERREPLY:
    handle_discover_reply((MDiscoverReply*)m);
    break;

    /*
  case MSG_MDS_INODEUPDATE:
    handle_inode_update((MInodeUpdate*)m);
    break;
    */

  case MSG_MDS_INODELINK:
    handle_inode_link((MInodeLink*)m);
    break;
  case MSG_MDS_INODELINKACK:
    handle_inode_link_ack((MInodeLinkAck*)m);
    break;

  case MSG_MDS_DIRUPDATE:
    handle_dir_update((MDirUpdate*)m);
    break;

  case MSG_MDS_CACHEEXPIRE:
    handle_cache_expire((MCacheExpire*)m);
    break;



  case MSG_MDS_DENTRYUNLINK:
    handle_dentry_unlink((MDentryUnlink*)m);
    break;


    

    
  default:
    dout(7) << "cache unknown message " << m->get_type() << endl;
    assert(0);
    break;
  }
}


/* path_traverse
 *
 * return values:
 *   <0 : traverse error (ENOTDIR, ENOENT)
 *    0 : success
 *   >0 : delayed or forwarded
 *
 * Notes:
 *   onfinish context is only needed if you specify MDS_TRAVERSE_DISCOVER _and_
 *   you aren't absolutely certain that the path actually exists.  If it doesn't,
 *   the context is needed to pass a (failure) result code.
 */

class C_MDC_TraverseDiscover : public Context {
  Context *onfinish, *ondelay;
 public:
  C_MDC_TraverseDiscover(Context *onfinish, Context *ondelay) {
    this->ondelay = ondelay;
    this->onfinish = onfinish;
  }
  void finish(int r) {
    //dout(10) << "TraverseDiscover r = " << r << endl;
    if (r < 0 && onfinish) {   // ENOENT on discover, pass back to caller.
      onfinish->finish(r);
    } else {
      ondelay->finish(r);      // retry as usual
    }
    delete onfinish;
    delete ondelay;
  }
};

int MDCache::path_traverse(filepath& origpath, 
                           vector<CDentry*>& trace, 
                           bool follow_trailing_symlink,
                           Message *req,
                           Context *ondelay,
                           int onfail,
                           Context *onfinish,
                           bool is_client_req)  // true if req is MClientRequest .. gross, FIXME
{
  int whoami = mds->get_nodeid();
  set< pair<CInode*, string> > symlinks_resolved; // keep a list of symlinks we touch to avoid loops

  bool noperm = false;
  if (onfail == MDS_TRAVERSE_DISCOVER ||
      onfail == MDS_TRAVERSE_DISCOVERXLOCK) noperm = true;

  // root
  CInode *cur = get_root();
  if (cur == NULL) {
    dout(7) << "traverse: i don't have root" << endl;
    open_root(ondelay);
    if (onfinish) delete onfinish;
    return 1;
  }

  // start trace
  trace.clear();

  // make our own copy, since we'll modify when we hit symlinks
  filepath path = origpath;  

  unsigned depth = 0;
  while (depth < path.depth()) {
    dout(12) << "traverse: path seg depth " << depth << " = " << path[depth] << endl;
    
    // ENOTDIR?
    if (!cur->is_dir()) {
      dout(7) << "traverse: " << *cur << " not a dir " << endl;
      delete ondelay;
      if (onfinish) {
        onfinish->finish(-ENOTDIR);
        delete onfinish;
      }
      return -ENOTDIR;
    }

    // open dir
    if (!cur->dir) {
      if (cur->dir_is_auth()) {
        // parent dir frozen_dir?
        if (cur->is_frozen_dir()) {
          dout(7) << "traverse: " << *cur->get_parent_dir() << " is frozen_dir, waiting" << endl;
          cur->get_parent_dir()->add_waiter(CDIR_WAIT_UNFREEZE, ondelay);
          if (onfinish) delete onfinish;
          return 1;
        }

        cur->get_or_open_dir(this);
        assert(cur->dir);
      } else {
        // discover dir from/via inode auth
        assert(!cur->is_auth());
        if (cur->waiting_for(CINODE_WAIT_DIR)) {
          dout(10) << "traverse: need dir for " << *cur << ", already doing discover" << endl;
        } else {
          filepath want = path.postfixpath(depth);
          dout(10) << "traverse: need dir for " << *cur << ", doing discover, want " << want.get_path() << endl;
          mds->send_message_mds(new MDiscover(mds->get_nodeid(),
					      cur->ino(),
					      want,
					      true),  // need this dir too
				cur->authority(), MDS_PORT_CACHE);
        }
        cur->add_waiter(CINODE_WAIT_DIR, ondelay);
        if (onfinish) delete onfinish;
        return 1;
      }
    }
    
    // frozen?
    /*
    if (cur->dir->is_frozen()) {
      // doh!
      // FIXME: traverse is allowed?
      dout(7) << "traverse: " << *cur->dir << " is frozen, waiting" << endl;
      cur->dir->add_waiter(CDIR_WAIT_UNFREEZE, ondelay);
      if (onfinish) delete onfinish;
      return 1;
    }
    */

    // must read directory hard data (permissions, x bit) to traverse
    if (!noperm && !mds->locker->inode_hard_read_try(cur, ondelay)) {
      if (onfinish) delete onfinish;
      return 1;
    }
    
    // check permissions?
    // XXX
    
    // ..?
    if (path[depth] == "..") {
      trace.pop_back();
      depth++;
      cur = cur->get_parent_inode();
      dout(10) << "traverse: following .. back to " << *cur << endl;
      continue;
    }


    // dentry
    CDentry *dn = cur->dir->lookup(path[depth]);

    // null and last_bit and xlocked by me?
    if (dn && dn->is_null() && 
        dn->is_xlockedbyme(req) &&
        depth == path.depth()-1) {
      dout(10) << "traverse: hit (my) xlocked dentry at tail of traverse, succeeding" << endl;
      trace.push_back(dn);
      break; // done!
    }

    if (dn && !dn->is_null()) {
      // dentry exists.  xlocked?
      if (!noperm && dn->is_xlockedbyother(req)) {
        dout(10) << "traverse: xlocked dentry at " << *dn << endl;
        cur->dir->add_waiter(CDIR_WAIT_DNREAD,
                             path[depth],
                             ondelay);
        if (onfinish) delete onfinish;
        return 1;
      }

      // do we have inode?
      if (!dn->inode) {
        assert(dn->is_remote());
        // do i have it?
        CInode *in = get_inode(dn->get_remote_ino());
        if (in) {
          dout(7) << "linking in remote in " << *in << endl;
          dn->link_remote(in);
        } else {
          dout(7) << "remote link to " << dn->get_remote_ino() << ", which i don't have" << endl;
          open_remote_ino(dn->get_remote_ino(), req,
                          ondelay);
          return 1;
        }        
      }

      // symlink?
      if (dn->inode->is_symlink() &&
          (follow_trailing_symlink || depth < path.depth()-1)) {
        // symlink, resolve!
        filepath sym = dn->inode->symlink;
        dout(10) << "traverse: hit symlink " << *dn->inode << " to " << sym << endl;

        // break up path components
        // /head/symlink/tail
        filepath head = path.prefixpath(depth);
        filepath tail = path.postfixpath(depth+1);
        dout(10) << "traverse: path head = " << head << endl;
        dout(10) << "traverse: path tail = " << tail << endl;
        
        if (symlinks_resolved.count(pair<CInode*,string>(dn->inode, tail.get_path()))) {
          dout(10) << "already hit this symlink, bailing to avoid the loop" << endl;
          return -ELOOP;
        }
        symlinks_resolved.insert(pair<CInode*,string>(dn->inode, tail.get_path()));

        // start at root?
        if (dn->inode->symlink[0] == '/') {
          // absolute
          trace.clear();
          depth = 0;
          path = tail;
          dout(10) << "traverse: absolute symlink, path now " << path << " depth " << depth << endl;
        } else {
          // relative
          path = head;
          path.append(sym);
          path.append(tail);
          dout(10) << "traverse: relative symlink, path now " << path << " depth " << depth << endl;
        }
        continue;        
      } else {
        // keep going.

        // forwarder wants replicas?
        if (is_client_req && ((MClientRequest*)req)->get_mds_wants_replica_in_dirino()) {
          dout(30) << "traverse: REP is here, " << ((MClientRequest*)req)->get_mds_wants_replica_in_dirino() << " vs " << cur->dir->ino() << endl;
          
          if (((MClientRequest*)req)->get_mds_wants_replica_in_dirino() == cur->dir->ino() &&
              cur->dir->is_auth() && 
              cur->dir->is_rep() &&
              cur->dir->is_replica(req->get_source().num()) &&
              dn->get_inode()->is_auth()
              ) {
            assert(req->get_source().is_mds());
            int from = req->get_source().num();
            
            if (dn->get_inode()->is_replica(from)) {
              dout(15) << "traverse: REP would replicate to mds" << from << ", but already cached_by " 
                       << req->get_source() << " dn " << *dn << endl; 
            } else {
              dout(10) << "traverse: REP replicating to " << req->get_source() << " dn " << *dn << endl;
              MDiscoverReply *reply = new MDiscoverReply(cur->dir->ino());
              reply->add_dentry( dn->replicate_to( from ) );
              reply->add_inode( dn->inode->replicate_to( from ) );
              mds->send_message_mds(reply, req->get_source().num(), MDS_PORT_CACHE);
            }
          }
        }
            
        trace.push_back(dn);
        cur = dn->inode;
        touch_inode(cur);
        depth++;
        continue;
      }
    }
    
    // MISS.  don't have it.

    int dauth = cur->dir->dentry_authority( path[depth] );
    dout(12) << "traverse: miss on dentry " << path[depth] << " dauth " << dauth << " in " << *cur->dir << endl;
    

    if (dauth == whoami) {
      // dentry is mine.
      if (cur->dir->is_complete()) {
        // file not found
        delete ondelay;
        if (onfinish) {
          onfinish->finish(-ENOENT);
          delete onfinish;
        }
        return -ENOENT;
      } else {
        
        //wrong?
        //if (onfail == MDS_TRAVERSE_DISCOVER) 
        //  return -1;
        
        // directory isn't complete; reload
        dout(7) << "traverse: incomplete dir contents for " << *cur << ", fetching" << endl;
        touch_inode(cur);
        mds->mdstore->fetch_dir(cur->dir, ondelay);
        
        if (mds->logger) mds->logger->inc("cmiss");

        if (onfinish) delete onfinish;
        return 1;
      }
    } else {
      // dentry is not mine.
      
      /* no, let's let auth handle the discovery/replication ..
      if (onfail == MDS_TRAVERSE_FORWARD && 
          onfinish == 0 &&   // no funnyness
          cur->dir->is_rep()) {
        dout(5) << "trying to discover in popular dir " << *cur->dir << endl;
        onfail = MDS_TRAVERSE_DISCOVER;
      }
      */

      if ((onfail == MDS_TRAVERSE_DISCOVER ||
           onfail == MDS_TRAVERSE_DISCOVERXLOCK)) {
        // discover

        filepath want = path.postfixpath(depth);
        if (cur->dir->waiting_for(CDIR_WAIT_DENTRY, path[depth])) {
          dout(7) << "traverse: already waiting for discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
        } else {
          dout(7) << "traverse: discover on " << *cur << " for " << want.get_path() << " to mds" << dauth << endl;
          
          touch_inode(cur);
        
          mds->send_message_mds(new MDiscover(mds->get_nodeid(),
					      cur->ino(),
					      want,
					      false),
				dauth, MDS_PORT_CACHE);
          if (mds->logger) mds->logger->inc("dis");
        }
        
        // delay processing of current request.
        //  delay finish vs ondelay until result of traverse, so that ENOENT can be 
        //  passed to onfinish if necessary
        cur->dir->add_waiter(CDIR_WAIT_DENTRY, 
                             path[depth], 
                             new C_MDC_TraverseDiscover(onfinish, ondelay));
        
        if (mds->logger) mds->logger->inc("cmiss");
        return 1;
      } 
      if (onfail == MDS_TRAVERSE_FORWARD) {
        // forward
        dout(7) << "traverse: not auth for " << path << " at " << path[depth] << ", fwd to mds" << dauth << endl;

        if (is_client_req && cur->dir->is_rep()) {
          dout(15) << "traverse: REP fw to mds" << dauth << ", requesting rep under " << *cur->dir << " req " << *(MClientRequest*)req << endl;
          ((MClientRequest*)req)->set_mds_wants_replica_in_dirino(cur->dir->ino());
          req->clear_payload();  // reencode!
        }

        mds->send_message_mds(req, dauth, req->get_dest_port());
        //show_imports();
        
        if (mds->logger) mds->logger->inc("cfw");
        if (onfinish) delete onfinish;
        delete ondelay;
        return 2;
      }    
      if (onfail == MDS_TRAVERSE_FAIL) {
        delete ondelay;
        if (onfinish) {
          onfinish->finish(-ENOENT);  // -ENOENT, but only because i'm not the authority!
          delete onfinish;
        }
        return -ENOENT;  // not necessarily exactly true....
      }
    }
    
    assert(0);  // i shouldn't get here
  }
  
  // success.
  delete ondelay;
  if (onfinish) {
    onfinish->finish(0);
    delete onfinish;
  }
  return 0;
}



void MDCache::open_remote_dir(CInode *diri,
                              Context *fin) 
{
  dout(10) << "open_remote_dir on " << *diri << endl;
  
  assert(diri->is_dir());
  assert(!diri->dir_is_auth());
  assert(!diri->is_auth());
  assert(diri->dir == 0);

  filepath want;  // no dentries, i just want the dir open
  mds->send_message_mds(new MDiscover(mds->get_nodeid(),
				      diri->ino(),
				      want,
				      true),  // need the dir open
			diri->authority(), MDS_PORT_CACHE);

  diri->add_waiter(CINODE_WAIT_DIR, fin);
}



class C_MDC_OpenRemoteInoLookup : public Context {
  MDCache *mdc;
  inodeno_t ino;
  Message *req;
  Context *onfinish;
public:
  vector<Anchor*> anchortrace;
  C_MDC_OpenRemoteInoLookup(MDCache *mdc, inodeno_t ino, Message *req, Context *onfinish) {
    this->mdc = mdc;
    this->ino = ino;
    this->req = req;
    this->onfinish = onfinish;
  }
  void finish(int r) {
    assert(r == 0);
    if (r == 0)
      mdc->open_remote_ino_2(ino, req, anchortrace, onfinish);
    else {
      onfinish->finish(r);
      delete onfinish;
    }
  }
};

void MDCache::open_remote_ino(inodeno_t ino,
                              Message *req,
                              Context *onfinish)
{
  dout(7) << "open_remote_ino on " << ino << endl;
  
  C_MDC_OpenRemoteInoLookup *c = new C_MDC_OpenRemoteInoLookup(this, ino, req, onfinish);
  mds->anchorclient->lookup(ino, c->anchortrace, c);
}

void MDCache::open_remote_ino_2(inodeno_t ino,
                                Message *req,
                                vector<Anchor*>& anchortrace,
                                Context *onfinish)
{
  dout(7) << "open_remote_ino_2 on " << ino << ", trace depth is " << anchortrace.size() << endl;
  
  // construct path
  filepath path;
  for (unsigned i=0; i<anchortrace.size(); i++) 
    path.add_dentry(anchortrace[i]->ref_dn);

  dout(7) << " path is " << path << endl;

  vector<CDentry*> trace;
  int r = path_traverse(path, trace, false,
                        req,
                        onfinish,  // delay actually
                        MDS_TRAVERSE_DISCOVER);
  if (r > 0) return;
  
  onfinish->finish(r);
  delete onfinish;
}




// path pins

bool MDCache::path_pin(vector<CDentry*>& trace,
                       Message *m,
                       Context *c)
{
  // verify everything is pinnable
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++) {
    CDentry *dn = *it;
    if (!dn->is_pinnable(m)) {
      // wait
      if (c) {
        dout(10) << "path_pin can't pin " << *dn << ", waiting" << endl;
        dn->dir->add_waiter(CDIR_WAIT_DNPINNABLE,   
                            dn->name,
                            c);
      } else {
        dout(10) << "path_pin can't pin, no waiter, failing." << endl;
      }
      return false;
    }
  }

  // pin!
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++) {
    (*it)->pin(m);
    dout(11) << "path_pinned " << *(*it) << endl;
  }

  delete c;
  return true;
}


void MDCache::path_unpin(vector<CDentry*>& trace,
                         Message *m)
{
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++) {
    CDentry *dn = *it;
    dn->unpin(m);
    dout(11) << "path_unpinned " << *dn << endl;

    // did we completely unpin a waiter?
    if (dn->lockstate == DN_LOCK_UNPINNING && !dn->get_num_ref()) {
      // return state to sync, in case the unpinner flails
      dn->lockstate = DN_LOCK_SYNC;

      // run finisher right now to give them a fair shot.
      dn->dir->finish_waiting(CDIR_WAIT_DNUNPINNED, dn->name);
    }
  }
}


void MDCache::make_trace(vector<CDentry*>& trace, CInode *in)
{
  CInode *parent = in->get_parent_inode();
  if (parent) {
    make_trace(trace, parent);

    CDentry *dn = in->get_parent_dn();
    dout(15) << "make_trace adding " << *dn << endl;
    trace.push_back(dn);
  }
}


bool MDCache::request_start(Message *req,
                            CInode *ref,
                            vector<CDentry*>& trace)
{
  assert(active_requests.count(req) == 0);

  // pin path
  if (trace.size()) {
    if (!path_pin(trace, req, new C_MDS_RetryMessage(mds,req))) return false;
  }

  dout(7) << "request_start " << *req << endl;

  // add to map
  active_requests[req].ref = ref;
  if (trace.size()) active_requests[req].traces[trace[trace.size()-1]] = trace;

  // request pins
  request_pin_inode(req, ref);
  
  if (mds->logger) mds->logger->inc("req");

  return true;
}


void MDCache::request_pin_inode(Message *req, CInode *in) 
{
  if (active_requests[req].request_pins.count(in) == 0) {
    in->request_pin_get();
    active_requests[req].request_pins.insert(in);
  }
}

void MDCache::request_pin_dir(Message *req, CDir *dir) 
{
  if (active_requests[req].request_dir_pins.count(dir) == 0) {
    dir->request_pin_get();
    active_requests[req].request_dir_pins.insert(dir);
  }
}


void MDCache::request_cleanup(Message *req)
{
  assert(active_requests.count(req) == 1);

  // leftover xlocks?
  if (active_requests[req].xlocks.size()) {
    set<CDentry*> dns = active_requests[req].xlocks;

    for (set<CDentry*>::iterator it = dns.begin();
         it != dns.end();
         it++) {
      CDentry *dn = *it;
      
      dout(7) << "request_cleanup leftover xlock " << *dn << endl;
      
      mds->locker->dentry_xlock_finish(dn);
      
      // queue finishers
      dn->dir->take_waiting(CDIR_WAIT_ANY, dn->name, mds->finished_queue);

      // remove clean, null dentry?  (from a failed rename or whatever)
      if (dn->is_null() && dn->is_sync() && !dn->is_dirty()) {
        dn->dir->remove_dentry(dn);
      }
    }
    
    assert(active_requests[req].xlocks.empty());  // we just finished finished them
  }

  // foreign xlocks?
  if (active_requests[req].foreign_xlocks.size()) {
    set<CDentry*> dns = active_requests[req].foreign_xlocks;
    active_requests[req].foreign_xlocks.clear();
    
    for (set<CDentry*>::iterator it = dns.begin();
         it != dns.end();
         it++) {
      CDentry *dn = *it;
      
      dout(7) << "request_cleanup sending unxlock for foreign xlock on " << *dn << endl;
      assert(dn->is_xlocked());
      int dauth = dn->dir->dentry_authority(dn->name);
      MLock *m = new MLock(LOCK_AC_UNXLOCK, mds->get_nodeid());
      m->set_dn(dn->dir->ino(), dn->name);
      mds->send_message_mds(m, dauth, MDS_PORT_CACHE);
    }
  }

  // unpin paths
  for (map< CDentry*, vector<CDentry*> >::iterator it = active_requests[req].traces.begin();
       it != active_requests[req].traces.end();
       it++) {
    path_unpin(it->second, req);
  }
  
  // request pins
  for (set<CInode*>::iterator it = active_requests[req].request_pins.begin();
       it != active_requests[req].request_pins.end();
       it++) {
    (*it)->request_pin_put();
  }
  for (set<CDir*>::iterator it = active_requests[req].request_dir_pins.begin();
       it != active_requests[req].request_dir_pins.end();
       it++) {
    (*it)->request_pin_put();
  }

  // remove from map
  active_requests.erase(req);


  // log some stats *****
  if (mds->logger) {
    mds->logger->set("c", lru.lru_get_size());
    mds->logger->set("cpin", lru.lru_get_num_pinned());
    mds->logger->set("ctop", lru.lru_get_top());
    mds->logger->set("cbot", lru.lru_get_bot());
    mds->logger->set("cptail", lru.lru_get_pintail());
    //mds->logger->set("buf",buffer_total_alloc);
  }

  if (g_conf.log_pins) {
    // pin
    /*
for (int i=0; i<CINODE_NUM_PINS; i++) {
      if (mds->logger2) mds->logger2->set(cinode_pin_names[i],
					  cinode_pins[i]);
    }
    */
    /*
      for (map<int,int>::iterator it = cdir_pins.begin();
      it != cdir_pins.end();
      it++) {
      //string s = "D";
      //s += cdir_pin_names[it->first];
      if (mds->logger2) mds->logger2->set(//s, 
      cdir_pin_names[it->first],
      it->second);
      }
    */
  }

}

void MDCache::request_finish(Message *req)
{
  dout(7) << "request_finish " << *req << endl;
  request_cleanup(req);
  delete req;  // delete req
  
  if (mds->logger) mds->logger->inc("reply");


  //dump();
}


void MDCache::request_forward(Message *req, int who, int port)
{
  if (!port) port = MDS_PORT_SERVER;

  dout(7) << "request_forward to " << who << " req " << *req << endl;
  request_cleanup(req);
  mds->send_message_mds(req, who, port);

  if (mds->logger) mds->logger->inc("fw");
}



// ANCHORS

class C_MDC_AnchorInode : public Context {
  CInode *in;
  
public:
  C_MDC_AnchorInode(CInode *in) {
    this->in = in;
  }
  void finish(int r) {
    if (r == 0) {
      assert(in->inode.anchored == false);
      in->inode.anchored = true;

      in->state_clear(CInode::STATE_ANCHORING);
      in->put(CInode::PIN_ANCHORING);
      
      in->_mark_dirty(); // fixme
    }

    // trigger
    in->finish_waiting(CINODE_WAIT_ANCHORED, r);
  }
};

void MDCache::anchor_inode(CInode *in, Context *onfinish)
{
  assert(in->is_auth());

  // already anchoring?
  if (in->state_test(CInode::STATE_ANCHORING)) {
    dout(7) << "anchor_inode already anchoring " << *in << endl;

    // wait
    in->add_waiter(CINODE_WAIT_ANCHORED,
                   onfinish);

  } else {
    dout(7) << "anchor_inode anchoring " << *in << endl;

    // auth: do it
    in->state_set(CInode::STATE_ANCHORING);
    in->get(CInode::PIN_ANCHORING);
    
    // wait
    in->add_waiter(CINODE_WAIT_ANCHORED,
                   onfinish);
    
    // make trace
    vector<Anchor*> trace;
    in->make_anchor_trace(trace);
    
    // do it
    mds->anchorclient->create(in->ino(), trace, 
                           new C_MDC_AnchorInode( in ));
  }
}


void MDCache::handle_inode_link(MInodeLink *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  if (!in->is_auth()) {
    assert(in->is_proxy());
    dout(7) << "handle_inode_link not auth for " << *in << ", fw to auth" << endl;
    mds->send_message_mds(m, in->authority(), MDS_PORT_CACHE);
    return;
  }

  dout(7) << "handle_inode_link on " << *in << endl;

  if (!in->is_anchored()) {
    assert(in->inode.nlink == 1);
    dout(7) << "needs anchor, nlink=" << in->inode.nlink << ", creating anchor" << endl;
    
    anchor_inode(in,
                 new C_MDS_RetryMessage(mds, m));
    return;
  }

  in->inode.nlink++;
  in->_mark_dirty(); // fixme

  // reply
  dout(7) << " nlink++, now " << in->inode.nlink++ << endl;

  mds->send_message_mds(new MInodeLinkAck(m->get_ino(), true), m->get_from(), MDS_PORT_CACHE);
  delete m;
}


void MDCache::handle_inode_link_ack(MInodeLinkAck *m) 
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  dout(7) << "handle_inode_link_ack success = " << m->is_success() << " on " << *in << endl;
  in->finish_waiting(CINODE_WAIT_LINK,
                     m->is_success() ? 1:-1);
}



// REPLICAS


void MDCache::handle_discover(MDiscover *dis) 
{
  int whoami = mds->get_nodeid();

  // from me to me?
  if (dis->get_asker() == whoami) {
    dout(7) << "discover for " << dis->get_want().get_path() << " bounced back to me, dropping." << endl;
    delete dis;
    return;
  }

  CInode *cur = 0;
  MDiscoverReply *reply = 0;
  //filepath fullpath;

  // get started.
  if (dis->get_base_ino() == 0) {
    // wants root
    dout(7) << "discover from mds" << dis->get_asker() << " wants root + " << dis->get_want().get_path() << endl;

    assert(mds->get_nodeid() == 0);
    assert(root->is_auth());

    //fullpath = dis->get_want();


    // add root
    reply = new MDiscoverReply(0);
    reply->add_inode( root->replicate_to( dis->get_asker() ) );
    dout(10) << "added root " << *root << endl;

    cur = root;
    
  } else {
    // there's a base inode
    cur = get_inode(dis->get_base_ino());
    assert(cur);

    if (dis->wants_base_dir()) {
      dout(7) << "discover from mds" << dis->get_asker() << " has " << *cur << " wants dir+" << dis->get_want().get_path() << endl;
    } else {
      dout(7) << "discover from mds" << dis->get_asker() << " has " << *cur->dir << " wants " << dis->get_want().get_path() << endl;
    }
    
    assert(cur->is_dir());
    
    // crazyness?
    if (!cur->dir && !cur->is_auth()) {
      int iauth = cur->authority();
      dout(7) << "no dir and not inode auth; fwd to auth " << iauth << endl;
      mds->send_message_mds( dis, iauth, MDS_PORT_CACHE);
      return;
    }

    // frozen_dir?
    if (!cur->dir && cur->is_frozen_dir()) {
      dout(7) << "is frozen_dir, waiting" << endl;
      cur->get_parent_dir()->add_waiter(CDIR_WAIT_UNFREEZE, 
                                        new C_MDS_RetryMessage(mds, dis));
      return;
    }

    if (!cur->dir) 
      cur->get_or_open_dir(this);
    assert(cur->dir);

    dout(10) << "dir is " << *cur->dir << endl;
    
    // create reply
    reply = new MDiscoverReply(cur->ino());
  }

  assert(reply);
  assert(cur);
  
  /*
  // first traverse and make sure we won't have to do any waiting
  dout(10) << "traversing full discover path = " << fullpath << endl;
  vector<CInode*> trav;
  int r = path_traverse(fullpath, trav, dis, MDS_TRAVERSE_FAIL);
  if (r > 0) 
    return;  // fw or delay
  dout(10) << "traverse finish w/o blocking, continuing" << endl;
  // ok, now we know we won't block on dentry locks or readdir.
  */


  // add content
  // do some fidgeting to include a dir if they asked for the base dir, or just root.
  for (unsigned i = 0; i < dis->get_want().depth() || dis->get_want().depth() == 0; i++) {
    // add dir
    if (reply->is_empty() && !dis->wants_base_dir()) {
      dout(7) << "they don't want the base dir" << endl;
    } else {
      // is it actaully a dir at all?
      if (!cur->is_dir()) {
        dout(7) << "not a dir " << *cur << endl;
        reply->set_flag_error_dir();
        break;
      }

      // add dir
      if (!cur->dir_is_auth()) {
        dout(7) << *cur << " dir auth is someone else, i'm done" << endl;
        break;
      }
      
      // did we hit a frozen_dir?
      if (!cur->dir && cur->is_frozen_dir()) {
        dout(7) << *cur << " is frozen_dir, stopping" << endl;
        break;
      }
      
      if (!cur->dir) cur->get_or_open_dir(this);
      
      reply->add_dir( new CDirDiscover( cur->dir, 
                                        cur->dir->add_replica( dis->get_asker() ) ) );
      dout(7) << "added dir " << *cur->dir << endl;
    }
    if (dis->get_want().depth() == 0) break;
    
    // lookup dentry
    int dentry_auth = cur->dir->dentry_authority( dis->get_dentry(i) );
    if (dentry_auth != mds->get_nodeid()) {
      dout(7) << *cur->dir << "dentry " << dis->get_dentry(i) << " auth " << dentry_auth << ", i'm done." << endl;
      break;      // that's it for us!
    }

    // get inode
    CDentry *dn = cur->dir->lookup( dis->get_dentry(i) );
    
    /*
    if (dn && !dn->can_read()) { // xlocked?
      dout(7) << "waiting on " << *dn << endl;
      cur->dir->add_waiter(CDIR_WAIT_DNREAD,
                           dn->name,
                           new C_MDS_RetryMessage(mds, dis));
      return;
    }
    */
    
    if (dn) {
      if (!dn->inode && dn->is_sync()) {
        dout(7) << "mds" << whoami << " dentry " << dis->get_dentry(i) << " null in " << *cur->dir << ", returning error" << endl;
        reply->set_flag_error_dn( dis->get_dentry(i) );
        break;   // don't replicate null but non-locked dentries.
      }
      
      reply->add_dentry( dn->replicate_to( dis->get_asker() ) );
      dout(7) << "added dentry " << *dn << endl;
      
      if (!dn->inode) break;  // we're done.
    }

    if (dn && dn->inode) {
        CInode *next = dn->inode;
        assert(next->is_auth());

        // add inode
        //int nonce = next->cached_by_add(dis->get_asker());
        reply->add_inode( next->replicate_to( dis->get_asker() ) );
        dout(7) << "added inode " << *next << endl;// " nonce=" << nonce<< endl;

        // descend
        cur = next;
    } else {
      // don't have inode?
      if (cur->dir->is_complete()) {
        // set error flag in reply
        dout(7) << "mds" << whoami << " dentry " << dis->get_dentry(i) << " not found in " << *cur->dir << ", returning error" << endl;
        reply->set_flag_error_dn( dis->get_dentry(i) );
        break;
      } else {
        // readdir
        dout(7) << "mds" << whoami << " incomplete dir contents for " << *cur->dir << ", fetching" << endl;

        //mds->mdstore->fetch_dir(cur->dir, NULL); //new C_MDS_RetryMessage(mds, dis));
        //break; // send what we have so far

        mds->mdstore->fetch_dir(cur->dir, new C_MDS_RetryMessage(mds, dis));
        return;
      }
    }
  }
       
  // how did we do.
  if (reply->is_empty()) {

    // discard empty reply
    delete reply;

    if ((cur->is_auth() || cur->is_proxy() || cur->dir->is_proxy()) &&
        !cur->dir->is_auth()) {
      // fwd to dir auth
      int dirauth = cur->dir->authority();
      if (dirauth == dis->get_asker()) {
        dout(7) << "from (new?) dir auth, dropping (obsolete) discover on floor." << endl;  // XXX FIXME is this right?
        //assert(dis->get_asker() == dis->get_source());  //might be a weird other loop.  either way, asker has it.
        delete dis;
      } else {
        dout(7) << "fwd to dir auth " << dirauth << endl;
        mds->send_message_mds( dis, dirauth, MDS_PORT_CACHE );
      }
      return;
    }
    
    dout(7) << "i'm not auth or proxy, dropping (this empty reply).  i bet i just exported." << endl;
    //assert(0);
    
  } else {
    // send back to asker
    dout(7) << "sending result back to asker mds" << dis->get_asker() << endl;
    mds->send_message_mds(reply, dis->get_asker(), MDS_PORT_CACHE);
  }

  // done.
  delete dis;
}


void MDCache::handle_discover_reply(MDiscoverReply *m) 
{
  // starting point
  CInode *cur;
  list<Context*> finished, error;
  
  if (m->has_root()) {
    // nowhere!
    dout(7) << "discover_reply root + " << m->get_path() << " " << m->get_num_inodes() << " inodes" << endl;
    assert(!root);
    assert(m->get_base_ino() == 0);
    assert(!m->has_base_dentry());
    assert(!m->has_base_dir());
    
    // add in root
    cur = new CInode(this, false);
      
    m->get_inode(0).update_inode(cur);
    
    // root
    set_root( cur );
    add_inode( cur );
    dout(7) << " got root: " << *cur << endl;

    // take waiters
    finished.swap(waiting_for_root);
  } else {
    // grab inode
    cur = get_inode(m->get_base_ino());
    
    if (!cur) {
      dout(7) << "discover_reply don't have base ino " << m->get_base_ino() << ", dropping" << endl;
      delete m;
      return;
    }
    
    dout(7) << "discover_reply " << *cur << " + " << m->get_path() << ", have " << m->get_num_inodes() << " inodes" << endl;
  }

  // fyi
  if (m->is_flag_error_dir()) dout(7) << " flag error, dir" << endl;
  if (m->is_flag_error_dn()) dout(7) << " flag error, dentry = " << m->get_error_dentry() << endl;
  dout(10) << "depth is " << m->get_depth() << ", has_root = " << m->has_root() << endl;
  
  // loop over discover results.
  // indexese follow each ([[dir] dentry] inode) 
  // can start, end with any type.
  
  for (int i=m->has_root(); i<m->get_depth(); i++) {
    dout(10) << "discover_reply i=" << i << " cur " << *cur << endl;

    // dir
    if ((i >  0) ||
        (i == 0 && m->has_base_dir())) {
      if (cur->dir) {
        // had it
        /* this is strange, but it happens when:
           we discover multiple dentries under a dir.
           bc, no flag to indicate a dir discover is underway, (as there is w/ a dentry one).
           this is actually good, since (dir aside) they're asking for different information.
        */
        dout(7) << "had " << *cur->dir;
        m->get_dir(i).update_dir(cur->dir);
        dout2(7) << ", now " << *cur->dir << endl;
      } else {
        // add it (_replica_)
        cur->set_dir( new CDir(cur, this, false) );
        m->get_dir(i).update_dir(cur->dir);
        dout(7) << "added " << *cur->dir << " nonce " << cur->dir->replica_nonce << endl;

        // get waiters
        cur->take_waiting(CINODE_WAIT_DIR, finished);
      }
    }    

    // dentry error?
    if (i == m->get_depth()-1 && 
        m->is_flag_error_dn()) {
      // error!
      assert(cur->is_dir());
      if (cur->dir) {
        dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dentry?" << endl;
        cur->dir->take_waiting(CDIR_WAIT_DENTRY,
                               m->get_error_dentry(),
                               error);
      } else {
        dout(7) << " flag_error on dentry " << m->get_error_dentry() << ", triggering dir?" << endl;
        cur->take_waiting(CINODE_WAIT_DIR, error);
      }
      break;
    }

    if (i >= m->get_num_dentries()) break;
    
    // dentry
    dout(7) << "i = " << i << " dentry is " << m->get_dentry(i).get_dname() << endl;

    CDentry *dn = 0;
    if (i > 0 || 
        m->has_base_dentry()) {
      dn = cur->dir->lookup( m->get_dentry(i).get_dname() );
      
      if (dn) {
        dout(7) << "had " << *dn << endl;
	dn->replica_nonce = m->get_dentry(i).get_nonce();  // fix nonce.
      } else {
        dn = cur->dir->add_dentry( m->get_dentry(i).get_dname(), 0, false );
	m->get_dentry(i).update_dentry(dn);
        dout(7) << "added " << *dn << endl;
      }

      cur->dir->take_waiting(CDIR_WAIT_DENTRY,
                             m->get_dentry(i).get_dname(),
                             finished);
    }
    
    if (i >= m->get_num_inodes()) break;

    // inode
    dout(7) << "i = " << i << " ino is " << m->get_ino(i) << endl;
    CInode *in = get_inode( m->get_inode(i).get_ino() );
    assert(dn);
    
    if (in) {
      dout(7) << "had " << *in << endl;
      
      // fix nonce
      dout(7) << " my nonce is " << in->replica_nonce << ", taking from discover, which  has " << m->get_inode(i).get_replica_nonce() << endl;
      in->replica_nonce = m->get_inode(i).get_replica_nonce();
      
      if (dn && in != dn->inode) {
        dout(7) << " but it's not linked via dentry " << *dn << endl;
        // link
        if (dn->inode) {
          dout(7) << "dentry WAS linked to " << *dn->inode << endl;
          assert(0);  // WTF.
        }
        dn->dir->link_inode(dn, in);
      }
    }
    else {
      assert(dn->inode == 0);  // better not be something else linked to this dentry...

      // didn't have it.
      in = new CInode(this, false);
      
      m->get_inode(i).update_inode(in);
        
      // link in
      add_inode( in );
      dn->dir->link_inode(dn, in);
      
      dout(7) << "added " << *in << " nonce " << in->replica_nonce << endl;
    }
    
    // onward!
    cur = in;
  }

  // dir error at the end there?
  if (m->is_flag_error_dir()) {
    dout(7) << " flag_error on dir " << *cur << endl;
    assert(!cur->is_dir());
    cur->take_waiting(CINODE_WAIT_DIR, error);
  }

  // finish errors directly
  finish_contexts(error, -ENOENT);

  mds->queue_finished(finished);

  // done
  delete m;
}








/*
int MDCache::send_inode_updates(CInode *in)
{
  assert(in->is_auth());
  for (set<int>::iterator it = in->cached_by_begin(); 
       it != in->cached_by_end(); 
       it++) {
    dout(7) << "sending inode_update on " << *in << " to " << *it << endl;
    assert(*it != mds->get_nodeid());
    mds->send_message_mds(new MInodeUpdate(in, in->get_cached_by_nonce(*it)), *it, MDS_PORT_CACHE);
  }

  return 0;
}


void MDCache::handle_inode_update(MInodeUpdate *m)
{
  inodeno_t ino = m->get_ino();
  CInode *in = get_inode(m->get_ino());
  if (!in) {
    //dout(7) << "inode_update on " << m->get_ino() << ", don't have it, ignoring" << endl;
    dout(7) << "inode_update on " << m->get_ino() << ", don't have it, sending expire" << endl;
    MCacheExpire *expire = new MCacheExpire(mds->get_nodeid());
    expire->add_inode(m->get_ino(), m->get_nonce());
    mds->send_message_mds(expire, m->get_source().num(), MDS_PORT_CACHE);
    goto out;
  }

  if (in->is_auth()) {
    dout(7) << "inode_update on " << *in << ", but i'm the authority!" << endl;
    assert(0); // this should never happen
  }
  
  dout(7) << "inode_update on " << *in << endl;

  // update! NOTE dir_auth is unaffected by this.
  in->decode_basic_state(m->get_payload());

 out:
  // done
  delete m;
}
*/



void MDCache::handle_cache_expire(MCacheExpire *m)
{
  int from = m->get_from();
  int source = m->get_source().num();
  map<int, MCacheExpire*> proxymap;
  
  if (m->get_from() == source) {
    dout(7) << "cache_expire from mds" << from << endl;
  } else {
    dout(7) << "cache_expire from mds" << from << " via " << source << endl;
  }

  // inodes
  for (map<inodeno_t,int>::iterator it = m->get_inodes().begin();
       it != m->get_inodes().end();
       it++) {
    CInode *in = get_inode(it->first);
    int nonce = it->second;
    
    if (!in) {
      dout(0) << "inode expire on " << it->first << " from " << from << ", don't have it" << endl;
      assert(in);  // i should be authority, or proxy .. and pinned
    }  
    if (!in->is_auth()) {
      int newauth = in->authority();
      dout(7) << "proxy inode expire on " << *in << " to " << newauth << endl;
      assert(newauth >= 0);
      if (!in->state_test(CInode::STATE_PROXY)) dout(0) << "missing proxy bit on " << *in << endl;
      assert(in->state_test(CInode::STATE_PROXY));
      if (proxymap.count(newauth) == 0) proxymap[newauth] = new MCacheExpire(from);
      proxymap[newauth]->add_inode(it->first, it->second);
      continue;
    }
    
    // check nonce
    if (from == mds->get_nodeid()) {
      // my cache_expire, and the export_dir giving auth back to me crossed paths!  
      // we can ignore this.  no danger of confusion since the two parties are both me.
      dout(7) << "inode expire on " << *in << " from mds" << from << " .. ME!  ignoring." << endl;
    } 
    else if (nonce == in->get_replica_nonce(from)) {
      // remove from our cached_by
      dout(7) << "inode expire on " << *in << " from mds" << from << " cached_by was " << in->get_replicas() << endl;
      inode_remove_replica(in, from);

    } 
    else {
      // this is an old nonce, ignore expire.
      dout(7) << "inode expire on " << *in << " from mds" << from
	      << " with old nonce " << nonce << " (current " << in->get_replica_nonce(from) << "), dropping" 
	      << endl;
      assert(in->get_replica_nonce(from) > nonce);
    }
  }

  // dirs
  for (map<inodeno_t,int>::iterator it = m->get_dirs().begin();
       it != m->get_dirs().end();
       it++) {
    CInode *diri = get_inode(it->first);
    assert(diri);
    CDir *dir = diri->dir;
    int nonce = it->second;
    
    if (!dir) {
      dout(0) << "dir expire on " << it->first << " from " << from << ", don't have it" << endl;
      assert(dir);  // i should be authority, or proxy ... and pinned
    }  
    if (!dir->is_auth()) {
      int newauth = dir->authority();
      dout(7) << "proxy dir expire on " << *dir << " to " << newauth << endl;
      if (!dir->is_proxy()) dout(0) << "nonproxy dir expire? " << *dir << " .. auth is " << newauth << " .. expire is from " << from << endl;
      assert(dir->is_proxy());
      assert(newauth >= 0);
      assert(dir->state_test(CDIR_STATE_PROXY));
      if (proxymap.count(newauth) == 0) proxymap[newauth] = new MCacheExpire(from);
      proxymap[newauth]->add_dir(it->first, it->second);
      continue;
    }
    
    // check nonce
    if (from == mds->get_nodeid()) {
      dout(7) << "dir expire on " << *dir << " from mds" << from
	      << " .. ME!  ignoring" << endl;
    } 
    else if (nonce == dir->get_replica_nonce(from)) {
      // remove from our cached_by
      dout(7) << "dir expire on " << *dir << " from mds" << from
	      << " replicas was " << dir->replicas << endl;
      dir->remove_replica(from);
    } 
    else {
      // this is an old nonce, ignore expire.
      dout(7) << "dir expire on " << *dir << " from mds" << from 
	      << " with old nonce " << nonce << " (current " << dir->get_replica_nonce(from)
	      << "), dropping" << endl;
      assert(dir->get_replica_nonce(from) > nonce);
    }
  }

  // dentries
  for (map<inodeno_t, map<string,int> >::iterator pd = m->get_dentries().begin();
       pd != m->get_dentries().end();
       ++pd) {
    dout(0) << "dn expires in dir " << pd->first << endl;
    CInode *diri = get_inode(pd->first);
    CDir *dir = diri->dir;
    assert(dir);

    if (!dir->is_auth()) {
      int newauth = dir->authority();
      dout(7) << "proxy dentry expires on " << *dir << " to " << newauth << endl;
      if (!dir->is_proxy()) 
	dout(0) << "nonproxy dentry expires? " << *dir << " .. auth is " << newauth
		<< " .. expire is from " << from << endl;
      assert(dir->is_proxy());
      assert(newauth >= 0);
      assert(dir->state_test(CDIR_STATE_PROXY));
      if (proxymap.count(newauth) == 0) proxymap[newauth] = new MCacheExpire(from);
      proxymap[newauth]->add_dentries(pd->first, pd->second);
      continue;
    }

    for (map<string,int>::iterator p = pd->second.begin();
	 p != pd->second.end();
	 ++p) {
      int nonce = p->second;

      CDentry *dn = dir->lookup(p->first);
      if (!dn) 
	dout(0) << "missing dentry for " << p->first << " in " << *dir << endl;
      assert(dn);
      
      if (from == mds->get_nodeid()) {
	dout(7) << "dentry_expire on " << *dn << " from mds" << from 
		<< " .. ME! ignoring" << endl;
      } 
      else if (nonce == dn->get_replica_nonce(from)) {
	dout(7) << "dentry_expire on " << *dn << " from mds" << from << endl;
	dn->remove_replica(from);
      } 
      else {
	dout(7) << "dentry_expire on " << *dn << " from mds" << from
		<< " with old nonce " << nonce << " (current " << dn->get_replica_nonce(from)
		<< "), dropping" << endl;
	assert(dn->get_replica_nonce(from) > nonce);
      }
    }
  }

  // send proxy forwards
  for (map<int, MCacheExpire*>::iterator it = proxymap.begin();
       it != proxymap.end();
       it++) {
    dout(7) << "sending proxy forward to " << it->first << endl;
    mds->send_message_mds(it->second, it->first, MDS_PORT_CACHE);
  }

  // done
  delete m;
}

void MDCache::inode_remove_replica(CInode *in, int from)
{
  in->remove_replica(from);
  in->mds_caps_wanted.erase(from);
  
  // note: this code calls _eval more often than it needs to!
  // fix lock
  if (in->hardlock.is_gathering(from)) {
    in->hardlock.gather_set.erase(from);
    if (in->hardlock.gather_set.size() == 0)
      mds->locker->inode_hard_eval(in);
  }
  if (in->filelock.is_gathering(from)) {
    in->filelock.gather_set.erase(from);
    if (in->filelock.gather_set.size() == 0)
      mds->locker->inode_file_eval(in);
  }
  
  // alone now?
  if (!in->is_replicated()) {
    mds->locker->inode_hard_eval(in);
    mds->locker->inode_file_eval(in);
  }
}


int MDCache::send_dir_updates(CDir *dir, bool bcast)
{
  // this is an FYI, re: replication

  set<int> who;
  if (bcast) {
    mds->get_mds_map()->get_active_mds_set(who);
  } else {
    for (map<int,int>::iterator p = dir->replicas_begin();
	 p != dir->replicas_end();
	 ++p)
      who.insert(p->first);
  }
  
  dout(7) << "sending dir_update on " << *dir << " bcast " << bcast << " to " << who << endl;

  string path;
  dir->inode->make_path(path);

  int whoami = mds->get_nodeid();
  for (set<int>::iterator it = who.begin();
       it != who.end();
       it++) {
    if (*it == whoami) continue;
    //if (*it == except) continue;
    dout(7) << "sending dir_update on " << *dir << " to " << *it << endl;

    mds->send_message_mds(new MDirUpdate(dir->ino(),
					 dir->dir_rep,
					 dir->dir_rep_by,
					 path,
					 bcast),
			  *it, MDS_PORT_CACHE);
  }

  return 0;
}


void MDCache::handle_dir_update(MDirUpdate *m)
{
  CInode *in = get_inode(m->get_ino());
  if (!in || !in->dir) {
    dout(5) << "dir_update on " << m->get_ino() << ", don't have it" << endl;

    // discover it?
    if (m->should_discover()) {
      m->tried_discover();  // only once!
      vector<CDentry*> trace;
      filepath path = m->get_path();

      dout(5) << "trying discover on dir_update for " << path << endl;

      int r = path_traverse(path, trace, true,
                            m, new C_MDS_RetryMessage(mds, m),
                            MDS_TRAVERSE_DISCOVER);
      if (r > 0)
        return;
      if (r == 0) {
        assert(in);
        open_remote_dir(in, new C_MDS_RetryMessage(mds, m));
        return;
      }
      assert(0);
    }

    goto out;
  }

  // update
  dout(5) << "dir_update on " << *in->dir << endl;
  in->dir->dir_rep = m->get_dir_rep();
  in->dir->dir_rep_by = m->get_dir_rep_by();
  
  // done
 out:
  delete m;
}





class C_MDC_DentryUnlink : public Context {
public:
  MDCache *mdc;
  CDentry *dn;
  CDir *dir;
  Context *c;
  C_MDC_DentryUnlink(MDCache *mdc, CDentry *dn, CDir *dir, Context *c) {
    this->mdc = mdc;
    this->dn = dn;
    this->dir = dir;
    this->c = c;
  }
  void finish(int r) {
    assert(r == 0);
    mdc->dentry_unlink_finish(dn, dir, c);
  }
};


// NAMESPACE FUN

void MDCache::dentry_unlink(CDentry *dn, Context *c)
{
  CDir *dir = dn->dir;
  string dname = dn->name;

  assert(dn->lockstate == DN_LOCK_XLOCK);

  // i need the inode to do any of this properly
  assert(dn->inode);

  // log it
  if (dn->inode) dn->inode->mark_unsafe();   // XXX ??? FIXME
  mds->mdlog->submit_entry(new EString("unlink fixme fixme"),//EUnlink(dir, dn, dn->inode),
                           NULL);    // FIXME FIXME FIXME

  // tell replicas
  if (dir->is_replicated()) {
    for (map<int,int>::iterator it = dir->replicas_begin();
         it != dir->replicas_end();
         it++) {
      dout(7) << "inode_unlink sending DentryUnlink to mds" << it->first << endl;
      
      mds->send_message_mds(new MDentryUnlink(dir->ino(), dn->name), it->first, MDS_PORT_CACHE);
    }

    // don't need ack.
  }


  // inode deleted?
  if (dn->is_primary()) {
    assert(dn->inode->is_auth());
    dn->inode->inode.nlink--;
    
    if (dn->inode->is_dir()) assert(dn->inode->inode.nlink == 0);  // no hard links on dirs

    // last link?
    if (dn->inode->inode.nlink == 0) {
      // truly dangling      
      if (dn->inode->dir) {
        // mark dir clean too, since it now dne!
        assert(dn->inode->dir->is_auth());
        dn->inode->dir->state_set(CDIR_STATE_DELETED);
        dn->inode->dir->remove_null_dentries();
        dn->inode->dir->mark_clean();
      }

      // mark it clean, it's dead
      if (dn->inode->is_dirty())
        dn->inode->mark_clean();
      
    } else {
      // migrate to inode file
      dout(7) << "removed primary, but there are remote links, moving to inode file: " << *dn->inode << endl;

      // dangling but still linked.  
      assert(dn->inode->is_anchored());

      // unlink locally
      CInode *in = dn->inode;
      dn->dir->unlink_inode( dn );
      dn->_mark_dirty(); // fixme

      // mark it dirty!
      in->_mark_dirty(); // fixme

      // update anchor to point to inode file+mds
      vector<Anchor*> atrace;
      in->make_anchor_trace(atrace);
      assert(atrace.size() == 1);   // it's dangling
      mds->anchorclient->update(in->ino(), atrace, 
                             new C_MDC_DentryUnlink(this, dn, dir, c));
      return;
    }
  }
  else if (dn->is_remote()) {
    // need to dec nlink on primary
    if (dn->inode->is_auth()) {
      // awesome, i can do it
      dout(7) << "remote target is local, nlink--" << endl;
      dn->inode->inode.nlink--;
      dn->inode->_mark_dirty(); // fixme

      if (( dn->inode->state_test(CInode::STATE_DANGLING) && dn->inode->inode.nlink == 0) ||
          (!dn->inode->state_test(CInode::STATE_DANGLING) && dn->inode->inode.nlink == 1)) {
        dout(7) << "nlink=1+primary or 0+dangling, removing anchor" << endl;

        // remove anchor (async)
        mds->anchorclient->destroy(dn->inode->ino(), NULL);
      }
    } else {
      int auth = dn->inode->authority();
      dout(7) << "remote target is remote, sending unlink request to " << auth << endl;

      mds->send_message_mds(new MInodeUnlink(dn->inode->ino(), mds->get_nodeid()),
			    auth, MDS_PORT_CACHE);

      // unlink locally
      CInode *in = dn->inode;
      dn->dir->unlink_inode( dn );
      dn->_mark_dirty(); // fixme

      // add waiter
      in->add_waiter(CINODE_WAIT_UNLINK, c);
      return;
    }
  }
  else 
    assert(0);   // unlink on null dentry??
 
  // unlink locally
  dn->dir->unlink_inode( dn );
  dn->_mark_dirty(); // fixme

  // finish!
  dentry_unlink_finish(dn, dir, c);
}


void MDCache::dentry_unlink_finish(CDentry *dn, CDir *dir, Context *c)
{
  dout(7) << "dentry_unlink_finish on " << *dn << endl;
  string dname = dn->name;

  // unpin dir / unxlock
  mds->locker->dentry_xlock_finish(dn, true); // quiet, no need to bother replicas since they're already unlinking
  
  // did i empty out an imported dir?
  if (dir->is_import() && !dir->inode->is_root() && dir->get_size() == 0) 
    migrator->export_empty_import(dir);

  // wake up any waiters
  dir->take_waiting(CDIR_WAIT_ANY, dname, mds->finished_queue);

  c->finish(0);
}




void MDCache::handle_dentry_unlink(MDentryUnlink *m)
{
  CInode *diri = get_inode(m->get_dirino());
  CDir *dir = 0;
  if (diri) dir = diri->dir;

  if (!diri || !dir) {
    dout(7) << "handle_dentry_unlink don't have dir " << m->get_dirino() << endl;
  }
  else {
    CDentry *dn = dir->lookup(m->get_dn());
    if (!dn) {
      dout(7) << "handle_dentry_unlink don't have dentry " << *dir << " dn " << m->get_dn() << endl;
    } else {
      dout(7) << "handle_dentry_unlink on " << *dn << endl;
      
      // dir?
      if (dn->inode) {
        if (dn->inode->dir) {
          dn->inode->dir->state_set(CDIR_STATE_DELETED);
          dn->inode->dir->remove_null_dentries();
        }
      }
      
      string dname = dn->name;
      
      // unlink
      dn->dir->remove_dentry(dn);
      
      // wake up
      //dir->finish_waiting(CDIR_WAIT_DNREAD, dname);
      dir->take_waiting(CDIR_WAIT_DNREAD, dname, mds->finished_queue);
    }
  }

  delete m;
  return;
}


void MDCache::handle_inode_unlink(MInodeUnlink *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  // proxy?
  if (in->is_proxy()) {
    dout(7) << "handle_inode_unlink proxy on " << *in << endl;
    mds->send_message_mds(m, in->authority(), MDS_PORT_CACHE);
    return;
  }
  assert(in->is_auth());

  // do it.
  dout(7) << "handle_inode_unlink nlink=" << in->inode.nlink << " on " << *in << endl;
  assert(in->inode.nlink > 0);
  in->inode.nlink--;

  if (in->state_test(CInode::STATE_DANGLING)) {
    // already dangling.
    // last link?
    if (in->inode.nlink == 0) {
      dout(7) << "last link, marking clean and removing anchor" << endl;
      
      in->mark_clean();       // mark it clean.
      
      // remove anchor (async)
      mds->anchorclient->destroy(in->ino(), NULL);
    }
    else {
      in->_mark_dirty(); // fixme
    }
  } else {
    // has primary link still.
    assert(in->inode.nlink >= 1);
    in->_mark_dirty(); // fixme

    if (in->inode.nlink == 1) {
      dout(7) << "nlink=1, removing anchor" << endl;
      
      // remove anchor (async)
      mds->anchorclient->destroy(in->ino(), NULL);
    }
  }

  // ack
  mds->send_message_mds(new MInodeUnlinkAck(m->get_ino()), m->get_from(), MDS_PORT_CACHE);
}

void MDCache::handle_inode_unlink_ack(MInodeUnlinkAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  dout(7) << "handle_inode_unlink_ack on " << *in << endl;
  in->finish_waiting(CINODE_WAIT_UNLINK, 0);
}










/*
 * some import/export helpers
 */

/** con = get_auth_container(dir)
 * Returns the directory in which authority is delegated for *dir.  
 * This may be because a directory is an import, or because it is hashed
 * and we are nested underneath an inode in that dir (that hashes to us).
 * Thus do not assume result->is_auth()!  It is_auth() || is_hashed().
 */
CDir *MDCache::get_auth_container(CDir *dir)
{
  CDir *imp = dir;  // might be *dir

  // find the underlying import or hash that delegates dir
  while (true) {
    if (imp->is_import()) break; // import
    imp = imp->get_parent_dir();
    if (!imp) break;             // none
    if (imp->is_hashed()) break; // hash
  }

  return imp;
}

CDir *MDCache::get_export_container(CDir *dir)
{
  CDir *ex = dir;  // might be *dir
  assert(!ex->is_auth());
  
  // find the underlying import or hash that delegates dir away
  while (true) {
    if (ex->is_export()) break; // import
    ex = ex->get_parent_dir();
    assert(ex);
    if (ex->is_hashed()) break; // hash
  }

  return ex;
}


void MDCache::find_nested_exports(CDir *dir, set<CDir*>& s) 
{
  CDir *import = get_auth_container(dir);
  find_nested_exports_under(import, dir, s);
}

void MDCache::find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s)
{
  dout(10) << "find_nested_exports for " << *dir << endl;
  dout(10) << "find_nested_exports_under import " << *import << endl;

  if (import == dir) {
    // yay, my job is easy!
    for (set<CDir*>::iterator p = nested_exports[import].begin();
         p != nested_exports[import].end();
         p++) {
      CDir *nested = *p;
      s.insert(nested);
      dout(10) << "find_nested_exports " << *dir << " " << *nested << endl;
    }
    return;
  }

  // ok, my job is annoying.
  for (set<CDir*>::iterator p = nested_exports[import].begin();
       p != nested_exports[import].end();
       p++) {
    CDir *nested = *p;
    
    dout(12) << "find_nested_exports checking " << *nested << endl;

    // trace back to import, or dir
    CDir *cur = nested->get_parent_dir();
    while (!cur->is_import() || cur == dir) {
      if (cur == dir) {
        s.insert(nested);
        dout(10) << "find_nested_exports " << *dir << " " << *nested << endl;
        break;
      } else {
        cur = cur->get_parent_dir();
      }
    }
  }
}


















// ==============================================================
// debug crap


void MDCache::show_imports()
{
  int db = 10;

  if (imports.empty() &&
      hashdirs.empty()) {
    dout(db) << "show_imports: no imports/exports/hashdirs" << endl;
    return;
  }
  dout(db) << "show_imports:" << endl;

  set<CDir*> ecopy = exports;

  set<CDir*>::iterator it = hashdirs.begin();
  while (1) {
    if (it == hashdirs.end()) it = imports.begin();
    if (it == imports.end() ) break;
    
    CDir *im = *it;
    
    if (im->is_import()) {
      dout(db) << "  + import (" << im->popularity[MDS_POP_CURDOM] << "/" << im->popularity[MDS_POP_ANYDOM] << ")  " << *im << endl;
      //assert( im->is_auth() );
    } 
    else if (im->is_hashed()) {
      if (im->is_import()) continue;  // if import AND hash, list as import.
      dout(db) << "  + hash (" << im->popularity[MDS_POP_CURDOM] << "/" << im->popularity[MDS_POP_ANYDOM] << ")  " << *im << endl;
    }
    
    for (set<CDir*>::iterator p = nested_exports[im].begin();
         p != nested_exports[im].end();
         p++) {
      CDir *exp = *p;
      if (exp->is_hashed()) {
        //assert(0);  // we don't do it this way actually
        dout(db) << "      - hash (" << exp->popularity[MDS_POP_NESTED] << ", " << exp->popularity[MDS_POP_ANYDOM] << ")  " << *exp << " to " << exp->dir_auth << endl;
        //assert( !exp->is_auth() );
      } else {
        dout(db) << "      - ex (" << exp->popularity[MDS_POP_NESTED] << ", " << exp->popularity[MDS_POP_ANYDOM] << ")  " << *exp << " to " << exp->dir_auth << endl;
        assert( exp->is_export() );
        //assert( !exp->is_auth() );
      }

      if ( get_auth_container(exp) != im ) {
        dout(1) << "uh oh, auth container is " << *get_auth_container(exp) << endl;
        assert( get_auth_container(exp) == im );
      }
      
      if (ecopy.count(exp) != 1) {
        dout(1) << "***** nested_export " << *exp << " not in exports" << endl;
        assert(0);
      }
      ecopy.erase(exp);
    }

    it++;
  }
  
  if (ecopy.size()) {
    for (set<CDir*>::iterator it = ecopy.begin();
         it != ecopy.end();
         it++) 
      dout(1) << "***** stray item in exports: " << **it << endl;
    assert(ecopy.size() == 0);
  }
}


void MDCache::show_cache()
{
  dout(7) << "show_cache" << endl;

  for (hash_map<inodeno_t,CInode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it++) {
    dout(7) << *((*it).second) << endl;
    
    CDentry *dn = (*it).second->get_parent_dn();
    if (dn) 
      dout(7) << "       dn " << *dn << endl;
    if ((*it).second->dir) 
      dout(7) << "   subdir " << *(*it).second->dir << endl;
  }
}

