

// =======================================================================
// HASHING


void Migrator::import_hashed_content(CDir *dir, bufferlist& bl, int nden, int oldauth)
{
  int off = 0;
  
  for (; nden>0; nden--) {
    // dentry
    string dname;
    _decode(dname, bl, off);
    dout(15) << "dname is " << dname << endl;
    
    char icode;
    bl.copy(off, 1, &icode);
    off++;
    
    CDentry *dn = dir->lookup(dname);
    if (!dn)
      dn = dir->add_dentry(dname);  // null
    
    // mark dn dirty _after_ we link the inode (scroll down)
    
    if (icode == 'N') {
      
      // null dentry
      assert(dn->is_null());  
      
      // fall thru
    }
    else if (icode == 'L') {
      // remote link
      inodeno_t ino;
      bl.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);
      dir->link_inode(dn, ino);
    }
    else if (icode == 'I') {
      // inode
      decode_import_inode(dn, bl, off, oldauth);
      
      // fix up subdir export?
      if (dn->inode->dir) {
        assert(dn->inode->dir->state_test(CDIR_STATE_IMPORTBOUND));
        dn->inode->dir->put(CDir::PIN_IMPORTBOUND);
        dn->inode->dir->state_clear(CDIR_STATE_IMPORTBOUND);

        if (dn->inode->dir->is_auth()) {
          // mine.  must have been an import.
          assert(dn->inode->dir->is_import());
          dout(7) << "unimporting subdir now that inode is mine " << *dn->inode->dir << endl;
          dn->inode->dir->set_dir_auth( CDIR_AUTH_PARENT );
          cache->imports.erase(dn->inode->dir);
          dn->inode->dir->put(CDir::PIN_IMPORT);
          dn->inode->dir->state_clear(CDIR_STATE_IMPORT);
          
          // move nested under hashdir
          for (set<CDir*>::iterator it = cache->nested_exports[dn->inode->dir].begin();
               it != cache->nested_exports[dn->inode->dir].end();
               it++) 
            cache->nested_exports[dir].insert(*it);
          cache->nested_exports.erase(dn->inode->dir);

          // now it matches the inode
          dn->inode->dir->set_dir_auth( CDIR_AUTH_PARENT );
        }
        else {
          // not mine.  make it an export.
          dout(7) << "making subdir into export " << *dn->inode->dir << endl;
          dn->inode->dir->get(CDir::PIN_EXPORT);
          dn->inode->dir->state_set(CDIR_STATE_EXPORT);
          cache->exports.insert(dn->inode->dir);
          cache->nested_exports[dir].insert(dn->inode->dir);
          
          if (dn->inode->dir->get_dir_auth().first == CDIR_AUTH_PARENT)
            dn->inode->dir->set_dir_auth( oldauth );          // no longer matches inode
          assert(dn->inode->dir->get_dir_auth().first >= 0);
        }
      }
    }
    
    // mark dentry dirty?  (only _after_ we link the inode!)
    dn->_mark_dirty(); // fixme
  }
}

/*
 
 notes on interaction of hashing and export/import:

  - dir->is_auth() is completely independent of hashing.  for a hashed dir,
     - all nodes are partially authoritative
     - all nodes dir->is_hashed() == true
     - all nodes dir->inode->dir_is_hashed() == true
     - one node dir->is_auth() == true, the rest == false
  - dir_auth for all subdirs in a hashed dir will (likely?) be explicit.

  - remember simple rule: dir auth follows inode, unless dir_auth is explicit.

  - export_dir_walk and decode_import_dir take care with dir_auth:   (for import/export)
     - on export, -1 is changed to mds->get_nodeid()
     - on import, nothing special, actually.

  - hashed dir files aren't included in export; subdirs are converted to imports 
    or exports as necessary.
  - hashed dir subdirs are discovered on export. this is important
    because dirs are needed to tie together auth hierarchy, for auth to know about
    imports/exports, etc.

  - dir state is maintained on auth.
    - COMPLETE and HASHED are transfered to importers.
    - DIRTY is set everywhere.

  - hashed dir is like an import: hashed dir used for nested_exports map.
    - nested_exports is updated appropriately on auth and replicas.
    - a subtree terminates as a hashed dir, since the hashing explicitly
      redelegates all inodes.  thus export_dir_walk includes hashed dirs, but 
      not their inodes.
*/

// HASH on auth

class C_MDC_HashFreeze : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_HashFreeze(Migrator *m, CDir *d) : mig(m), dir(d) {}
  virtual void finish(int r) {
    mig->hash_dir_frozen(dir);
  }
};

class C_MDC_HashComplete : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_HashComplete(Migrator *mig, CDir *dir) {
    this->mig = mig;
    this->dir = dir;
  }
  virtual void finish(int r) {
    mig->hash_dir_complete(dir);
  }
};


/** hash_dir(dir)
 * start hashing a directory.
 */
void Migrator::hash_dir(CDir *dir)
{
  dout(-7) << "hash_dir " << *dir << endl;

  assert(!dir->is_hashed());
  assert(dir->is_auth());
  
  if (dir->is_frozen() ||
      dir->is_freezing()) {
    dout(7) << " can't hash, freezing|frozen." << endl;
    return;
  }

  // pin path?
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  if (!cache->path_pin(trace, 0, 0)) {
    dout(7) << "hash_dir couldn't pin path, failing." << endl;
    return;
  }

  // ok, go
  dir->state_set(CDIR_STATE_HASHING);
  dir->get(CDir::PIN_HASHING);
  assert(dir->hashed_subset.empty());

  // discover on all mds
  assert(hash_gather.count(dir) == 0);
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;  // except me
    hash_gather[dir].insert(i);
    mds->send_message_mds(new MHashDirDiscover(dir->inode), i, MDS_PORT_MIGRATOR);
  }
  dir->auth_pin();  // pin until discovers are all acked.
  
  // start freeze
  dir->freeze_dir(new C_MDC_HashFreeze(this, dir));

  // make complete
  if (!dir->is_complete()) {
    dout(7) << "hash_dir " << *dir << " not complete, fetching" << endl;
    mds->mdstore->fetch_dir(dir,
                            new C_MDC_HashComplete(this, dir));
  } else
    hash_dir_complete(dir);
}


/*
 * wait for everybody to discover and open the hashing dir
 *  then auth_unpin, to let the freeze happen
 */
void Migrator::handle_hash_dir_discover_ack(MHashDirDiscoverAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  int from = m->get_source().num();
  assert(hash_gather[dir].count(from));
  hash_gather[dir].erase(from);
  
  if (hash_gather[dir].empty()) {
    hash_gather.erase(dir);
    dout(7) << "hash_dir_discover_ack " << *dir << ", releasing auth_pin" << endl;
    dir->auth_unpin();   // unpin to allow freeze to complete
  } else {
    dout(7) << "hash_dir_discover_ack " << *dir << ", still waiting for " << hash_gather[dir] << endl;
  }
  
  delete m;  // done
}



/*
 * once the dir is completely in memory,
 *  mark all migrating inodes dirty (to pin in cache)
 */
void Migrator::hash_dir_complete(CDir *dir)
{
  dout(7) << "hash_dir_complete " << *dir << ", dirtying inodes" << endl;

  assert(!dir->is_hashed());
  assert(dir->is_auth());
  
  // mark dirty to pin in cache
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CInode *in = it->second->inode;
    in->_mark_dirty(); // fixme
  }
  
  if (dir->is_frozen_dir())
    hash_dir_go(dir);
}


/*
 * once the dir is frozen,
 *  make sure it's complete
 *  send the prep messages!
 */
void Migrator::hash_dir_frozen(CDir *dir)
{
  dout(7) << "hash_dir_frozen " << *dir << endl;
  
  assert(!dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  
  if (!dir->is_complete()) {
    dout(7) << "hash_dir_frozen !complete, waiting still on " << *dir << endl;
    return;  
  }

  // send prep messages w/ export directories to open
  vector<MHashDirPrep*> msgs(mds->get_mds_map()->get_num_mds());

  // check for subdirs
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    CInode *in = dn->inode;
    
    if (!in->is_dir()) continue;
    if (!in->dir) continue;
    
    int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
    if (dentryhashcode == mds->get_nodeid()) continue;

    // msg?
    if (msgs[dentryhashcode] == 0) {
      msgs[dentryhashcode] = new MHashDirPrep(dir->ino());
    }
    msgs[dentryhashcode]->add_inode(it->first, in->replicate_to(dentryhashcode));
  }

  // send them!
  assert(hash_gather[dir].empty());
  for (unsigned i=0; i<msgs.size(); i++) {
    if (msgs[i]) {
      mds->send_message_mds(msgs[i], i, MDS_PORT_MIGRATOR);
      hash_gather[dir].insert(i);
    }
  }
  
  if (hash_gather[dir].empty()) {
    // no subdirs!  continue!
    hash_gather.erase(dir);
    hash_dir_go(dir);
  } else {
    // wait!
  }
}

/* 
 * wait for peers to open all subdirs
 */
void Migrator::handle_hash_dir_prep_ack(MHashDirPrepAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);

  int from = m->get_source().num();

  assert(hash_gather[dir].count(from) == 1);
  hash_gather[dir].erase(from);

  if (hash_gather[dir].empty()) {
    hash_gather.erase(dir);
    dout(7) << "handle_hash_dir_prep_ack on " << *dir << ", last one" << endl;
    hash_dir_go(dir);
  } else {
    dout(7) << "handle_hash_dir_prep_ack on " << *dir << ", waiting for " << hash_gather[dir] << endl;    
  }

  delete m;
}


/*
 * once the dir is frozen,
 *  make sure it's complete
 *  do the hashing!
 */
void Migrator::hash_dir_go(CDir *dir)
{
  dout(7) << "hash_dir_go " << *dir << endl;
  
  assert(!dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());

  // get messages to other nodes ready
  vector<MHashDir*> msgs(mds->get_mds_map()->get_num_mds());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    msgs[i] = new MHashDir(dir->ino());
  }

  // pick a hash seed.
  dir->inode->inode.hash_seed = 1;//dir->ino();

  // suck up all waiters
  C_Contexts *fin = new C_Contexts;
  list<Context*> waiting;
  dir->take_waiting(CDIR_WAIT_ANY, waiting);    // all dir waiters
  fin->take(waiting);
  
  // get containing import.  might be me.
  CDir *containing_import = cache->get_auth_container(dir);
  assert(containing_import != dir || dir->is_import());  

  // divy up contents
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    CInode *in = dn->inode;

    int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
    if (dentryhashcode == mds->get_nodeid()) {
      continue;      // still mine!
    }

    bufferlist *bl = msgs[dentryhashcode]->get_state_ptr();
    assert(bl);
    
    // -- dentry
    dout(7) << "hash_dir_go sending to " << dentryhashcode << " dn " << *dn << endl;
    _encode(it->first, *bl);
    
    // null dentry?
    if (dn->is_null()) {
      bl->append("N", 1);  // null dentry
      assert(dn->is_sync());
      continue;
    }

    if (dn->is_remote()) {
      // remote link
      bl->append("L", 1);  // remote link

      inodeno_t ino = dn->get_remote_ino();
      bl->append((char*)&ino, sizeof(ino));
      continue;
    }

    // primary link
    // -- inode
    bl->append("I", 1);    // inode dentry
    
    encode_export_inode(in, *bl, dentryhashcode);  // encode, and (update state for) export
    msgs[dentryhashcode]->inc_nden();
    
    if (dn->is_dirty()) 
      dn->mark_clean();

    // add to proxy
    hash_proxy_inos[dir].push_back(in);
    in->state_set(CInode::STATE_PROXY);
    in->get(CInode::PIN_PROXY);

    // fix up subdirs
    if (in->dir) {
      if (in->dir->is_auth()) {
        // mine.  make it into an import.
        dout(7) << "making subdir into import " << *in->dir << endl;
        in->dir->set_dir_auth( mds->get_nodeid() );
        cache->imports.insert(in->dir);
        in->dir->get(CDir::PIN_IMPORT);
        in->dir->state_set(CDIR_STATE_IMPORT);

        // fix nested bits
        for (set<CDir*>::iterator it = cache->nested_exports[containing_import].begin();
             it != cache->nested_exports[containing_import].end(); ) {
          CDir *ex = *it;  
          it++;
          if (cache->get_auth_container(ex) == in->dir) {
            dout(10) << "moving nested export " << *ex << endl;
            cache->nested_exports[containing_import].erase(ex);
            cache->nested_exports[in->dir].insert(ex);
          }
        }
      }
      else {
        // not mine.
        dout(7) << "un-exporting subdir that's being hashed away " << *in->dir << endl;
        assert(in->dir->is_export());
        in->dir->put(CDir::PIN_EXPORT);
        in->dir->state_clear(CDIR_STATE_EXPORT);
        cache->exports.erase(in->dir);
        cache->nested_exports[containing_import].erase(in->dir);
        if (in->dir->authority() == dentryhashcode)
          in->dir->set_dir_auth( CDIR_AUTH_PARENT );
        else
          in->dir->set_dir_auth( in->dir->authority() );
      }
    }
    
    // waiters
    list<Context*> waiters;
    in->take_waiting(CINODE_WAIT_ANY, waiters);
    fin->take(waiters);
  }

  // dir state
  dir->state_set(CDIR_STATE_HASHED);
  dir->get(CDir::PIN_HASHED);
  cache->hashdirs.insert(dir);
  dir->mark_dirty(dir->pre_dirty()); // fixme
  mds->mdlog->submit_entry(new EString("dirty dir fixme"));

  // inode state
  if (dir->inode->is_auth()) {
    dir->inode->_mark_dirty(); // fixme
    mds->mdlog->submit_entry(new EString("hash dirty fixme"));
  }

  // fix up nested_exports?
  if (containing_import != dir) {
    dout(7) << "moving nested exports under hashed dir" << endl;
    for (set<CDir*>::iterator it = cache->nested_exports[containing_import].begin();
         it != cache->nested_exports[containing_import].end(); ) {
      CDir *ex = *it;
      it++;
      if (cache->get_auth_container(ex) == dir) {
        dout(7) << " moving nested export under hashed dir: " << *ex << endl;
        cache->nested_exports[containing_import].erase(ex);
        cache->nested_exports[dir].insert(ex);
      } else {
        dout(7) << " NOT moving nested export under hashed dir: " << *ex << endl;
      }
    }
  }

  // send hash messages
  assert(hash_gather[dir].empty());
  assert(hash_notify_gather[dir].empty());
  assert(dir->hashed_subset.empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    // all nodes hashed locally..
    dir->hashed_subset.insert(i);

    if (i == mds->get_nodeid()) continue;

    // init hash_gather and hash_notify_gather sets
    hash_gather[dir].insert(i);
    
    assert(hash_notify_gather[dir][i].empty());
    for (int j=0; j<mds->get_mds_map()->get_num_mds(); j++) {
      if (j == mds->get_nodeid()) continue;
      if (j == i) continue;
      hash_notify_gather[dir][i].insert(j);
    }

    mds->send_message_mds(msgs[i], i, MDS_PORT_MIGRATOR);
  }

  // wait for all the acks.
}


void Migrator::handle_hash_dir_ack(MHashDirAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);

  assert(dir->is_hashed());
  assert(dir->is_hashing());

  int from = m->get_source().num();
  assert(hash_gather[dir].count(from) == 1);
  hash_gather[dir].erase(from);
  
  if (hash_gather[dir].empty()) {
    dout(7) << "handle_hash_dir_ack on " << *dir << ", last one" << endl;

    if (hash_notify_gather[dir].empty()) {
      dout(7) << "got notifies too, all done" << endl;
      hash_dir_finish(dir);
    } else {
      dout(7) << "waiting on notifies " << endl;
    }

  } else {
    dout(7) << "handle_hash_dir_ack on " << *dir << ", waiting for " << hash_gather[dir] << endl;    
  }

  delete m;
}


void Migrator::hash_dir_finish(CDir *dir)
{
  dout(7) << "hash_dir_finish finishing " << *dir << endl;
  assert(dir->is_hashed());
  assert(dir->is_hashing());
  
  // dir state
  hash_gather.erase(dir);
  dir->state_clear(CDIR_STATE_HASHING);
  dir->put(CDir::PIN_HASHING);
  dir->hashed_subset.clear();

  // unproxy inodes
  //  this _could_ happen sooner, on a per-peer basis, but no harm in waiting a few more seconds.
  for (list<CInode*>::iterator it = hash_proxy_inos[dir].begin();
       it != hash_proxy_inos[dir].end();
       it++) {
    CInode *in = *it;
    assert(in->state_test(CInode::STATE_PROXY));
    in->state_clear(CInode::STATE_PROXY);
    in->put(CInode::PIN_PROXY);
  }
  hash_proxy_inos.erase(dir);

  // unpin path
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  cache->path_unpin(trace, 0);

  // unfreeze
  dir->unfreeze_dir();

  show_imports();
  assert(hash_gather.count(dir) == 0);

  // stats
  //if (mds->logger) mds->logger->inc("nh", 1);

}




// HASH on auth and non-auth

void Migrator::handle_hash_dir_notify(MHashDirNotify *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  assert(dir->is_hashing());

  dout(5) << "handle_hash_dir_notify " << *dir << endl;
  int from = m->get_from();

  int source = m->get_source().num();
  if (dir->is_auth()) {
    // gather notifies
    assert(dir->is_hashed());
    
    assert(    hash_notify_gather[dir][from].count(source) );
    hash_notify_gather[dir][from].erase(source);
    
    if (hash_notify_gather[dir][from].empty()) {
      dout(7) << "last notify from " << from << endl;
      hash_notify_gather[dir].erase(from);

      if (hash_notify_gather[dir].empty()) {
        dout(7) << "last notify!" << endl;
        hash_notify_gather.erase(dir);
        
        if (hash_gather[dir].empty()) {
          dout(7) << "got acks too, all done" << endl;
          hash_dir_finish(dir);
        } else {
          dout(7) << "still waiting on acks from " << hash_gather[dir] << endl;
        }
      } else {
        dout(7) << "still waiting for notify gathers from " << hash_notify_gather[dir].size() << " others" << endl;
      }
    } else {
      dout(7) << "still waiting for notifies from " << from << " via " << hash_notify_gather[dir][from] << endl;
    }

    // delete msg
    delete m;
  } else {
    // update dir hashed_subset 
    assert(dir->hashed_subset.count(from) == 0);
    dir->hashed_subset.insert(from);
    
    // update open subdirs
    for (CDir_map_t::iterator it = dir->begin(); 
         it != dir->end(); 
         it++) {
      CDentry *dn = it->second;
      CInode *in = dn->get_inode();
      if (!in) continue;
      if (!in->dir) continue;
      
      int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
      if (dentryhashcode != from) continue;   // we'll import these in a minute
      
      if (in->dir->authority() != dentryhashcode)
        in->dir->set_dir_auth( in->dir->authority() );
      else
        in->dir->set_dir_auth( CDIR_AUTH_PARENT );
    }
    
    // remove from notify gather set
    assert(hash_gather[dir].count(from));
    hash_gather[dir].erase(from);

    // last notify?
    if (hash_gather[dir].empty()) {
      dout(7) << "gathered all the notifies, finishing hash of " << *dir << endl;
      hash_gather.erase(dir);
      
      dir->state_clear(CDIR_STATE_HASHING);
      dir->put(CDir::PIN_HASHING);
      dir->hashed_subset.clear();
    } else {
      dout(7) << "still waiting for notify from " << hash_gather[dir] << endl;
    }

    // fw notify to auth
    mds->send_message_mds(m, dir->authority(), MDS_PORT_MIGRATOR);
  }
}




// HASH on non-auth

/*
 * discover step:
 *  each peer needs to open up the directory and pin it before we start
 */
class C_MDC_HashDirDiscover : public Context {
  Migrator *mig;
  MHashDirDiscover *m;
public:
  vector<CDentry*> trace;
  C_MDC_HashDirDiscover(Migrator *mig, MHashDirDiscover *m) {
    this->mig = mig;
    this->m = m;
  }
  void finish(int r) {
    CInode *in = 0;
    if (r >= 0) {
      if (trace.size())
        in = trace[trace.size()-1]->get_inode();
      else
        in = mig->cache->get_root();
    }
    mig->handle_hash_dir_discover_2(m, in, r);
  }
};  

void Migrator::handle_hash_dir_discover(MHashDirDiscover *m)
{
  assert(m->get_source().num() != mds->get_nodeid());

  dout(7) << "handle_hash_dir_discover on " << m->get_path() << endl;

  // must discover it!
  C_MDC_HashDirDiscover *onfinish = new C_MDC_HashDirDiscover(this, m);
  filepath fpath(m->get_path());
  cache->path_traverse(fpath, onfinish->trace, true,
		       m, new C_MDS_RetryMessage(mds,m),       // on delay/retry
		       MDS_TRAVERSE_DISCOVER,
		       onfinish);  // on completion|error
}

void Migrator::handle_hash_dir_discover_2(MHashDirDiscover *m, CInode *in, int r)
{
  // yay!
  if (in) {
    dout(7) << "handle_hash_dir_discover_2 has " << *in << endl;
  }

  if (r < 0 || !in->is_dir()) {
    dout(7) << "handle_hash_dir_discover_2 failed to discover or not dir " << m->get_path() << ", NAK" << endl;
    assert(0);    // this shouldn't happen if the auth pins his path properly!!!! 
  }
  assert(in->is_dir());

  // is dir open?
  if (!in->dir) {
    dout(7) << "handle_hash_dir_discover_2 opening dir " << *in << endl;
    cache->open_remote_dir(in,
			   new C_MDS_RetryMessage(mds, m));
    return;
  }
  CDir *dir = in->dir;

  // pin dir, set hashing flag
  dir->state_set(CDIR_STATE_HASHING);
  dir->get(CDir::PIN_HASHING);
  assert(dir->hashed_subset.empty());
  
  // inode state
  dir->inode->inode.hash_seed = 1;// dir->ino();
  if (dir->inode->is_auth()) {
    dir->inode->_mark_dirty(); // fixme
    mds->mdlog->submit_entry(new EString("hash dirty fixme"));
  }

  // get gather set ready for notifies
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    if (i == dir->authority()) continue;
    hash_gather[dir].insert(i);
  }

  // reply
  dout(7) << " sending hash_dir_discover_ack on " << *dir << endl;
  mds->send_message_mds(new MHashDirDiscoverAck(dir->ino()),
			m->get_source().num(), MDS_PORT_MIGRATOR);
  delete m;
}

/*
 * prep step:
 *  peers need to open up all subdirs of the hashed dir
 */

void Migrator::handle_hash_dir_prep(MHashDirPrep *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_hash_dir_prep " << *dir << endl;

  if (!m->did_assim()) {
    m->mark_assim();  // only do this the first time!

    // assimilate dentry+inodes for exports
    for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
         it != m->get_inodes().end();
         it++) {
      CInode *in = cache->get_inode( it->second->get_ino() );
      if (in) {
        it->second->update_inode(in);
        dout(5) << " updated " << *in << endl;
      } else {
        in = new CInode(mds->mdcache, false);
        it->second->update_inode(in);
        cache->add_inode(in);
        
        // link 
        dir->add_dentry( it->first, in );
        dout(5) << "   added " << *in << endl;
      }

      // open!
      if (!in->dir) {
        dout(5) << "  opening nested export on " << *in << endl;
        cache->open_remote_dir(in,
			       new C_MDS_RetryMessage(mds, m));
      }
    }
  }

  // verify!
  int waiting_for = 0;
  for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
       it != m->get_inodes().end();
       it++) {
    CInode *in = cache->get_inode( it->second->get_ino() );
    assert(in);

    if (in->dir) {
      if (!in->dir->state_test(CDIR_STATE_IMPORTBOUND)) {
        dout(5) << "  pinning nested export " << *in->dir << endl;
        in->dir->get(CDir::PIN_IMPORTBOUND);
        in->dir->state_set(CDIR_STATE_IMPORTBOUND);
      } else {
        dout(5) << "  already pinned nested export " << *in << endl;
      }
    } else {
      dout(5) << "  waiting for nested export dir on " << *in << endl;
      waiting_for++;
    }
  }

  if (waiting_for) {
    dout(5) << "waiting for " << waiting_for << " dirs to open" << endl;
    return;
  } 

  // ack!
  mds->send_message_mds(new MHashDirPrepAck(dir->ino()),
			m->get_source().num(), MDS_PORT_MIGRATOR);
  
  // done.
  delete m;
}


/*
 * hash step:
 */

void Migrator::handle_hash_dir(MHashDir *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  assert(!dir->is_auth());
  assert(!dir->is_hashed());
  assert(dir->is_hashing());

  dout(5) << "handle_hash_dir " << *dir << endl;
  int oldauth = m->get_source().num();

  // content
  import_hashed_content(dir, m->get_state(), m->get_nden(), oldauth);

  // dir state
  dir->state_set(CDIR_STATE_HASHED);
  dir->get(CDir::PIN_HASHED);
  cache->hashdirs.insert(dir);
  dir->hashed_subset.insert(mds->get_nodeid());

  // dir is complete
  dir->mark_complete();
  dir->mark_dirty(dir->pre_dirty()); // fixme
  mds->mdlog->submit_entry(new EString("dirty dir fixme"));

  // commit
  mds->mdstore->commit_dir(dir, 0);
  
  // send notifies
  dout(7) << "sending notifies" << endl;
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    if (i == m->get_source().num()) continue;
    mds->send_message_mds(new MHashDirNotify(dir->ino(), mds->get_nodeid()),
			  i, MDS_PORT_MIGRATOR);
  }

  // ack
  dout(7) << "acking" << endl;
  mds->send_message_mds(new MHashDirAck(dir->ino()),
			m->get_source().num(), MDS_PORT_MIGRATOR);
  
  // done.
  delete m;

  show_imports();
}





// UNHASH on auth

class C_MDC_UnhashFreeze : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_UnhashFreeze(Migrator *m, CDir *d)  : mig(m), dir(d) {}
  virtual void finish(int r) {
    mig->unhash_dir_frozen(dir);
  }
};

class C_MDC_UnhashComplete : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_UnhashComplete(Migrator *m, CDir *d) : mig(m), dir(d) {}
  virtual void finish(int r) {
    mig->unhash_dir_complete(dir);
  }
};


void Migrator::unhash_dir(CDir *dir)
{
  dout(-7) << "unhash_dir " << *dir << endl;

  assert(dir->is_hashed());
  assert(!dir->is_unhashing());
  assert(dir->is_auth());
  assert(hash_gather.count(dir)==0);

  // pin path?
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  if (!cache->path_pin(trace, 0, 0)) {
    dout(7) << "unhash_dir couldn't pin path, failing." << endl;
    return;
  }

  // twiddle state
  dir->state_set(CDIR_STATE_UNHASHING);

  // first, freeze the dir.
  dir->freeze_dir(new C_MDC_UnhashFreeze(this, dir));

  // make complete
  if (!dir->is_complete()) {
    dout(7) << "unhash_dir " << *dir << " not complete, fetching" << endl;
    mds->mdstore->fetch_dir(dir,
                            new C_MDC_UnhashComplete(this, dir));
  } else
    unhash_dir_complete(dir);

}

void Migrator::unhash_dir_frozen(CDir *dir)
{
  dout(7) << "unhash_dir_frozen " << *dir << endl;
  
  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  
  if (!dir->is_complete()) {
    dout(7) << "unhash_dir_frozen !complete, waiting still on " << *dir << endl;
  } else
    unhash_dir_prep(dir);
}


/*
 * ask peers to freeze and complete hashed dir
 */
void Migrator::unhash_dir_prep(CDir *dir)
{
  dout(7) << "unhash_dir_prep " << *dir << endl;
  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  assert(dir->is_complete());

  if (!hash_gather[dir].empty()) return;  // already been here..freeze must have been instantaneous

  // send unhash prep to all peers
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    hash_gather[dir].insert(i);
    mds->send_message_mds(new MUnhashDirPrep(dir->ino()),
			  i, MDS_PORT_MIGRATOR);
  }
}

/* 
 * wait for peers to freeze and complete hashed dirs
 */
void Migrator::handle_unhash_dir_prep_ack(MUnhashDirPrepAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  int from = m->get_source().num();
  dout(7) << "handle_unhash_dir_prep_ack from " << from << " " << *dir << endl;

  if (!m->did_assim()) {
    m->mark_assim();  // only do this the first time!
    
    // assimilate dentry+inodes for exports
    for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
         it != m->get_inodes().end();
         it++) {
      CInode *in = cache->get_inode( it->second->get_ino() );
      if (in) {
        it->second->update_inode(in);
        dout(5) << " updated " << *in << endl;
      } else {
        in = new CInode(mds->mdcache, false);
        it->second->update_inode(in);
        cache->add_inode(in);
        
        // link 
        dir->add_dentry( it->first, in );
        dout(5) << "   added " << *in << endl;
      }
      
      // open!
      if (!in->dir) {
        dout(5) << "  opening nested export on " << *in << endl;
        cache->open_remote_dir(in,
			       new C_MDS_RetryMessage(mds, m));
      }
    }
  }
  
  // verify!
  int waiting_for = 0;
  for (map<string,CInodeDiscover*>::iterator it = m->get_inodes().begin();
       it != m->get_inodes().end();
       it++) {
    CInode *in = cache->get_inode( it->second->get_ino() );
    assert(in);
    
    if (in->dir) {
      if (!in->dir->state_test(CDIR_STATE_IMPORTBOUND)) {
        dout(5) << "  pinning nested export " << *in->dir << endl;
        in->dir->get(CDir::PIN_IMPORTBOUND);
        in->dir->state_set(CDIR_STATE_IMPORTBOUND);
      } else {
        dout(5) << "  already pinned nested export " << *in << endl;
      }
    } else {
      dout(5) << "  waiting for nested export dir on " << *in << endl;
      waiting_for++;
    }
  }
  
  if (waiting_for) {
    dout(5) << "waiting for " << waiting_for << " dirs to open" << endl;
    return;
  } 
  
  // ok, done with this PrepAck
  assert(hash_gather[dir].count(from) == 1);
  hash_gather[dir].erase(from);
  
  if (hash_gather[dir].empty()) {
    hash_gather.erase(dir);
    dout(7) << "handle_unhash_dir_prep_ack on " << *dir << ", last one" << endl;
    unhash_dir_go(dir);
  } else {
    dout(7) << "handle_unhash_dir_prep_ack on " << *dir << ", waiting for " << hash_gather[dir] << endl;    
  }
  
  delete m;
}


/*
 * auth:
 *  send out MHashDir's to peers
 */
void Migrator::unhash_dir_go(CDir *dir)
{
  dout(7) << "unhash_dir_go " << *dir << endl;
  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(dir->is_frozen_dir());
  assert(dir->is_complete());

  // send unhash prep to all peers
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;
    hash_gather[dir].insert(i);
    mds->send_message_mds(new MUnhashDir(dir->ino()),
			  i, MDS_PORT_MIGRATOR);
  }
}

/*
 * auth:
 *  assimilate unhashing content
 */
void Migrator::handle_unhash_dir_ack(MUnhashDirAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_ack " << *dir << endl;
  assert(dir->is_hashed());

  // assimilate content
  int from = m->get_source().num();
  import_hashed_content(dir, m->get_state(), m->get_nden(), from);
  delete m;

  // done?
  assert(hash_gather[dir].count(from));
  hash_gather[dir].erase(from);
  
  if (!hash_gather[dir].empty()) {
    dout(7) << "still waiting for unhash acks from " << hash_gather[dir] << endl;
    return;
  } 

  // done!
  
  // fix up nested_exports
  CDir *containing_import = cache->get_auth_container(dir);
  if (containing_import != dir) {
    for (set<CDir*>::iterator it = cache->nested_exports[dir].begin();
         it != cache->nested_exports[dir].end();
         it++) {
      dout(7) << "moving nested export out from under hashed dir : " << **it << endl;
      cache->nested_exports[containing_import].insert(*it);
    }
    cache->nested_exports.erase(dir);
  }
  
  // dir state
  //dir->state_clear(CDIR_STATE_UNHASHING); //later
  dir->state_clear(CDIR_STATE_HASHED);
  dir->put(CDir::PIN_HASHED);
  cache->hashdirs.erase(dir);
  
  // commit!
  assert(dir->is_complete());
  //dir->mark_complete();
  dir->mark_dirty(dir->pre_dirty()); // fixme
  mds->mdstore->commit_dir(dir, 0);

  // inode state
  dir->inode->inode.hash_seed = 0;
  if (dir->inode->is_auth()) {
    dir->inode->_mark_dirty(); // fixme
    mds->mdlog->submit_entry(new EString("hash inode dirty fixme"));
  }
  
  // notify
  assert(hash_gather[dir].empty());
  for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
    if (i == mds->get_nodeid()) continue;

    hash_gather[dir].insert(i);
    
    mds->send_message_mds(new MUnhashDirNotify(dir->ino()),
			  i, MDS_PORT_MIGRATOR);
  }
}


/*
 * sent by peer to flush mds links.  unfreeze when all gathered.
 */
void Migrator::handle_unhash_dir_notify_ack(MUnhashDirNotifyAck *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_ack " << *dir << endl;
  assert(!dir->is_hashed());
  assert(dir->is_unhashing());
  assert(dir->is_frozen_dir());

  // done?
  int from = m->get_source().num();
  assert(hash_gather[dir].count(from));
  hash_gather[dir].erase(from);
  delete m;

  if (!hash_gather[dir].empty()) {
    dout(7) << "still waiting for notifyack from " << hash_gather[dir] << " on " << *dir << endl;
  } else {
    unhash_dir_finish(dir);
  }  
}


/*
 * all mds links are flushed.  unfreeze dir!
 */
void Migrator::unhash_dir_finish(CDir *dir)
{
  dout(7) << "unhash_dir_finish " << *dir << endl;
  hash_gather.erase(dir);

  // unpin path
  vector<CDentry*> trace;
  cache->make_trace(trace, dir->inode);
  cache->path_unpin(trace, 0);

  // state
  dir->state_clear(CDIR_STATE_UNHASHING);

  // unfreeze
  dir->unfreeze_dir();

}



// UNHASH on all

/*
 * hashed dir is complete.  
 *  mark all migrating inodes dirty (to pin in cache)
 *  if frozen too, then go to next step (depending on auth)
 */
void Migrator::unhash_dir_complete(CDir *dir)
{
  dout(7) << "unhash_dir_complete " << *dir << ", dirtying inodes" << endl;
  
  assert(dir->is_hashed());
  assert(dir->is_complete());
  
  // mark dirty to pin in cache
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CInode *in = it->second->inode;
    if (in->is_auth()) {
      in->_mark_dirty(); // fixme
      mds->mdlog->submit_entry(new EString("unhash dirty fixme"));
    }
  }
  
  if (!dir->is_frozen_dir()) {
    dout(7) << "dir complete but !frozen, waiting " << *dir << endl;
  } else {
    if (dir->is_auth())
      unhash_dir_prep(dir);            // auth
    else
      unhash_dir_prep_finish(dir);  // nonauth
  }
}


// UNHASH on non-auth

class C_MDC_UnhashPrepFreeze : public Context {
public:
  Migrator *mig;
  CDir *dir;
  C_MDC_UnhashPrepFreeze(Migrator *m, CDir *d) : mig(m), dir(d) {}
  virtual void finish(int r) {
    mig->unhash_dir_prep_frozen(dir);
  }
};


/*
 * peers need to freeze their dir and make them complete
 */
void Migrator::handle_unhash_dir_prep(MUnhashDirPrep *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_prep " << *dir << endl;
  assert(dir->is_hashed());

  // freeze
  dir->freeze_dir(new C_MDC_UnhashPrepFreeze(this, dir));

  // make complete
  if (!dir->is_complete()) {
    dout(7) << "unhash_dir " << *dir << " not complete, fetching" << endl;
    mds->mdstore->fetch_dir(dir,
                            new C_MDC_UnhashComplete(this, dir));
  } else {
    unhash_dir_complete(dir);
  }
  
  delete m;
}

/*
 * peer has hashed dir frozen.  
 *  complete too?
 */
void Migrator::unhash_dir_prep_frozen(CDir *dir)
{
  dout(7) << "unhash_dir_prep_frozen " << *dir << endl;
  
  assert(dir->is_hashed());
  assert(dir->is_frozen_dir());
  assert(!dir->is_auth());
  
  if (!dir->is_complete()) {
    dout(7) << "unhash_dir_prep_frozen !complete, waiting still on " << *dir << endl;
  } else
    unhash_dir_prep_finish(dir);
}

/*
 * peer has hashed dir complete and frozen.  ack.
 */
void Migrator::unhash_dir_prep_finish(CDir *dir)
{
  dout(7) << "unhash_dir_prep_finish " << *dir << endl;
  assert(dir->is_hashed());
  assert(!dir->is_auth());
  assert(dir->is_frozen());
  assert(dir->is_complete());
  
  // twiddle state
  if (dir->is_unhashing())
    return;  // already replied.
  dir->state_set(CDIR_STATE_UNHASHING);

  // send subdirs back to auth
  MUnhashDirPrepAck *ack = new MUnhashDirPrepAck(dir->ino());
  int auth = dir->authority();
  
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    CInode *in = dn->inode;
    
    if (!in->is_dir()) continue;
    if (!in->dir) continue;
    
    int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
    if (dentryhashcode != mds->get_nodeid()) continue;
    
    // msg?
    ack->add_inode(it->first, in->replicate_to(auth));
  }
  
  // ack
  mds->send_message_mds(ack, auth, MDS_PORT_MIGRATOR);
}



/*
 * peer needs to send hashed dir content back to auth.
 *  unhash dir.
 */
void Migrator::handle_unhash_dir(MUnhashDir *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir " << *dir << endl;//" .. hash_seed is " << dir->inode->inode.hash_seed << endl;
  assert(dir->is_hashed());
  assert(dir->is_unhashing());
  assert(!dir->is_auth());
  
  // get message ready
  bufferlist bl;
  int nden = 0;

  // suck up all waiters
  C_Contexts *fin = new C_Contexts;
  list<Context*> waiting;
  dir->take_waiting(CDIR_WAIT_ANY, waiting);    // all dir waiters
  fin->take(waiting);
  
  // divy up contents
  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    CInode *in = dn->inode;

    int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
    if (dentryhashcode != mds->get_nodeid()) {
      // not mine!
      // twiddle dir_auth?
      if (in->dir) {
        if (in->dir->authority() != dir->authority())
          in->dir->set_dir_auth( in->dir->authority() );
        else
          in->dir->set_dir_auth( CDIR_AUTH_PARENT );
      }
      continue;
    }
    
    // -- dentry
    dout(7) << "unhash_dir_go sending to " << dentryhashcode << " dn " << *dn << endl;
    _encode(it->first, bl);
    
    // null dentry?
    if (dn->is_null()) {
      bl.append("N", 1);  // null dentry
      assert(dn->is_sync());
      continue;
    }

    if (dn->is_remote()) {
      // remote link
      bl.append("L", 1);  // remote link

      inodeno_t ino = dn->get_remote_ino();
      bl.append((char*)&ino, sizeof(ino));
      continue;
    }

    // primary link
    // -- inode
    bl.append("I", 1);    // inode dentry
    
    encode_export_inode(in, bl, dentryhashcode);  // encode, and (update state for) export
    nden++;

    if (dn->is_dirty()) 
      dn->mark_clean();

    // proxy
    in->state_set(CInode::STATE_PROXY);
    in->get(CInode::PIN_PROXY);
    hash_proxy_inos[dir].push_back(in);

    if (in->dir) {
      if (in->dir->is_auth()) {
        // mine.  make it into an import.
        dout(7) << "making subdir into import " << *in->dir << endl;
        in->dir->set_dir_auth( mds->get_nodeid() );
        cache->imports.insert(in->dir);
        in->dir->get(CDir::PIN_IMPORT);
        in->dir->state_set(CDIR_STATE_IMPORT);
      }
      else {
        // not mine.
        dout(7) << "un-exporting subdir that's being unhashed away " << *in->dir << endl;
        assert(in->dir->is_export());
        in->dir->put(CDir::PIN_EXPORT);
        in->dir->state_clear(CDIR_STATE_EXPORT);
        cache->exports.erase(in->dir);
        cache->nested_exports[dir].erase(in->dir);
      }
    }
    
    // waiters
    list<Context*> waiters;
    in->take_waiting(CINODE_WAIT_ANY, waiters);
    fin->take(waiters);
  }

  // we should have no nested exports; we're not auth for the dir!
  assert(cache->nested_exports[dir].empty());
  cache->nested_exports.erase(dir);

  // dir state
  //dir->state_clear(CDIR_STATE_UNHASHING);  // later
  dir->state_clear(CDIR_STATE_HASHED);
  dir->put(CDir::PIN_HASHED);
  cache->hashdirs.erase(dir);
  dir->mark_clean();

  // inode state
  dir->inode->inode.hash_seed = 0;
  if (dir->inode->is_auth()) {
    dir->inode->_mark_dirty(); // fixme
    mds->mdlog->submit_entry(new EString("unhash inode dirty fixme"));
  }

  // init gather set
  mds->get_mds_map()->get_active_mds_set( hash_gather[dir] );
  hash_gather[dir].erase(mds->get_nodeid());

  // send unhash message
  mds->send_message_mds(new MUnhashDirAck(dir->ino(), bl, nden),
			dir->authority(), MDS_PORT_MIGRATOR);
}


/*
 * first notify comes from auth.
 *  send notifies to all other peers, with peer = self
 * if we get notify from peer=other, remove from our gather list.
 * when we've gotten notifies from everyone,
 *  unpin proxies,
 *  send notify_ack to auth.
 * this ensures that all mds links are flushed of cache_expire type messages.
 */
void Migrator::handle_unhash_dir_notify(MUnhashDirNotify *m)
{
  CInode *in = cache->get_inode(m->get_ino());
  assert(in);
  CDir *dir = in->dir;
  assert(dir);
  
  dout(7) << "handle_unhash_dir_finish " << *dir << endl;
  assert(!dir->is_hashed());
  assert(dir->is_unhashing());
  assert(!dir->is_auth());
  
  int from = m->get_source().num();
  assert(hash_gather[dir].count(from) == 1);
  hash_gather[dir].erase(from);
  delete m;

  // did we send our shout out?
  if (from == dir->authority()) {
    // send notify to everyone else in weird chatter storm
    for (int i=0; i<mds->get_mds_map()->get_num_mds(); i++) {
      if (i == from) continue;
      if (i == mds->get_nodeid()) continue;
      mds->send_message_mds(new MUnhashDirNotify(dir->ino()), i, MDS_PORT_MIGRATOR);
    }
  }

  // are we done?
  if (!hash_gather[dir].empty()) {
    dout(7) << "still waiting for notify from " << hash_gather[dir] << endl;
    return;
  }
  hash_gather.erase(dir);

  // all done!
  dout(7) << "all mds links flushed, unpinning unhash proxies" << endl;

  // unpin proxies
  for (list<CInode*>::iterator it = hash_proxy_inos[dir].begin();
       it != hash_proxy_inos[dir].end();
       it++) {
    CInode *in = *it;
    assert(in->state_test(CInode::STATE_PROXY));
    in->state_clear(CInode::STATE_PROXY);
    in->put(CInode::PIN_PROXY);
  }

  // unfreeze
  dir->unfreeze_dir();
  
  // ack
  dout(7) << "sending notify_ack to auth for unhash of " << *dir << endl;
  mds->send_message_mds(new MUnhashDirNotifyAck(dir->ino()), dir->authority(), MDS_PORT_MIGRATOR);
  
}
