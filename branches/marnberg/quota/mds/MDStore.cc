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



#include "MDStore.h"
#include "MDS.h"
#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "MDSMap.h"

#include "osd/OSDMap.h"
#include "osdc/Filer.h"

#include "msg/Message.h"

#include <cassert>
#include <iostream>
using namespace std;


#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".store "


/*
 * separate hashed dir slices into "regions"
 */
size_t get_hash_offset(int hashcode) {
  if (hashcode < 0)   
    return 0;  // not hashed
  else
    return (size_t)(1<<30) * (size_t)(1+hashcode);
}




// ==========================================================================
// FETCH


class C_MDS_Fetch : public Context {
 protected:
  MDStore *ms;
  inodeno_t ino;

 public:
  C_MDS_Fetch(MDStore *ms, inodeno_t ino) : Context() {
    this->ms = ms;
    this->ino = ino;
  }
  
  void finish(int result) {
    ms->fetch_dir_2( result, ino );
  }
};

/** fetch_dir(dir, context)
 * public call to fetch a dir.
 */
void MDStore::fetch_dir( CDir *dir,
                         Context *c )
{
  dout(7) << "fetch_dir " << *dir << " context is " << c << endl;
  assert(dir->is_auth() ||
         dir->is_hashed());

  // wait
  if (c) dir->add_waiter(CDIR_WAIT_COMPLETE, c);
  
  // already fetching?
  if (dir->state_test(CDIR_STATE_FETCHING)) {
    dout(7) << "already fetching " << *dir << "; waiting" << endl;
    return;
  }
  
  // state
  dir->state_set(CDIR_STATE_FETCHING);
  
  // stats
  if (mds->logger) mds->logger->inc("fdir");
  
  // create return context
  Context *fin = new C_MDS_Fetch( this, dir->ino() );
  if (dir->is_hashed()) 
    fetch_dir_hash( dir, fin, mds->get_nodeid());   // hashed
  else 
    fetch_dir_hash( dir, fin );                     // normal
}

/*
 * called by low level fn when it's fetched.
 * fix up dir state.
 */
void MDStore::fetch_dir_2( int result, 
                           inodeno_t ino)
{
  CInode *idir = mds->mdcache->get_inode(ino);
  
  if (!idir || result < 0) return;  // hmm!  nevermind i guess.

  assert(idir);
  CDir *dir = idir->dir;
  assert(dir);
  
  // dir is now complete
  dir->state_set(CDIR_STATE_COMPLETE);
  dir->state_clear(CDIR_STATE_FETCHING);

  // finish
  list<Context*> finished;
  dir->take_waiting(CDIR_WAIT_COMPLETE|CDIR_WAIT_DENTRY, finished);
  finish_contexts(finished, result);
}


/** low level methods **/

class C_MDS_FetchHash : public Context {
protected:
  MDS *mds;
  inode_t inode;
  int hashcode;
  Context *context;
  
public:
  bufferlist bl;
  bufferlist bl2;
  
  C_MDS_FetchHash(MDS *mds, inode_t inode, Context *c, int hashcode) : Context() {
    this->mds = mds;
    this->inode = inode;
    this->hashcode = hashcode;
    this->context = c;
  }
  
  void finish(int result) {
    assert(result>0);

    // combine bufferlists bl + bl2 -> bl
    bl.claim_append(bl2);

    // did i get the whole thing?
    size_t size;
    bl.copy(0, sizeof(size_t), (char*)&size);
    size_t got = bl.length() - sizeof(size);
    size_t left = size - got;
    size_t from = bl.length();

    // what part of dir are we getting?
    from += get_hash_offset(hashcode);
    
    if (got >= size) {
      // done.
      mds->mdstore->fetch_dir_hash_2( bl, inode, context, hashcode );
    }
    else {
      // read the rest!
      dout(12) << "fetch_dir_hash_2 dir size is " << size << ", got " << got << ", reading remaniing " << left << " from off " << from << endl;
      
      // create return context
      C_MDS_FetchHash *fin = new C_MDS_FetchHash( mds, inode, context, hashcode );
      fin->bl.claim( bl );
      mds->filer->read(inode,
                       from, left, 
                       &fin->bl2,
                       fin );
      return;
    }
  }
};

/** fetch_dir_hash
 * low level method.
 * fetch part of a dir.  either the whole thing if hashcode is -1, or a specific 
 * hash segment.
 */
void MDStore::fetch_dir_hash( CDir *dir,
                              Context *c, 
                              int hashcode)
{
  dout(11) << "fetch_dir_hash hashcode " << hashcode << " " << *dir << endl;
  
  // create return context
  C_MDS_FetchHash *fin = new C_MDS_FetchHash( mds, dir->get_inode()->inode, c, hashcode );
  
  // grab first stripe bit (which had better be more than 16 bytes!)
  assert(dir->get_inode()->inode.layout.stripe_size >= 16);
  mds->filer->read(dir->get_inode()->inode,
                   get_hash_offset(hashcode), dir->get_inode()->inode.layout.stripe_size, 
                   &fin->bl,
                   fin );
}

void MDStore::fetch_dir_hash_2( bufferlist& bl,
                                inode_t& inode,
                                Context *c,                           
                                int hashcode)
{
  CInode *idir = mds->mdcache->get_inode(inode.ino);
  if (!idir) {
    dout(7) << "fetch_dir_hash_2 on ino " << inode.ino << " but no longer in our cache!" << endl;
    c->finish(-1);
    delete c;
    return;
  } 

  if (!idir->dir_is_auth() ||
      !idir->dir) {
    dout(7) << "fetch_dir_hash_2 on " << *idir << ", but i'm not auth, or dir not open" << endl;
    c->finish(-1);
    delete c;
    return;
  } 

  // make sure we have a CDir
  CDir *dir = idir->get_or_open_dir(mds->mdcache);
  
  // do it
  dout(7) << "fetch_dir_hash_2 hashcode " << hashcode << " dir " << *dir << endl;
  
  // parse buffer contents into cache
  dout(15) << "bl is " << bl << endl;

  int off = 0;
  size_t size;
  __uint32_t num;
  version_t got_version;
  int got_hashcode;
  bl.copy(off, sizeof(size), (char*)&size);
  off += sizeof(size);
  assert(bl.length() >= size + sizeof(size));  
  bl.copy(off, sizeof(num), (char*)&num);
  off += sizeof(num);
  bl.copy(off, sizeof(got_version), (char*)&got_version);
  off += sizeof(got_version);
  bl.copy(off, sizeof(got_hashcode), (char*)&got_hashcode);
  off += sizeof(got_hashcode);

  assert(got_hashcode == hashcode);  
  
  int buflen = bl.length();
  
  dout(10) << "  " << num << " items in " << size << " bytes" << endl;

  unsigned parsed = 0;
  while (parsed < num) {
    assert(off < buflen && num > 0);
    parsed++;
    
    dout(24) << " " << parsed << "/" << num << " pos " << off << endl;

    // dentry
    string dname;
    ::_decode(dname, bl, off);
    dout(24) << "parse filename '" << dname << "'" << endl;
    
    CDentry *dn = dir->lookup(dname);  // existing dentry?
    
    char type = bl[off];
    ++off;
    if (type == 'L') {
      // hard link
      inodeno_t ino;
      bl.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);

      // what to do?
      if (hashcode >= 0) {
        int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), dname );
        assert(dentryhashcode == hashcode);
      }

      if (dn) {
        if (dn->get_inode() == 0) {
          // negative dentry?
          dout(12) << "readdir had NEG dentry " << dname << endl;
        } else {
          // had dentry
          dout(12) << "readdir had dentry " << dname << endl;
        }
        continue;
      }

      // (remote) link
      CDentry *dn = dir->add_dentry( dname, ino );

      // link to inode?
      CInode *in = mds->mdcache->get_inode(ino);   // we may or may not have it.
      if (in) {
        dn->link_remote(in);
        dout(12) << "readdir got remote link " << ino << " which we have " << *in << endl;
      } else {
        dout(12) << "readdir got remote link " << ino << " (dont' have it)" << endl;
      }
    } 
    else if (type == 'I') {
      // inode
      
      // parse out inode
      inode_t inode;
      bl.copy(off, sizeof(inode), (char*)&inode);
      off += sizeof(inode);

      string symlink;
      if (inode.is_symlink())
        ::_decode(symlink, bl, off);
      
      // what to do?
      if (hashcode >= 0) {
        int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), dname );
        assert(dentryhashcode == hashcode);
      }
      
      if (dn) {
        if (dn->get_inode() == 0) {
          // negative dentry?
          dout(12) << "readdir had NEG dentry " << dname << endl;
        } else {
          // had dentry
          dout(12) << "readdir had dentry " << dname << endl;

	  // under water?
	  if (dn->get_version() <= got_version) {
	    assert(dn->get_inode()->get_version() <= got_version);
	    dout(10) << "readdir had underwater dentry " << dname << " and inode, marking clean" << endl;
	    dn->mark_clean();
	    dn->get_inode()->mark_clean();
	  }
        }
        continue;
      }
      
      // add inode
      CInode *in = 0;
      if (mds->mdcache->have_inode(inode.ino)) {
        in = mds->mdcache->get_inode(inode.ino);
        dout(12) << "readdir got (but i already had) " << *in 
		 << " mode " << in->inode.mode 
		 << " mtime " << in->inode.mtime << endl;
      } else {
        // inode
        in = new CInode(mds->mdcache);
	in->inode = inode;
        
        // symlink?
        if (in->is_symlink()) {
          in->symlink = symlink;
        }
        
        // add 
        mds->mdcache->add_inode( in );
      }

      // link
      dir->add_dentry( dname, in );
      dout(12) << "readdir got " << *in << " mode " << in->inode.mode << " mtime " << in->inode.mtime << endl;
    }
    else {
      dout(1) << "corrupt directory, i got tag char '" << type << "' val " << (int)(type) 
	      << " at pos " << off << endl;
      assert(0);
    }
  }
  dout(15) << "parsed " << parsed << endl;
  
  if (c) {
    c->finish(0);
    delete c;
  }
}




// ==================================================================
// COMMIT

class C_MDS_CommitDirVerify : public Context {
public:
  MDS *mds;
  inodeno_t ino;
  version_t version;
  Context *c;
  
  C_MDS_CommitDirVerify( MDS *mds, 
                        inodeno_t ino, 
                        version_t version,
                        Context *c) {
    this->mds = mds;
    this->c = c;
    this->version = version;
    this->ino = ino;
  }
  
  virtual void finish(int r) {

    if (r >= 0) {
      CInode *in = mds->mdcache->get_inode(ino);
      assert(in && in->dir);
      if (in && in->dir && in->dir->is_auth()) {
        dout(7) << "CommitDirVerify: current = " << in->dir->get_version() 
		<< ", last committed = " << in->dir->get_last_committed_version() 
		<< ", required = " << version << endl;
        
        if (in->dir->get_last_committed_version() >= version) {
          dout(7) << "my required version is safe, done." << endl;
	  if (c) {
	    c->finish(0);
	    delete c;
	  }
        } else { 
          dout(7) << "my required version is still not safe, committing again." << endl;

          // what was requested isn't committed yet.
          mds->mdstore->commit_dir(in->dir, 
                                   version,
                                   c);
        }
	return;
      }
    } 
      
    // must have exported ors omethign!
    dout(7) << "can't retry commit dir on " << ino << ", must have exported?" << endl;
    
    // finish.
    if (c) {
      c->finish(-1);
      delete c;
    }
  }
};

class C_MDS_CommitDirFinish : public Context {
 protected:
  MDStore *ms;
  CDir *dir;
  version_t version;

 public:

  C_MDS_CommitDirFinish(MDStore *ms, CDir *dir) : Context() {
    this->ms = ms;
    this->dir = dir;
    this->version = dir->get_version();   // just for sanity check later
  }
  
  void finish(int result) {
    ms->commit_dir_2( result, dir, version );
  }
};


void MDStore::commit_dir( CDir *dir,
                          Context *c )
{
  assert(dir->is_dirty());
  
  // commit thru current version
  commit_dir(dir, dir->get_version(), c);
}

void MDStore::commit_dir( CDir *dir,
                          version_t version,
                          Context *c )
{
  assert(dir->is_auth() ||
         dir->is_hashed());
  
  // already committing?
  if (dir->state_test(CDIR_STATE_COMMITTING)) {
    // already mid-commit!
    dout(7) << "commit_dir " << *dir << " mid-commit of " << dir->get_committing_version() << endl;
    dout(7) << "  current version = " << dir->get_version() << endl;
    dout(7) << "requested version = " << version << endl;

    assert(version >= dir->get_last_committed_version());  // why would we request _old_ one?

    dir->add_waiter(CDIR_WAIT_COMMITTED, 
                    new C_MDS_CommitDirVerify(mds, dir->ino(), version, c) );
    return;
  }

  if (!dir->can_auth_pin()) {
    // something must be frozen up the hiearchy!
    dout(7) << "commit_dir " << *dir << " can't auth_pin, waiting" << endl;
    dir->add_waiter(CDIR_WAIT_AUTHPINNABLE,
                    new C_MDS_CommitDirVerify(mds, dir->ino(), version, c) );
    return;
  }


  // is it complete?
  if (!dir->is_complete()) {
    dout(7) << "commit_dir " << *dir << " not complete, fetching first" << endl;
    // fetch dir first
    fetch_dir(dir, 
              new C_MDS_CommitDirVerify(mds, dir->ino(), version, c) );
    return;
  }


  // ok go
  dout(7) << "commit_dir " << *dir << " version " << dir->get_version() << endl;

  // add waiter
  if (c) dir->add_waiter(CDIR_WAIT_COMMITTED, c);

  // get continuation ready
  Context *fin = new C_MDS_CommitDirFinish(this, dir);
  
  // state
  dir->state_set(CDIR_STATE_COMMITTING);
  dir->set_committing_version(); 

  // stats
  if (mds->logger) mds->logger->inc("cdir");

  if (dir->is_hashed()) {
    // hashed
    commit_dir_slice( dir, fin, mds->get_nodeid() );
  } else {
    // non-hashed
    commit_dir_slice( dir, fin );
  }
}

void MDStore::commit_dir_2( int result,
                            CDir *dir,
                            version_t committed_version)
{
  dout(5) << "commit_dir_2 " << *dir << " committed " << committed_version << ", current version " << dir->get_version() << endl;
  assert(committed_version == dir->get_committing_version());

  // remember which version is now safe
  dir->set_last_committed_version(committed_version);
  
  // is the dir now clean?
  if (committed_version == dir->get_version())
    dir->mark_clean();
 
  dir->state_clear(CDIR_STATE_COMMITTING);

  // finish
  dir->finish_waiting(CDIR_WAIT_COMMITTED);
}




// low-level committer (hashed or normal)

class C_MDS_CommitSlice : public Context {
 protected:
  MDStore *ms;
  CDir *dir;
  Context *c;
  int hashcode;
  version_t version;

public:
  bufferlist bl;

  C_MDS_CommitSlice(MDStore *ms, CDir *dir, Context *c, int w) : Context() {
    this->ms = ms;
    this->dir = dir;
    this->c = c;
    this->hashcode = w;
    version = dir->get_version();
  }
  
  void finish(int result) {
    ms->commit_dir_slice_2( result, dir, c, version, hashcode );
  }
};


void MDStore::commit_dir_slice( CDir *dir,
                               Context *c,
                               int hashcode)
{
  if (hashcode >= 0) {
    assert(dir->is_hashed());
    dout(10) << "commit_dir_slice hashcode " << hashcode << " " << *dir << " version " << dir->get_version() << endl;
  } else {
    assert(dir->is_auth());
    dout(10) << "commit_dir_slice (whole dir) " << *dir << " version " << dir->get_version() << endl;
  }
  
  // get continuation ready
  C_MDS_CommitSlice *fin = new C_MDS_CommitSlice(this, dir, c, hashcode);
  
  // fill buffer
  __uint32_t num = 0;
  
  bufferlist dirdata;

  version_t v = dir->get_version();
  dirdata.append((char*)&v, sizeof(v));
  dirdata.append((char*)&hashcode, sizeof(hashcode));
  
  for (CDir_map_t::iterator it = dir->begin();
       it != dir->end();
       it++) {
    CDentry *dn = it->second;

    if (hashcode >= 0) {
      int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), it->first );
      if (dentryhashcode != hashcode) continue;
    }

    if (dn->is_null()) continue;  // skipping negative entry

    // primary or remote?
    if (dn->is_remote()) {

      inodeno_t ino = dn->get_remote_ino();
      dout(14) << " pos " << dirdata.length() << " dn '" << it->first << "' remote ino " << ino << endl;

      // name, marker, ion
      dirdata.append( it->first.c_str(), it->first.length() + 1);
      dirdata.append( "L", 1 );         // remote link
      dirdata.append((char*)&ino, sizeof(ino));

    } else {
      // primary link
      CInode *in = dn->get_inode();
      assert(in);

      dout(14) << " pos " << dirdata.length() << " dn '" << it->first << "' inode " << *in << endl;
  
      // name, marker, inode, [symlink string]
      dirdata.append( it->first.c_str(), it->first.length() + 1);
      dirdata.append( "I", 1 );         // inode
      dirdata.append( (char*) &in->inode, sizeof(inode_t));
      
      if (in->is_symlink()) {
        // include symlink destination!
        dout(18) << "    inlcuding symlink ptr " << in->symlink << endl;
        dirdata.append( (char*) in->symlink.c_str(), in->symlink.length() + 1);
      }
    }

    num++;
  }
  dout(14) << "num " << num << endl;
  
  // put count in buffer
  //bufferlist bl;
  size_t size = sizeof(num) + dirdata.length();
  fin->bl.append((char*)&size, sizeof(size));
  fin->bl.append((char*)&num, sizeof(num));
  fin->bl.claim_append(dirdata);  //.c_str(), dirdata.length());
  assert(fin->bl.length() == size + sizeof(size));
  
  // pin inode
  dir->auth_pin();
  
  // submit to osd
  mds->filer->write( dir->get_inode()->inode,
                     0, fin->bl.length(), 
                     fin->bl,
                     0, //OSD_OP_FLAGS_TRUNCATE, // truncate file/object after end of this write
                     NULL, fin ); // on safe
}


void MDStore::commit_dir_slice_2( int result,
                                 CDir *dir,
                                 Context *c,
                                 version_t committed_version,
                                 int hashcode )
{
  dout(11) << "commit_dir_slice_2 hashcode " << hashcode << " " << *dir << " v " << committed_version << endl;
  
  // mark inodes and dentries clean too (if we committed them!)
  list<CDentry*> null_clean;
  for (CDir_map_t::iterator it = dir->begin();
       it != dir->end(); ) {
    CDentry *dn = it->second;
    it++;
    
    if (hashcode >= 0) {
      int dentryhashcode = mds->mdcache->hash_dentry( dir->ino(), dn->get_name() );
      if (dentryhashcode != hashcode) continue;
    }

    // dentry
    if (committed_version >= dn->get_version()) {
      if (dn->is_dirty()) {
	dout(15) << " dir " << committed_version << " >= dn " << dn->get_version() << " now clean " << *dn << endl;
	dn->mark_clean();
      } 
    } else {
      dout(15) << " dir " << committed_version << " < dn " << dn->get_version() << " still dirty " << *dn << endl;
    }

    // only do primary...
    if (!dn->is_primary()) 
      continue;
    
    CInode *in = dn->get_inode();
    assert(in);
    assert(in->is_auth());
    
    if (committed_version >= in->get_version()) {
      if (in->is_dirty()) {
	dout(15) << " dir " << committed_version << " >= inode " << in->get_version() << " now clean " << *in << endl;
	in->mark_clean();
      }
    } else {
      dout(15) << " dir " << committed_version << " < inode " << in->get_version() << " still dirty " << *in << endl;
      assert(in->is_dirty());
    }
  }

  // unpin
  dir->auth_unpin();

  // finish
  if (c) {
    c->finish(0);
    delete c;
  }
}












