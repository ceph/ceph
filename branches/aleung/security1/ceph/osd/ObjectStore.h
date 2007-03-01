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


#ifndef __OBJECTSTORE_H
#define __OBJECTSTORE_H

#include "include/types.h"
#include "osd_types.h"
#include "include/Context.h"
#include "include/buffer.h"

#include "include/Distribution.h"

#include <sys/stat.h>

#ifdef DARWIN
#include <sys/statvfs.h>
#else
#include <sys/vfs.h>    /* or <sys/statfs.h> */
#endif /* DARWIN */

#include <list>
using namespace std;

#ifndef MIN
# define MIN(a,b) ((a) < (b) ? (a):(b))
#endif

/*
 * low-level interface to the local OSD file system
 */



class ObjectStore {
public:


  class FragmentationStat {
  public:
    int total;
    int num_extent;
    int avg_extent;
    map<int,int> extent_dist;          // powers of two
    map<int,int> extent_dist_sum;          // powers of two

    float avg_extent_per_object;
    int avg_extent_jump;  // avg distance bweteen consecutive extents

    int total_free;
    int num_free_extent;
    int avg_free_extent;
    map<int,int> free_extent_dist;     // powers of two
    map<int,int> free_extent_dist_sum;     // powers of two
  };
  
  

  /*********************************
   * transaction
   */
  class Transaction {
  public:
    static const int OP_READ =          1;  // oid, offset, len, pbl
    static const int OP_STAT =          2;  // oid, pstat
    static const int OP_GETATTR =       3;  // oid, attrname, pattrval
    static const int OP_GETATTRS =      4;  // oid, pattrset

    static const int OP_WRITE =        10;  // oid, offset, len, bl
    static const int OP_TRUNCATE =     11;  // oid, len
    static const int OP_REMOVE =       13;  // oid
    static const int OP_SETATTR =      14;  // oid, attrname, attrval
    static const int OP_SETATTRS =     15;  // oid, attrset
    static const int OP_RMATTR =       16;  // oid, attrname
    static const int OP_CLONE =        17;  // oid, newoid

    static const int OP_TRIMCACHE =    18;  // oid, offset, len

    static const int OP_MKCOLL =       20;  // cid
    static const int OP_RMCOLL =       21;  // cid
    static const int OP_COLL_ADD =     22;  // cid, oid
    static const int OP_COLL_REMOVE =  23;  // cid, oid
    static const int OP_COLL_SETATTR = 24;  // cid, attrname, attrval
    static const int OP_COLL_RMATTR =  25;  // cid, attrname

    list<int> ops;
    list<bufferlist> bls;
    list<object_t> oids;
    list<coll_t>   cids;
    list<off_t>    offsets;
    list<size_t>   lengths;
    list<const char*> attrnames;
    //list< pair<const void*,int> > attrvals;
    list<bufferlist>  attrbls;

    list<bufferlist*> pbls;
    list<struct stat*> psts;
    list< pair<void*,int*> > pattrvals;
    list< map<string,bufferptr>* > pattrsets;

    void read(object_t oid, off_t off, size_t len, bufferlist *pbl) {
      int op = OP_READ;
      ops.push_back(op);
      oids.push_back(oid);
      offsets.push_back(off);
      lengths.push_back(len);
      pbls.push_back(pbl);
    }
    void stat(object_t oid, struct stat *st) {
      int op = OP_STAT;
      ops.push_back(op);
      oids.push_back(oid);
      psts.push_back(st);
    }
    void getattr(object_t oid, const char* name, void* val, int *plen) {
      int op = OP_GETATTR;
      ops.push_back(op);
      oids.push_back(oid);
      attrnames.push_back(name);
      pattrvals.push_back(pair<void*,int*>(val,plen));
    }
    void getattrs(object_t oid, map<string,bufferptr>& aset) {
      int op = OP_GETATTRS;
      ops.push_back(op);
      oids.push_back(oid);
      pattrsets.push_back(&aset);
    }

    void write(object_t oid, off_t off, size_t len, bufferlist& bl) {
      int op = OP_WRITE;
      ops.push_back(op);
      oids.push_back(oid);
      offsets.push_back(off);
      lengths.push_back(len);
      bls.push_back(bl);
    }
    void trim_from_cache(object_t oid, off_t off, size_t len) {
      int op = OP_TRIMCACHE;
      ops.push_back(op);
      oids.push_back(oid);
      offsets.push_back(off);
      lengths.push_back(len);
    }
    void truncate(object_t oid, off_t off) {
      int op = OP_TRUNCATE;
      ops.push_back(op);
      oids.push_back(oid);
      offsets.push_back(off);
    }
    void remove(object_t oid) {
      int op = OP_REMOVE;
      ops.push_back(op);
      oids.push_back(oid);
    }
    void setattr(object_t oid, const char* name, const void* val, int len) {
      int op = OP_SETATTR;
      ops.push_back(op);
      oids.push_back(oid);
      attrnames.push_back(name);
      //attrvals.push_back(pair<const void*,int>(val,len));
      bufferlist bl;
      bl.append((char*)val,len);
      attrbls.push_back(bl);
    }
    void setattrs(object_t oid, map<string,bufferptr>& attrset) {
      int op = OP_SETATTRS;
      ops.push_back(op);
      oids.push_back(oid);
      pattrsets.push_back(&attrset);
    }
    void rmattr(object_t oid, const char* name) {
      int op = OP_RMATTR;
      ops.push_back(op);
      oids.push_back(oid);
      attrnames.push_back(name);
    }
    void clone(object_t oid, object_t noid) {
      int op = OP_CLONE;
      ops.push_back(op);
      oids.push_back(oid);
      oids.push_back(noid);
    }
    void create_collection(coll_t cid) {
      int op = OP_MKCOLL;
      ops.push_back(op);
      cids.push_back(cid);
    }
    void remove_collection(coll_t cid) {
      int op = OP_RMCOLL;
      ops.push_back(op);
      cids.push_back(cid);
    }
    void collection_add(coll_t cid, object_t oid) {
      int op = OP_COLL_ADD;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
    }
    void collection_remove(coll_t cid, object_t oid) {
      int op = OP_COLL_REMOVE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
    }
    void collection_setattr(coll_t cid, const char* name, const void* val, int len) {
      int op = OP_COLL_SETATTR;
      ops.push_back(op);
      cids.push_back(cid);
      attrnames.push_back(name);
      //attrvals.push_back(pair<const void*,int>(val,len));
      bufferlist bl;
      bl.append((char*)val, len);
      attrbls.push_back(bl);
    }
    void collection_rmattr(coll_t cid, const char* name) {
      int op = OP_COLL_RMATTR;
      ops.push_back(op);
      cids.push_back(cid);
      attrnames.push_back(name);
    }

    // etc.
  };



  /* this implementation is here only for naive ObjectStores that
   * do not do atomic transactions natively.  it is not atomic.
   */
  virtual unsigned apply_transaction(Transaction& t, Context *onsafe=0) {
    // non-atomic implementation
    for (list<int>::iterator p = t.ops.begin();
         p != t.ops.end();
         p++) {
      switch (*p) {
      case Transaction::OP_READ:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          off_t offset = t.offsets.front(); t.offsets.pop_front();
          size_t len = t.lengths.front(); t.lengths.pop_front();
          bufferlist *pbl = t.pbls.front(); t.pbls.pop_front();
          read(oid, offset, len, *pbl);
        }
        break;
      case Transaction::OP_STAT:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          struct stat *st = t.psts.front(); t.psts.pop_front();
          stat(oid, st);
        }
        break;
      case Transaction::OP_GETATTR:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
          pair<void*,int*> pattrval = t.pattrvals.front(); t.pattrvals.pop_front();
          *pattrval.second = getattr(oid, attrname, pattrval.first, *pattrval.second);
        }
        break;
      case Transaction::OP_GETATTRS:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          map<string,bufferptr> *pset = t.pattrsets.front(); t.pattrsets.pop_front();
          getattrs(oid, *pset);
        }
        break;

      case Transaction::OP_WRITE:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          off_t offset = t.offsets.front(); t.offsets.pop_front();
          size_t len = t.lengths.front(); t.lengths.pop_front();
          bufferlist bl = t.bls.front(); t.bls.pop_front();
          write(oid, offset, len, bl, 0);
        }
        break;

      case Transaction::OP_TRIMCACHE:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          off_t offset = t.offsets.front(); t.offsets.pop_front();
          size_t len = t.lengths.front(); t.lengths.pop_front();
          trim_from_cache(oid, offset, len);
        }
        break;

      case Transaction::OP_TRUNCATE:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          off_t len = t.offsets.front(); t.offsets.pop_front();
          truncate(oid, len, 0);
        }
        break;

      case Transaction::OP_REMOVE:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          remove(oid, 0);
        }
        break;

      case Transaction::OP_SETATTR:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
          //pair<const void*,int> attrval = t.attrvals.front(); t.attrvals.pop_front();
          bufferlist bl;
          bl.claim( t.attrbls.front() );
          t.attrbls.pop_front();
          setattr(oid, attrname, bl.c_str(), bl.length(), 0);
        }
        break;
      case Transaction::OP_SETATTRS:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          map<string,bufferptr> *pattrset = t.pattrsets.front(); t.pattrsets.pop_front();
          setattrs(oid, *pattrset, 0);
        }
        break;

      case Transaction::OP_RMATTR:
        {
          object_t oid = t.oids.front(); t.oids.pop_front();
          const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
          rmattr(oid, attrname, 0);
        }
        break;

      case Transaction::OP_CLONE:
	{
          object_t oid = t.oids.front(); t.oids.pop_front();
          object_t noid = t.oids.front(); t.oids.pop_front();
	  clone(oid, noid);
	}
	break;

      case Transaction::OP_MKCOLL:
        {
          coll_t cid = t.cids.front(); t.cids.pop_front();
          create_collection(cid, 0);
        }
        break;

      case Transaction::OP_RMCOLL:
        {
          coll_t cid = t.cids.front(); t.cids.pop_front();
          destroy_collection(cid, 0);
        }
        break;

      case Transaction::OP_COLL_ADD:
        {
          coll_t cid = t.cids.front(); t.cids.pop_front();
          object_t oid = t.oids.front(); t.oids.pop_front();
          collection_add(cid, oid, 0);
        }
        break;

      case Transaction::OP_COLL_REMOVE:
        {
          coll_t cid = t.cids.front(); t.cids.pop_front();
          object_t oid = t.oids.front(); t.oids.pop_front();
          collection_remove(cid, oid, 0);
        }
        break;

      case Transaction::OP_COLL_SETATTR:
        {
          coll_t cid = t.cids.front(); t.cids.pop_front();
          const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
          //pair<const void*,int> attrval = t.attrvals.front(); t.attrvals.pop_front();
          bufferlist bl;
          bl.claim( t.attrbls.front() );
          t.attrbls.pop_front();
          collection_setattr(cid, attrname, bl.c_str(), bl.length(), 0);
        }
        break;

      case Transaction::OP_COLL_RMATTR:
        {
          coll_t cid = t.cids.front(); t.cids.pop_front();
          const char *attrname = t.attrnames.front(); t.attrnames.pop_front();
          collection_rmattr(cid, attrname, 0);
        }
        break;


      default:
        cerr << "bad op " << *p << endl;
        assert(0);
      }
    }

    if (onsafe) sync(onsafe);

    return 0;  // FIXME count errors
  }

  /*********************************************/



 public:
  ObjectStore() {}
  virtual ~ObjectStore() {}

  // mgmt
  virtual int mount() = 0;
  virtual int umount() = 0;
  virtual int mkfs() = 0;  // wipe

  virtual int statfs(struct statfs *buf) = 0;

  // objects
  virtual int pick_object_revision_lt(object_t& oid) = 0;

  virtual bool exists(object_t oid) = 0;                   // useful?
  virtual int stat(object_t oid, struct stat *st) = 0;     // struct stat?

  virtual int remove(object_t oid,
                     Context *onsafe=0) = 0;

  virtual int truncate(object_t oid, off_t size,
                       Context *onsafe=0) = 0;
  
  virtual int read(object_t oid, 
                   off_t offset, size_t len,
                   bufferlist& bl) = 0;

  /*virtual int write(object_t oid,
                    off_t offset, size_t len, 
                    bufferlist& bl,
                    bool fsync=true) = 0;     
  */
  virtual int write(object_t oid, 
                    off_t offset, size_t len,
                    bufferlist& bl, 
                    Context *onsafe) = 0;//{ return -1; }
  virtual void trim_from_cache(object_t oid, 
			       off_t offset, size_t len) { }

  virtual int setattr(object_t oid, const char *name,
                      const void *value, size_t size,
                      Context *onsafe=0) {return 0;} //= 0;
  virtual int setattrs(object_t oid, map<string,bufferptr>& aset,
                      Context *onsafe=0) {return 0;} //= 0;
  virtual int getattr(object_t oid, const char *name,
                      void *value, size_t size) {return 0;} //= 0;
  virtual int getattrs(object_t oid, map<string,bufferptr>& aset) {return 0;};

  virtual int rmattr(object_t oid, const char *name,
                     Context *onsafe=0) {return 0;}

  virtual int clone(object_t oid, object_t noid) {
    return -1; 
  }

  //virtual int listattr(object_t oid, char *attrs, size_t size) {return 0;} //= 0;
  
  // collections
  virtual int list_collections(list<coll_t>& ls) {return 0;}//= 0;
  virtual int create_collection(coll_t c,
                                Context *onsafe=0) {return 0;}//= 0;
  virtual int destroy_collection(coll_t c,
                                 Context *onsafe=0) {return 0;}//= 0;
  virtual bool collection_exists(coll_t c) {return 0;}
  virtual int collection_stat(coll_t c, struct stat *st) {return 0;}//= 0;
  virtual int collection_add(coll_t c, object_t o,
                             Context *onsafe=0) {return 0;}//= 0;
  virtual int collection_remove(coll_t c, object_t o,
                                Context *onsafe=0) {return 0;}// = 0;
  virtual int collection_list(coll_t c, list<object_t>& o) {return 0;}//= 0;

  virtual int collection_setattr(coll_t cid, const char *name,
                                 const void *value, size_t size,
                                 Context *onsafe=0) {return 0;} //= 0;
  virtual int collection_rmattr(coll_t cid, const char *name,
                                Context *onsafe=0) {return 0;} //= 0;
  virtual int collection_getattr(coll_t cid, const char *name,
                                 void *value, size_t size) {return 0;} //= 0;
  //virtual int collection_listattr(coll_t cid, char *attrs, size_t size) {return 0;} //= 0;
  
  virtual void sync(Context *onsync) {}
  virtual void sync() {}
  
  
  virtual void _fake_writes(bool b) {};

  virtual void _get_frag_stat(FragmentationStat& st) {};
  
};


#endif
