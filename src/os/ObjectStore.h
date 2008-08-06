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


#ifndef __OBJECTSTORE_H
#define __OBJECTSTORE_H

#include "include/types.h"
#include "include/Context.h"
#include "include/buffer.h"
#include "include/pobject.h"

#include "include/Distribution.h"

#include <sys/stat.h>

#ifdef DARWIN
#include <sys/statvfs.h>
#else
#include <sys/vfs.h>    /* or <sys/statfs.h> */
#endif /* DARWIN */

#include <list>
using std::list;

#ifndef MIN
# define MIN(a,b) ((a) < (b) ? (a):(b))
#endif

/*
 * low-level interface to the local OSD file system
 */


static inline void encode(const map<string,bufferptr> *attrset, bufferlist &bl) {
  ::encode(*attrset, bl);
}

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
    static const int OP_WRITE =        10;  // oid, offset, len, bl
    static const int OP_ZERO =         11;  // oid, offset, len
    static const int OP_TRUNCATE =     12;  // oid, len
    static const int OP_REMOVE =       13;  // oid
    static const int OP_SETATTR =      14;  // oid, attrname, bl
    static const int OP_SETATTRS =     15;  // oid, attrset
    static const int OP_RMATTR =       16;  // oid, attrname
    static const int OP_CLONE =        17;  // oid, newoid

    static const int OP_TRIMCACHE =    18;  // oid, offset, len

    static const int OP_MKCOLL =       20;  // cid
    static const int OP_RMCOLL =       21;  // cid
    static const int OP_COLL_ADD =     22;  // cid, oid
    static const int OP_COLL_REMOVE =  23;  // cid, oid
    static const int OP_COLL_SETATTR = 24;  // cid, attrname, bl
    static const int OP_COLL_RMATTR =  25;  // cid, attrname
    static const int OP_COLL_SETATTRS = 26;  // cid, attrset

  private:
    int len;
    int blen;  // for btrfs transactions
    list<int8_t> ops;
    list<bufferlist> bls;
    list<pobject_t> oids;
    list<coll_t> cids;
    list<int64_t> lengths;

    // for these guys, just use a pointer.
    // but, decode to a full value, and create pointers to that.
    list<const char*> attrnames;
    list<string> attrnames2;
    list<map<string,bufferptr> *> attrsets;
    list<map<string,bufferptr> > attrsets2;

  public:
    int get_len() { return len ? len : ops.size(); }
    int get_btrfs_len() { return blen; }

    bool have_op() {
      return !ops.empty();
    }
    int get_num_ops() { return ops.size(); }
    int get_op() {
      int op = ops.front();
      ops.pop_front();
      return op;
    }
    void get_bl(bufferlist& bl) {
      bl.claim(bls.front());
      bls.pop_front();
    }
    void get_oid(pobject_t& oid) {
      oid = oids.front();
      oids.pop_front();
    }
    void get_cid(coll_t& cid) {
      cid = cids.front();
      cids.pop_front();
    }
    void get_length(__u64& len) {
      len = lengths.front();
      lengths.pop_front();
    }
    void get_attrname(const char * &p) {
      p = attrnames.front();
      attrnames.pop_front();
    }
    void get_pattrset(map<string,bufferptr>* &ps) {
      ps = attrsets.front();
      attrsets.pop_front();
    }

    void write(coll_t cid, pobject_t oid, __u64 off, size_t len, const bufferlist& bl) {
      int op = OP_WRITE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      lengths.push_back(off);
      lengths.push_back(len);
      bls.push_back(bl);
      len++;
      blen += 3 + bl.buffers().size();
    }
    void zero(coll_t cid, pobject_t oid, __u64 off, size_t len) {
      int op = OP_ZERO;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      lengths.push_back(off);
      lengths.push_back(len);
      len++;
      blen += 3 + 1;
    }
    void trim_from_cache(coll_t cid, pobject_t oid, __u64 off, size_t len) {
      int op = OP_TRIMCACHE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      lengths.push_back(off);
      lengths.push_back(len);
      len++;
    }
    void truncate(coll_t cid, pobject_t oid, __u64 off) {
      int op = OP_TRUNCATE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      lengths.push_back(off);
      len++;
      blen++;
    }
    void remove(coll_t cid, pobject_t oid) {
      int op = OP_REMOVE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      len++;
      blen++;
    }
    void setattr(coll_t cid, pobject_t oid, const char* name, const void* val, int len) {
      bufferlist bl;
      bl.append((char*)val, len);
      setattr(cid, oid, name, bl);
    }
    void setattr(coll_t cid, pobject_t oid, const char* name, bufferlist& val) {
      int op = OP_SETATTR;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      attrnames.push_back(name);
      bls.push_back(val);
      len++;
      blen++;
    }
    void setattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& attrset) {
      int op = OP_SETATTRS;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      attrsets.push_back(&attrset);
      len++;
      blen += 5 + attrset.size();     // HACK allowance for removing old attrs
    }
    void rmattr(coll_t cid, pobject_t oid, const char* name) {
      int op = OP_RMATTR;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      attrnames.push_back(name);
      len++;
      blen++;
    }
    void clone(coll_t cid, pobject_t oid, pobject_t noid) {
      int op = OP_CLONE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      oids.push_back(noid);
      len++;
      blen += 5;
    }
    void create_collection(coll_t cid) {
      int op = OP_MKCOLL;
      ops.push_back(op);
      cids.push_back(cid);
      len++;
      blen++;
    }
    void remove_collection(coll_t cid) {
      int op = OP_RMCOLL;
      ops.push_back(op);
      cids.push_back(cid);
      len++;
      blen++;
    }
    void collection_add(coll_t cid, coll_t ocid, pobject_t oid) {
      int op = OP_COLL_ADD;
      ops.push_back(op);
      cids.push_back(cid);
      cids.push_back(ocid);
      oids.push_back(oid);
      len++;
      blen++;
    }
    void collection_remove(coll_t cid, pobject_t oid) {
      int op = OP_COLL_REMOVE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      len++;
       blen++;
   }
    void collection_setattr(coll_t cid, const char* name, const void* val, int len) {
      bufferlist bl;
      bl.append((char*)val, len);
      collection_setattr(cid, name, bl);
    }
    void collection_setattr(coll_t cid, const char* name, bufferlist& val) {
      int op = OP_COLL_SETATTR;
      ops.push_back(op);
      cids.push_back(cid);
      attrnames.push_back(name);
      bls.push_back(val);
      len++;
      blen++;
    }

    void collection_rmattr(coll_t cid, const char* name) {
      int op = OP_COLL_RMATTR;
      ops.push_back(op);
      cids.push_back(cid);
      attrnames.push_back(name);
      len++;
      blen++;
    }
    void collection_setattrs(coll_t cid, map<string,bufferptr>& aset) {
      int op = OP_COLL_SETATTRS;
      ops.push_back(op);
      cids.push_back(cid);
      attrsets.push_back(&aset);
      len++;
      blen += 5 + aset.size();
    }


    // etc.
    Transaction() : len(0) {}
    Transaction(bufferlist::iterator &p) : len(0) { decode(p); }
    Transaction(bufferlist &bl) : len(0) { 
      bufferlist::iterator p = bl.begin();
      decode(p); 
    }

    void encode(bufferlist& bl) const {
      ::encode(ops, bl);
      ::encode(bls, bl);
      ::encode(oids, bl);
      ::encode(cids, bl);
      ::encode(lengths, bl);
      ::encode(attrnames, bl);
      ::encode(attrsets, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(ops, bl);
      ::decode(bls, bl);
      ::decode(oids, bl);
      ::decode(cids, bl);
      ::decode(lengths, bl);
      ::decode(attrnames2, bl);
      for (list<string>::iterator p = attrnames2.begin();
	   p != attrnames2.end();
	   ++p)
	attrnames.push_back((*p).c_str());
      ::decode(attrsets2, bl);
      for (list<map<string,bufferptr> >::iterator p = attrsets2.begin();
	   p != attrsets2.end();
	   ++p)
	attrsets.push_back(&(*p));
    }
  };

  /*
   * these stubs should be implemented if we want to use the
   * apply_transaction() below and we want atomic transactions.
   */
  virtual int transaction_start(int len) { return 0; }
  virtual void transaction_end(int id) { }
  virtual unsigned apply_transaction(Transaction& t, Context *onsafe=0) {
    // non-atomic implementation
    int id = transaction_start(t.get_len());
    if (id < 0) return id;

    while (t.have_op()) {
      int op = t.get_op();
      switch (op) {
      case Transaction::OP_WRITE:
        {
 	  coll_t cid;
	  t.get_cid(cid);
	  pobject_t oid;
	  t.get_oid(oid);
          __u64 offset, len;
	  t.get_length(offset);
	  t.get_length(len);
          bufferlist bl;
	  t.get_bl(bl);
          write(cid, oid, offset, len, bl, 0);
        }
        break;

      case Transaction::OP_ZERO:
        {
  	  coll_t cid;
	  t.get_cid(cid);
	  pobject_t oid;
	  t.get_oid(oid);
          __u64 offset, len;
	  t.get_length(offset);
	  t.get_length(len);
          zero(cid, oid, offset, len, 0);
        }
        break;

      case Transaction::OP_TRIMCACHE:
        {
	  coll_t cid;
	  t.get_cid(cid);
	  pobject_t oid;
	  t.get_oid(oid);
          __u64 offset, len;
	  t.get_length(offset);
	  t.get_length(len);
          trim_from_cache(cid, oid, offset, len);
        }
        break;

      case Transaction::OP_TRUNCATE:
        {
  	  coll_t cid;
	  t.get_cid(cid);
	  pobject_t oid;
	  t.get_oid(oid);
          __u64 len;
	  t.get_length(len);
          truncate(cid, oid, len, 0);
        }
        break;

      case Transaction::OP_REMOVE:
        {
 	  coll_t cid;
	  t.get_cid(cid);
	  pobject_t oid;
	  t.get_oid(oid);
          remove(cid, oid, 0);
        }
        break;

      case Transaction::OP_SETATTR:
        {
 	  coll_t cid;
	  t.get_cid(cid);
	  pobject_t oid;
	  t.get_oid(oid);
          const char *attrname;
	  t.get_attrname(attrname);
          bufferlist bl;
	  t.get_bl(bl);
          setattr(cid, oid, attrname, bl.c_str(), bl.length(), 0);
        }
        break;
      case Transaction::OP_SETATTRS:
        {
	  coll_t cid;
	  t.get_cid(cid);
          pobject_t oid;
	  t.get_oid(oid);
          map<string,bufferptr> *pattrset;
	  t.get_pattrset(pattrset);
          setattrs(cid, oid, *pattrset, 0);
        }
        break;

      case Transaction::OP_RMATTR:
        {
  	  coll_t cid;
	  t.get_cid(cid);
	  pobject_t oid;
	  t.get_oid(oid);
          const char *attrname;
	  t.get_attrname(attrname);
          rmattr(cid, oid, attrname, 0);
        }
        break;

      case Transaction::OP_CLONE:
	{
  	  coll_t cid;
	  t.get_cid(cid);
	  pobject_t oid;
	  t.get_oid(oid);
          pobject_t noid;
	  t.get_oid(noid);
	  clone(cid, oid, noid);
	}
	break;

      case Transaction::OP_MKCOLL:
        {
          coll_t cid;
	  t.get_cid(cid);
          create_collection(cid, 0);
        }
        break;

      case Transaction::OP_RMCOLL:
        {
          coll_t cid;
	  t.get_cid(cid);
          destroy_collection(cid, 0);
        }
        break;

      case Transaction::OP_COLL_ADD:
        {
          coll_t cid, ocid;
	  t.get_cid(cid);
	  t.get_cid(ocid);
          pobject_t oid;
	  t.get_oid(oid);
          collection_add(cid, ocid, oid, 0);
        }
        break;

      case Transaction::OP_COLL_REMOVE:
        {
          coll_t cid;
	  t.get_cid(cid);
          pobject_t oid;
	  t.get_oid(oid);
          collection_remove(cid, oid, 0);
        }
        break;

      case Transaction::OP_COLL_SETATTR:
        {
          coll_t cid;
	  t.get_cid(cid);
          const char *attrname;
	  t.get_attrname(attrname);
          bufferlist bl;
	  t.get_bl(bl);
          collection_setattr(cid, attrname, bl.c_str(), bl.length(), 0);
        }
        break;

      case Transaction::OP_COLL_RMATTR:
        {
          coll_t cid;
	  t.get_cid(cid);
          const char *attrname;
	  t.get_attrname(attrname);
          collection_rmattr(cid, attrname, 0);
        }
        break;


      default:
        cerr << "bad op " << op << std::endl;
        assert(0);
      }
    }
    transaction_end(id);

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
  virtual int pick_object_revision_lt(coll_t cid, pobject_t& oid) = 0;
  virtual bool exists(coll_t cid, pobject_t oid) = 0;                   // useful?
  virtual int stat(coll_t cid, pobject_t oid, struct stat *st) = 0;     // struct stat?
  virtual int remove(coll_t cid, pobject_t oid, Context *onsafe=0) = 0;
  virtual int truncate(coll_t cid, pobject_t oid, __u64 size, Context *onsafe=0) = 0;
  
  virtual int read(coll_t cid, pobject_t oid, __u64 offset, size_t len, bufferlist& bl) = 0;
  virtual int write(coll_t cid, pobject_t oid, __u64 offset, size_t len, const bufferlist& bl, Context *onsafe) = 0;
  virtual int zero(coll_t cid, pobject_t oid, __u64 offset, size_t len, Context *onsafe) {
    // write zeros.. yuck!
    bufferptr bp(len);
    bufferlist bl;
    bl.push_back(bp);
    return write(cid, oid, offset, len, bl, onsafe);
  }
  virtual void trim_from_cache(coll_t cid, pobject_t oid, __u64 offset, size_t len) = 0; //{ }
  virtual int is_cached(coll_t cid, pobject_t oid, __u64 offset, size_t len) = 0;  //{ return -1; }

  virtual int setattr(coll_t cid, pobject_t oid, const char *name, const void *value, size_t size, Context *onsafe=0) = 0;
  virtual int setattr(coll_t cid, pobject_t oid, const char *name, const bufferptr &bp, Context *onsafe=0) {
    return setattr(cid, oid, name, bp.c_str(), bp.length(), onsafe);
  }
  virtual int setattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& aset, Context *onsafe=0) = 0;
  virtual int getattr(coll_t cid, pobject_t oid, const char *name, void *value, size_t size) = 0;
  virtual int getattr(coll_t cid, pobject_t oid, const char *name, bufferptr& value) = 0;
  virtual int getattrs(coll_t cid, pobject_t oid, map<string,bufferptr>& aset) {return 0;};

  virtual int rmattr(coll_t cid, pobject_t oid, const char *name,
                     Context *onsafe=0) {return 0;}

  virtual int clone(coll_t cid, pobject_t oid, pobject_t noid) {
    return -1;
  }
  
  virtual int get_object_collections(coll_t cid, pobject_t oid, set<coll_t>& ls) { return -1; }

  //virtual int listattr(pobject_t oid, char *attrs, size_t size) {return 0;} //= 0;
  
  // collections
  virtual int list_collections(list<coll_t>& ls) = 0;
  virtual int create_collection(coll_t c, Context *onsafe=0) = 0;
  virtual int destroy_collection(coll_t c, Context *onsafe=0) = 0;
  virtual bool collection_exists(coll_t c) = 0;
  virtual int collection_add(coll_t c, coll_t ocid, pobject_t o, Context *onsafe=0) = 0;
  virtual int collection_remove(coll_t c, pobject_t o, Context *onsafe=0) = 0;
  virtual int collection_list(coll_t c, list<pobject_t>& o) = 0;
  virtual int collection_setattr(coll_t cid, const char *name,
                                 const void *value, size_t size,
                                 Context *onsafe=0) = 0;
  virtual int collection_rmattr(coll_t cid, const char *name,
                                Context *onsafe=0) = 0;
  virtual int collection_getattr(coll_t cid, const char *name,
                                 void *value, size_t size) = 0;
  virtual int collection_getattr(coll_t cid, const char *name, bufferlist& bl) = 0;

  virtual int collection_getattrs(coll_t cid, map<string,bufferptr> &aset) = 0;
  virtual int collection_setattrs(coll_t cid, map<string,bufferptr> &aset) = 0;


  //virtual int collection_listattr(coll_t cid, char *attrs, size_t size) {return 0;} //= 0;
  
  virtual void sync(Context *onsync) {}
  virtual void sync() {}
  
  
  virtual void _fake_writes(bool b) {};

  virtual void _get_frag_stat(FragmentationStat& st) {};
  
};


#endif
