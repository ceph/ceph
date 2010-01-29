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
#include "include/nstring.h"

#include "include/Distribution.h"

#include "osd/osd_types.h"

#include <sys/stat.h>

#ifdef DARWIN
#include <sys/statvfs.h>
#else
#include <sys/vfs.h>    /* or <sys/statfs.h> */
#endif /* DARWIN */

#include <vector>
using std::vector;

#ifndef MIN
# define MIN(a,b) ((a) < (b) ? (a):(b))
#endif

typedef __u64 collection_list_handle_t;

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
    static const int OP_TOUCH =        9;   // cid, oid
    static const int OP_WRITE =        10;  // cid, oid, offset, len, bl
    static const int OP_ZERO =         11;  // cid, oid, offset, len
    static const int OP_TRUNCATE =     12;  // cid, oid, len
    static const int OP_REMOVE =       13;  // cid, oid
    static const int OP_SETATTR =      14;  // cid, oid, attrname, bl
    static const int OP_SETATTRS =     15;  // cid, oid, attrset
    static const int OP_RMATTR =       16;  // cid, oid, attrname
    static const int OP_CLONE =        17;  // cid, oid, newoid
    static const int OP_CLONERANGE =   18;  // cid, oid, newoid, offset, len

    static const int OP_TRIMCACHE =    19;  // cid, oid, offset, len

    static const int OP_MKCOLL =       20;  // cid
    static const int OP_RMCOLL =       21;  // cid
    static const int OP_COLL_ADD =     22;  // cid, oid
    static const int OP_COLL_REMOVE =  23;  // cid, oid
    static const int OP_COLL_SETATTR = 24;  // cid, attrname, bl
    static const int OP_COLL_RMATTR =  25;  // cid, attrname
    static const int OP_COLL_SETATTRS = 26;  // cid, attrset

    static const int OP_STARTSYNC =    27;  // start a sync 

    static const int OP_RMATTRS =      28;  // cid, oid

  private:
    /*
    int len;
    int blen;  // for btrfs transactions
    */
    vector<int8_t> ops;
    vector<bufferlist> bls;
    vector<sobject_t> oids;
    vector<coll_t> cids;
    vector<int64_t> lengths;

    // for these guys, just use a pointer.
    // but, decode to a full value, and create pointers to that.
    //vector<const char*> attrnames;
    vector<nstring> attrnames;
    vector<map<nstring,bufferptr> > attrsets;

    unsigned opp, blp, oidp, cidp, lengthp, attrnamep, attrsetp;

  public:
    /*
    int get_trans_len() { return len ? len : ops.size(); }
    int get_btrfs_len() { return blen; }
    */

    __u64 get_num_bytes() {
      // be conservative!
      __u64 s = 16384 +
	(ops.size() + oids.size() + cids.size() + lengths.size()) * 4096;
      for (vector<bufferlist>::iterator p = bls.begin(); p != bls.end(); p++)
	s += bls.size() + 4096;
      return s;
    }

    bool empty() {
      return ops.empty();
    }
    void clear_data() {
      bls.clear();
    }

    bool have_op() {
      return opp < ops.size();
    }
    int get_num_ops() { return ops.size(); }
    int get_op() {
      return ops[opp++];
    }
    bufferlist &get_bl() {
      return bls[blp++];
    }
    sobject_t get_oid() {
      return oids[oidp++];
    }
    coll_t get_cid() {
      return cids[cidp++];
    }
    __u64 get_length() {
      return lengths[lengthp++];
    }
    const char *get_attrname() {
      return attrnames[attrnamep++].c_str();
    }
    map<nstring,bufferptr>& get_attrset() {
      return attrsets[attrsetp++];
    }

    void start_sync() {
      int op = OP_STARTSYNC;
      ops.push_back(op);
    }
    void touch(coll_t cid, const sobject_t& oid) {
      int op = OP_TOUCH;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      //len++;
      //blen += 3;
    }
    void write(coll_t cid, const sobject_t& oid, __u64 off, size_t len, const bufferlist& bl) {
      int op = OP_WRITE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      lengths.push_back(off);
      lengths.push_back(len);
      bls.push_back(bl);
      //len++;
      //blen += 3 + bl.buffers().size();
    }
    void zero(coll_t cid, const sobject_t& oid, __u64 off, size_t len) {
      int op = OP_ZERO;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      lengths.push_back(off);
      lengths.push_back(len);
      //len++;
      //blen += 3 + 1;
    }
    void trim_from_cache(coll_t cid, const sobject_t& oid, __u64 off, size_t len) {
      int op = OP_TRIMCACHE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      lengths.push_back(off);
      lengths.push_back(len);
      //len++;
    }
    void truncate(coll_t cid, const sobject_t& oid, __u64 off) {
      int op = OP_TRUNCATE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      lengths.push_back(off);
      //len++;
      //blen++;
    }
    void remove(coll_t cid, const sobject_t& oid) {
      int op = OP_REMOVE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      //len++;
      //blen++;
    }
    void setattr(coll_t cid, const sobject_t& oid, const char* name, const void* val, int len) {
      nstring n(name);
      bufferlist bl;
      bl.append((char*)val, len);
      setattr(cid, oid, n, bl);
    }
    void setattr(coll_t cid, const sobject_t& oid, const char* name, bufferlist& val) {
      nstring n(name);
      setattr(cid, oid, n, val);
    }
    void setattr(coll_t cid, const sobject_t& oid, nstring& s, bufferlist& val) {
      int op = OP_SETATTR;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      attrnames.push_back(nstring());
      attrnames.back().swap(s);
      bls.push_back(val);
      //len++;
      //blen++;
    }
    void setattrs(coll_t cid, const sobject_t& oid, map<nstring,bufferptr>& attrset) {
      map<nstring,bufferptr> empty;
      int op = OP_SETATTRS;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      attrsets.push_back(empty);
      attrsets.back().swap(attrset);
      //len++;
      //blen += 5 + attrset.size();     // HACK allowance for removing old attrs
    }
    void rmattr(coll_t cid, const sobject_t& oid, const char *name) {
      nstring n(name);
      rmattr(cid, oid, n);
    }
    void rmattr(coll_t cid, const sobject_t& oid, nstring& s) {
      int op = OP_RMATTR;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      attrnames.push_back(nstring());
      attrnames.back().swap(s);
      //len++;
      //blen++;
    }
    void rmattrs(coll_t cid, const sobject_t& oid) {
      int op = OP_RMATTRS;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
    }
    void clone(coll_t cid, const sobject_t& oid, sobject_t noid) {
      int op = OP_CLONE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      oids.push_back(noid);
      //len++;
      //blen += 5;
    }
    void clone_range(coll_t cid, const sobject_t& oid, sobject_t noid, __u64 off, __u64 len) {
      int op = OP_CLONERANGE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      oids.push_back(noid);
      lengths.push_back(off);
      lengths.push_back(len);
      //len++;
      //blen += 5;
    }
    void create_collection(coll_t cid) {
      int op = OP_MKCOLL;
      ops.push_back(op);
      cids.push_back(cid);
      //len++;
      //blen++;
    }
    void remove_collection(coll_t cid) {
      int op = OP_RMCOLL;
      ops.push_back(op);
      cids.push_back(cid);
      //len++;
      //blen++;
    }
    void collection_add(coll_t cid, coll_t ocid, const sobject_t& oid) {
      int op = OP_COLL_ADD;
      ops.push_back(op);
      cids.push_back(cid);
      cids.push_back(ocid);
      oids.push_back(oid);
      //len++;
      //blen++;
    }
    void collection_remove(coll_t cid, const sobject_t& oid) {
      int op = OP_COLL_REMOVE;
      ops.push_back(op);
      cids.push_back(cid);
      oids.push_back(oid);
      //len++;
      //blen++;
   }
    void collection_setattr(coll_t cid, const char* name, const void* val, int len) {
      bufferlist bl;
      bl.append((char*)val, len);
      collection_setattr(cid, name, bl);
    }
    void collection_setattr(coll_t cid, const char* name, bufferlist& val) {
      nstring n(name);
      collection_setattr(cid, n, val);
    }
    void collection_setattr(coll_t cid, nstring& name, bufferlist& val) {
      int op = OP_COLL_SETATTR;
      ops.push_back(op);
      cids.push_back(cid);
      attrnames.push_back(nstring());
      attrnames.back().swap(name);
      bls.push_back(val);
      //len++;
      //blen++;
    }

    void collection_rmattr(coll_t cid, const char* name) {
      nstring n(name);
      collection_rmattr(cid, n);
    }
    void collection_rmattr(coll_t cid, nstring& name) {
      int op = OP_COLL_RMATTR;
      ops.push_back(op);
      cids.push_back(cid);
      attrnames.push_back(nstring());
      attrnames.back().swap(name);
      //len++;
      //blen++;
    }
    void collection_setattrs(coll_t cid, map<nstring,bufferptr>& aset) {
      int op = OP_COLL_SETATTRS;
      ops.push_back(op);
      cids.push_back(cid);
      attrsets.push_back(aset);
      //len++;
      //blen += 5 + aset.size();
    }


    // etc.
    Transaction() :
      //len(0),
      opp(0), blp(0), oidp(0), cidp(0), lengthp(0), attrnamep(0), attrsetp(0) {}
    Transaction(bufferlist::iterator &p) : 
      //len(0),
      opp(0), blp(0), oidp(0), cidp(0), lengthp(0), attrnamep(0), attrsetp(0)
    { decode(p); }
    Transaction(bufferlist &bl) : 
      //len(0),
      opp(0), blp(0), oidp(0), cidp(0), lengthp(0), attrnamep(0), attrsetp(0) { 
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
      ::decode(attrnames, bl);
      /*for (vector<nstring>::iterator p = attrnames2.begin();
	   p != attrnames2.end();
	   ++p)
	   attrnames.push_back((*p).c_str());*/
      ::decode(attrsets, bl);
    }
  };

  struct C_DeleteTransaction : public Context {
    ObjectStore::Transaction *t;
    C_DeleteTransaction(ObjectStore::Transaction *tt) : t(tt) {}
    void finish(int r) {
      delete t;
    }
  };


  virtual unsigned apply_transaction(Transaction& t, Context *onjournal=0, Context *ondisk=0) = 0;
  virtual unsigned apply_transactions(list<Transaction*>& tls, Context *onjournal=0, Context *ondisk=0) = 0;

  virtual int queue_transaction(Transaction* t) = 0;
  virtual int queue_transaction(Transaction *t, Context *onreadable, Context *onjournal=0, Context *ondisk=0) {
    list<Transaction*> tls;
    tls.push_back(t);
    return queue_transactions(tls, onreadable, onjournal, ondisk);
  }
  virtual int queue_transactions(list<Transaction*>& tls, Context *onreadable,
				 Context *onjournal=0, Context *ondisk=0) = 0;



 public:
  ObjectStore() {}
  virtual ~ObjectStore() {}

  // mgmt
  virtual int mount() = 0;
  virtual int umount() = 0;
  virtual int mkfs() = 0;  // wipe
  virtual int mkjournal() = 0; // journal only

  virtual int statfs(struct statfs *buf) = 0;

  // objects
  virtual bool exists(coll_t cid, const sobject_t& oid) = 0;                   // useful?
  virtual int stat(coll_t cid, const sobject_t& oid, struct stat *st) = 0;     // struct stat?
  virtual int read(coll_t cid, const sobject_t& oid, __u64 offset, size_t len, bufferlist& bl) = 0;

  /*
  virtual int _remove(coll_t cid, sobject_t oid) = 0;
  virtual int _truncate(coll_t cid, sobject_t oid, __u64 size) = 0;
  virtual int _write(coll_t cid, sobject_t oid, __u64 offset, size_t len, const bufferlist& bl) = 0;
  virtual int _zero(coll_t cid, sobject_t oid, __u64 offset, size_t len) {
    // write zeros.. yuck!
    bufferptr bp(len);
    bufferlist bl;
    bl.push_back(bp);
    return _write(cid, oid, offset, len, bl);
  }
  */

  virtual void trim_from_cache(coll_t cid, const sobject_t& oid, __u64 offset, size_t len) = 0; //{ }
  virtual int is_cached(coll_t cid, const sobject_t& oid, __u64 offset, size_t len) = 0;  //{ return -1; }

  virtual int getattr(coll_t cid, const sobject_t& oid, const char *name, void *value, size_t size) = 0;
  virtual int getattr(coll_t cid, const sobject_t& oid, const char *name, bufferptr& value) = 0;
  int getattr(coll_t cid, const sobject_t& oid, const char *name, bufferlist& value) {
    bufferptr bp;
    int r = getattr(cid, oid, name, bp);
    if (bp.length())
      value.push_back(bp);
    return r;
  }
  virtual int getattrs(coll_t cid, const sobject_t& oid, map<nstring,bufferptr>& aset, bool user_only = false) {return 0;};

  /*
  virtual int _setattr(coll_t cid, sobject_t oid, const char *name, const void *value, size_t size) = 0;
  virtual int _setattr(coll_t cid, sobject_t oid, const char *name, const bufferptr &bp) {
    return _setattr(cid, oid, name, bp.c_str(), bp.length());
  }
  virtual int _setattrs(coll_t cid, sobject_t oid, map<string,bufferptr>& aset) = 0;
  virtual int _rmattr(coll_t cid, sobject_t oid, const char *name) {return 0;}
  virtual int _clone(coll_t cid, sobject_t oid, sobject_t noid) {
    return -1;
  }
  */
    
  // collections
  virtual int list_collections(vector<coll_t>& ls) = 0;
  virtual bool collection_exists(coll_t c) = 0;
  virtual int collection_getattr(coll_t cid, const char *name,
                                 void *value, size_t size) = 0;
  virtual int collection_getattr(coll_t cid, const char *name, bufferlist& bl) = 0;
  virtual int collection_getattrs(coll_t cid, map<nstring,bufferptr> &aset) = 0;
  virtual bool collection_empty(coll_t c) = 0;
  virtual int collection_list_partial(coll_t c, snapid_t seq, vector<sobject_t>& o, int count, collection_list_handle_t *handle) = 0;
  virtual int collection_list(coll_t c, vector<sobject_t>& o) = 0;

  /*
  virtual int _create_collection(coll_t c) = 0;
  virtual int _destroy_collection(coll_t c) = 0;
  virtual int _collection_add(coll_t c, coll_t ocid, sobject_t o) = 0;
  virtual int _collection_remove(coll_t c, sobject_t o) = 0;
  virtual int _collection_setattr(coll_t cid, const char *name,
                                 const void *value, size_t size) = 0;
  virtual int _collection_rmattr(coll_t cid, const char *name) = 0;
  virtual int _collection_setattrs(coll_t cid, map<string,bufferptr> &aset) = 0;
  */

  virtual void sync(Context *onsync) {}
  virtual void sync() {}
  virtual void flush() {}
  virtual void sync_and_flush() {}
    
  virtual void _fake_writes(bool b) {};
  virtual void _get_frag_stat(FragmentationStat& st) {};
  
};


WRITE_CLASS_ENCODER(ObjectStore::Transaction)

#endif
