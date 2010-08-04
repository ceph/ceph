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


#ifndef CEPH_OBJECTSTORE_H
#define CEPH_OBJECTSTORE_H

#include "include/types.h"
#include "include/Context.h"
#include "include/buffer.h"

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
using std::string;

#ifndef MIN
# define MIN(a,b) ((a) < (b) ? (a):(b))
#endif

typedef uint64_t collection_list_handle_t;

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
  

  struct Sequencer {
    void *p;
    Sequencer() : p(NULL) {}
    ~Sequencer() {
      assert(p == NULL);
    }
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
    uint64_t ops, bytes;
    uint32_t largest_data_len, largest_data_off, largest_data_off_in_tbl;
    bufferlist tbl;
    bufferlist::iterator p;

    // -- old format --
    bool old;
    vector<int8_t> opvec;
    vector<bufferlist> bls;
    vector<sobject_t> oids;
    vector<coll_t> cids;
    vector<int64_t> lengths;

    // for these guys, just use a pointer.
    // but, decode to a full value, and create pointers to that.
    //vector<const char*> attrnames;
    vector<string> attrnames;
    deque<map<std::string,bufferptr> > attrsets;

    unsigned opp, blp, oidp, cidp, lengthp, attrnamep, attrsetp;

  public:
    uint64_t get_encoded_bytes() {
      return 1 + 8 + 8 + 4 + 4 + 4 + 4 + tbl.length();
    }

    uint64_t get_num_bytes() {
      if (old) {
	uint64_t s = 16384 +
	  (opvec.size() + oids.size() + cids.size() + lengths.size()) * 4096;
	for (vector<bufferlist>::iterator p = bls.begin(); p != bls.end(); p++)
	  s += bls.size() + 4096;
	return s;
      }
      return bytes;
    }

    uint32_t get_data_length() {
      return largest_data_len;
    }
    uint32_t get_data_offset() {
      if (largest_data_off_in_tbl) {
	return largest_data_off_in_tbl +
	  sizeof(__u8) +  // struct_v
	  sizeof(ops) +
	  sizeof(bytes) +
	  sizeof(largest_data_len) +
	  sizeof(largest_data_off) +
	  sizeof(largest_data_off_in_tbl) +
	  sizeof(__u32);  // tbl length
      }
      return 0;  // none
    }
    int get_data_alignment() {
      if (!largest_data_len)
	return -1;
      return (largest_data_off - get_data_offset()) & ~PAGE_MASK;
    }

    bool empty() {
      if (old)
	return opvec.empty();
      return !ops;
    }

    bool have_op() {
      if (old)
	return opp < opvec.size();
      if (p.get_off() == 0)
	p = tbl.begin();
      return !p.end();
    }
    int get_num_ops() {
      if (old)
	return opvec.size();
      return ops;
    }
    int get_op() {
      if (old)
	return opvec[opp++];
      if (p.get_off() == 0)
	p = tbl.begin();
      __u32 op;
      ::decode(op, p);
      return op;
    }
    void get_bl(bufferlist& bl) {
      if (old) {
	bl = bls[blp++];
	return;
      }
      if (p.get_off() == 0)
	p = tbl.begin();
      ::decode(bl, p);
    }
    sobject_t get_oid() {
      if (old)
	return oids[oidp++];
      if (p.get_off() == 0)
	p = tbl.begin();
      sobject_t soid;
      ::decode(soid, p);
      return soid;
    }
    coll_t get_cid() {
      if (old)
	return cids[cidp++];
      if (p.get_off() == 0)
	p = tbl.begin();
      coll_t c;
      ::decode(c, p);
      return c;
    }
    uint64_t get_length() {
      if (old)
	return lengths[lengthp++];
      if (p.get_off() == 0)
	p = tbl.begin();
      uint64_t len;
      ::decode(len, p);
      return len;
    }
    string get_attrname() {
      if (old)
	return string(attrnames[attrnamep++].c_str());
      if (p.get_off() == 0)
	p = tbl.begin();
      string s;
      ::decode(s, p);
      return s;
    }
    void get_attrset(map<string,bufferptr>& aset) {
      if (old) {
	aset = attrsets[attrsetp++];
	return;
      }
      if (p.get_off() == 0)
	p = tbl.begin();
      ::decode(aset, p);
    }

    // -----------------------------

    void start_sync() {
      __u32 op = OP_STARTSYNC;
      ::encode(op, tbl);
      ops++;
    }
    void touch(coll_t cid, const sobject_t& oid) {
      __u32 op = OP_TOUCH;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void write(coll_t cid, const sobject_t& oid, uint64_t off, uint64_t len, const bufferlist& data) {
      __u32 op = OP_WRITE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(off, tbl);
      ::encode(len, tbl);
      assert(len == data.length());
      if (data.length() > largest_data_len) {
	largest_data_len = data.length();
	largest_data_off = off;
	largest_data_off_in_tbl = tbl.length() + sizeof(__u32);  // we are about to 
      }
      ::encode(data, tbl);
      ops++;
    }
    void zero(coll_t cid, const sobject_t& oid, uint64_t off, uint64_t len) {
      __u32 op = OP_ZERO;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(off, tbl);
      ::encode(len, tbl);
      ops++;
    }
    void trim_from_cache(coll_t cid, const sobject_t& oid, uint64_t off, uint64_t len) {
      __u32 op = OP_TRIMCACHE;
       ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(off, tbl);
      ::encode(len, tbl);
      ops++;
    }
    void truncate(coll_t cid, const sobject_t& oid, uint64_t off) {
      __u32 op = OP_TRUNCATE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(off, tbl);
      ops++;
    }
    void remove(coll_t cid, const sobject_t& oid) {
      __u32 op = OP_REMOVE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void setattr(coll_t cid, const sobject_t& oid, const char* name, const void* val, int len) {
      string n(name);
      bufferlist bl;
      bl.append((char*)val, len);
      setattr(cid, oid, n, tbl);
    }
    void setattr(coll_t cid, const sobject_t& oid, const char* name, bufferlist& val) {
      string n(name);
      setattr(cid, oid, n, val);
    }
    void setattr(coll_t cid, const sobject_t& oid, string& s, bufferlist& val) {
      __u32 op = OP_SETATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(s, tbl);
      ::encode(val, tbl);
      ops++;
    }
    void setattrs(coll_t cid, const sobject_t& oid, map<string,bufferptr>& attrset) {
      __u32 op = OP_SETATTRS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(attrset, tbl);
      ops++;
    }
    void rmattr(coll_t cid, const sobject_t& oid, const char *name) {
      string n(name);
      rmattr(cid, oid, n);
    }
    void rmattr(coll_t cid, const sobject_t& oid, string& s) {
      __u32 op = OP_RMATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(s, tbl);
      ops++;
    }
    void rmattrs(coll_t cid, const sobject_t& oid) {
      __u32 op = OP_RMATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void clone(coll_t cid, const sobject_t& oid, sobject_t noid) {
      __u32 op = OP_CLONE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(noid, tbl);
      ops++;
    }
    void clone_range(coll_t cid, const sobject_t& oid, sobject_t noid, uint64_t off, uint64_t len) {
      __u32 op = OP_CLONERANGE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(noid, tbl);
      ::encode(off, tbl);
      ::encode(len, tbl);
      ops++;
    }
    void create_collection(coll_t cid) {
      __u32 op = OP_MKCOLL;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ops++;
    }
    void remove_collection(coll_t cid) {
      __u32 op = OP_RMCOLL;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ops++;
    }
    void collection_add(coll_t cid, coll_t ocid, const sobject_t& oid) {
      __u32 op = OP_COLL_ADD;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(ocid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void collection_remove(coll_t cid, const sobject_t& oid) {
      __u32 op = OP_COLL_REMOVE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void collection_setattr(coll_t cid, const char* name, const void* val, int len) {
      bufferlist bl;
      bl.append((char*)val, len);
      collection_setattr(cid, name, tbl);
    }
    void collection_setattr(coll_t cid, const char* name, bufferlist& val) {
      string n(name);
      collection_setattr(cid, n, val);
    }
    void collection_setattr(coll_t cid, string& name, bufferlist& val) {
      __u32 op = OP_COLL_SETATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(name, tbl);
      ::encode(val, tbl);
      ops++;
    }

    void collection_rmattr(coll_t cid, const char* name) {
      string n(name);
      collection_rmattr(cid, n);
    }
    void collection_rmattr(coll_t cid, string& name) {
      __u32 op = OP_COLL_RMATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(name, tbl);
      ops++;
    }
    void collection_setattrs(coll_t cid, map<string,bufferptr>& aset) {
      __u32 op = OP_COLL_SETATTRS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(aset, tbl);
      ops++;
    }


    // etc.
    Transaction() :
      ops(0), bytes(0), largest_data_len(0), largest_data_off(0), largest_data_off_in_tbl(0),
      old(false), opp(0), blp(0), oidp(0), cidp(0), lengthp(0), attrnamep(0), attrsetp(0) { }
    Transaction(bufferlist::iterator &dp) :
      ops(0), bytes(0), largest_data_len(0), largest_data_off(0), largest_data_off_in_tbl(0),
      old(false), opp(0), blp(0), oidp(0), cidp(0), lengthp(0), attrnamep(0), attrsetp(0) {
      decode(dp);
    }
    Transaction(bufferlist &nbl) :
      ops(0), bytes(0), largest_data_len(0), largest_data_off(0), largest_data_off_in_tbl(0),
      old(false), opp(0), blp(0), oidp(0), cidp(0), lengthp(0), attrnamep(0), attrsetp(0) {
      bufferlist::iterator dp = nbl.begin();
      decode(dp); 
    }

    void encode(bufferlist& bl) const {
      __u8 struct_v = 3;
      ::encode(struct_v, bl);
      ::encode(ops, bl);
      ::encode(bytes, bl);
      ::encode(largest_data_len, bl);
      ::encode(largest_data_off, bl);
      ::encode(largest_data_off_in_tbl, bl);
      ::encode(tbl, bl);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      if (struct_v == 1) {
	// old <= v0.19 format
	old = true;
	::decode(opvec, bl);
	::decode(bls, bl);
	::decode(oids, bl);
	::decode(cids, bl);
	::decode(lengths, bl);
	::decode(attrnames, bl);
	/*for (vector<string>::iterator p = attrnames2.begin();
          p != attrnames2.end();
          ++p)
          attrnames.push_back((*p).c_str());*/
	::decode(attrsets, bl);
      } else {
	assert(struct_v <= 3);
	::decode(ops, bl);
	::decode(bytes, bl);
	if (struct_v >= 3) {
	  ::decode(largest_data_len, bl);
	  ::decode(largest_data_off, bl);
	  ::decode(largest_data_off_in_tbl, bl);
	}
	::decode(tbl, bl);
      }
    }
  };

  struct C_DeleteTransaction : public Context {
    ObjectStore::Transaction *t;
    C_DeleteTransaction(ObjectStore::Transaction *tt) : t(tt) {}
    void finish(int r) {
      delete t;
    }
  };


  virtual unsigned apply_transaction(Transaction& t, Context *ondisk=0) = 0;
  virtual unsigned apply_transactions(list<Transaction*>& tls, Context *ondisk=0) = 0;

  virtual int queue_transaction(Sequencer *osr, Transaction* t) = 0;
  virtual int queue_transaction(Sequencer *osr, Transaction *t, Context *onreadable, Context *ondisk=0,
				Context *onreadable_sync=0) {
    list<Transaction*> tls;
    tls.push_back(t);
    return queue_transactions(osr, tls, onreadable, ondisk, onreadable_sync);
  }
  virtual int queue_transactions(Sequencer *osr, list<Transaction*>& tls, Context *onreadable, Context *ondisk=0,
				 Context *onreadable_sync=0) = 0;



 public:
  ObjectStore() {}
  virtual ~ObjectStore() {}

  // mgmt
  virtual bool test_mount_in_use() = 0;
  virtual int mount() = 0;
  virtual int umount() = 0;
  virtual int mkfs() = 0;  // wipe
  virtual int mkjournal() = 0; // journal only

  virtual int statfs(struct statfs *buf) = 0;

  // objects
  virtual bool exists(coll_t cid, const sobject_t& oid) = 0;                   // useful?
  virtual int stat(coll_t cid, const sobject_t& oid, struct stat *st) = 0;     // struct stat?
  virtual int read(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len, bufferlist& bl) = 0;
  virtual int fiemap(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len, bufferlist& bl) = 0;

  /*
  virtual int _remove(coll_t cid, sobject_t oid) = 0;
  virtual int _truncate(coll_t cid, sobject_t oid, uint64_t size) = 0;
  virtual int _write(coll_t cid, sobject_t oid, uint64_t offset, size_t len, const bufferlist& bl) = 0;
  virtual int _zero(coll_t cid, sobject_t oid, uint64_t offset, size_t len) {
    // write zeros.. yuck!
    bufferptr bp(len);
    bufferlist bl;
    bl.push_back(bp);
    return _write(cid, oid, offset, len, bl);
  }
  */

  virtual void trim_from_cache(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len) = 0; //{ }
  virtual int is_cached(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len) = 0;  //{ return -1; }

  virtual int getattr(coll_t cid, const sobject_t& oid, const char *name, void *value, size_t size) = 0;
  virtual int getattr(coll_t cid, const sobject_t& oid, const char *name, bufferptr& value) = 0;
  int getattr(coll_t cid, const sobject_t& oid, const char *name, bufferlist& value) {
    bufferptr bp;
    int r = getattr(cid, oid, name, bp);
    if (bp.length())
      value.push_back(bp);
    return r;
  }
  virtual int getattrs(coll_t cid, const sobject_t& oid, map<string,bufferptr>& aset, bool user_only = false) {return 0;};

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
  virtual int collection_getattrs(coll_t cid, map<string,bufferptr> &aset) = 0;
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
