#ifndef __ANCHORTABLE_H
#define __ANCHORTABLE_H

#include "include/types.h"
#include "include/Context.h"

#include <ext/hash_map>
using namespace __gnu_cxx;


class Anchor {
public:
  inodeno_t ino;      // my ino
  inodeno_t dirino;   // containing dir
  string    ref_dn;   // referring dentry
  int       nref;     // reference count

  Anchor() {}
  Anchor(inodeno_t ino, inodeno_t dirino, string& ref_dn, int nref=0) {
	this->ino = ino;
	this->dirino = dirino;
	this->ref_dn = ref_dn;
	this->nref = nref;
  }  

  void _rope(crope& r) {
	r.append((char*)this, sizeof(*this));
  }
  void _unrope(crope& r, int& off) {
	r.copy(off, sizeof(*this), (char*)this);
	off += sizeof(*this);
  }
} ;


class AnchorTable {
  MDS *mds;
  hash_map<inodeno_t, Anchor*>  anchor_map;

  // load/save entire table for now!
  void load(Context *onfinish);
  void save(Context *onfinish);

  bool opened;

  // remote state
  hash_map<inodeno_t, Context*>  pending_op;
  hash_map<inodeno_t, Context*>  pending_lookup_context;
  hash_map<inodeno_t, vector<Anchor*>*>  pending_lookup_trace;

 public:
  AnchorTable(MDS *mds) : opened(false) {
	this->mds = mds;

  	opened = true;  // for now, all in memory   HACK FIXME BLAH
  }

 protected:
  // 
  bool have_ino(inodeno_t ino) { 
	return true;                  // always in memory for now.
  } 
  void fetch_ino(inodeno_t ino, Context *onfinish) {
	assert(!opened);
	load(onfinish);
  }

  // adjust table
  bool add(inodeno_t ino, inodeno_t dirino, string& ref_dn);
  void inc(inodeno_t ino);
  void dec(inodeno_t ino);

  
  // high level interface
  void lookup(inodeno_t ino, vector<Anchor*>& trace);
  void create(inodeno_t ino, vector<Anchor*>& trace);
  void destroy(inodeno_t ino);

  // messages
 public:
  void proc_message(class Message *m);
 protected:
  void handle_anchor_request(class MAnchorRequest *m);  
  void handle_anchor_reply(class MAnchorReply *m);  


 public:
  // user interface
  void lookup(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish);
  void create(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish);
  void destroy(inodeno_t ino, Context *onfinish);

};

#endif
