#ifndef __EBOFS_BUFFERCACHE_H
#define __EBOFS_BUFFERCACHE_H

#include "include/lru.h"
#include "include/Context.h"

#include "common/Clock.h"

#include "types.h"
#include "AlignedBufferPool.h"
#include "BlockDevice.h"

#include "include/interval_set.h"

class ObjectCache;
class BufferCache;
class Onode;

class BufferHead : public LRUObject {
 public:
  /*
   * - buffer_heads should always break across disk extent boundaries
   * - partial buffer_heads are always 1 block.
   */
  const static int STATE_MISSING = 0; //     missing; data is on disk, but not loaded.
  const static int STATE_CLEAN = 1;   // Rw  clean
  const static int STATE_DIRTY = 2;   // RW  dirty
  const static int STATE_TX = 3;      // Rw  flushing to disk
  const static int STATE_RX = 4;      //  w  reading from disk
  const static int STATE_PARTIAL = 5; // reading from disk, + partial content map.  always 1 block.

  class PartialWrite {
  public:
	map<off_t, bufferlist> partial;   // partial dirty content overlayed onto incoming data
	block_t                block;
	version_t              epoch;
  };

 public:
  ObjectCache *oc;

  bufferlist data;

  ioh_t     rx_ioh;         // 
  ioh_t     tx_ioh;         // 

  list<Context*> waitfor_read;
  list<Context*> waitfor_flush;

 private:
  map<off_t, bufferlist>     partial;   // partial dirty content overlayed onto incoming data

  map<block_t, PartialWrite> partial_write;  // queued writes w/ partial content

  int        ref;
  int        state;

 public:
  version_t  epoch_modified;
  
  version_t  version;        // current version in cache
  version_t  last_flushed;   // last version flushed to disk
 
  Extent     object_loc;     // block position _in_object_

  utime_t    dirty_stamp;

 public:
  BufferHead(ObjectCache *o) :
	oc(o), //cancellable_ioh(0), tx_epoch(0),
	rx_ioh(0), tx_ioh(0), 
	ref(0), state(STATE_MISSING), epoch_modified(0), version(0), last_flushed(0)
	{}
  
  ObjectCache *get_oc() { return oc; }

  int get() {
	assert(ref >= 0);
	if (ref == 0) lru_pin();
	return ++ref;
  }
  int put() {
	assert(ref > 0);
	if (ref == 1) lru_unpin();
	return --ref;
  }

  block_t start() { return object_loc.start; }
  void set_start(block_t s) { object_loc.start = s; }
  block_t length() { return object_loc.length; }
  void set_length(block_t l) { object_loc.length = l; }
  block_t end() { return start() + length(); }
  block_t last() { return end()-1; }
  
  version_t get_version() { return version; }
  void set_version(version_t v) { version = v; }
  version_t get_last_flushed() { return last_flushed; }
  void set_last_flushed(version_t v) { 
	assert(v > last_flushed);
	last_flushed = v; 
  }

  utime_t get_dirty_stamp() { return dirty_stamp; }
  void set_dirty_stamp(utime_t t) { dirty_stamp = t; }

  void set_state(int s) {
	if (s == STATE_PARTIAL || s == STATE_RX || s == STATE_TX) get();
	if (state == STATE_PARTIAL || state == STATE_RX || state == STATE_TX) put();
	state = s;
  }
  int get_state() { return state; }

  bool is_missing() { return state == STATE_MISSING; }
  bool is_dirty() { return state == STATE_DIRTY; }
  bool is_clean() { return state == STATE_CLEAN; }
  bool is_tx() { return state == STATE_TX; }
  bool is_rx() { return state == STATE_RX; }
  bool is_partial() { return state == STATE_PARTIAL; }
  
  bool is_partial_writes() { return !partial_write.empty(); }
  void finish_partials();
  void queue_partial_write(block_t b);


  void copy_partial_substr(off_t start, off_t end, bufferlist& bl) {
	map<off_t, bufferlist>::iterator i = partial.begin();
	
	// skip first bits (fully to left)
	while ((i->first + i->second.length() < start) &&
		   i != partial.end()) 
	  i++;
	assert(i != partial.end());
	assert(i->first <= start);
	
	// first
	unsigned bhoff = MAX(start, i->first) - i->first;
	unsigned bhlen = MIN(end-start, i->second.length());
	bl.substr_of( i->second, bhoff, bhlen );

	off_t pos = i->first + i->second.length();
	
	// have continuous to end?
	for (i++; i != partial.end(); i++) {
	  if (pos >= end) break;
	  assert(pos == i->first);

	  pos = i->first + i->second.length();

	  if (pos <= end) {	  // this whole frag
		bl.append( i->second );
	  } else {            // partial end
		unsigned bhlen = end-start-bl.length();
		bufferlist frag;
		frag.substr_of( i->second, 0, bhlen );
		bl.claim_append(frag);
		break;  // done.
	  }
	}
	
	assert(pos >= end);
	assert(bl.length() == (unsigned)(end-start));
  }

  bool have_partial_range(off_t start, off_t end) {
	map<off_t, bufferlist>::iterator i = partial.begin();

	// skip first bits (fully to left)
	while ((i->first + i->second.length() < start) &&
		   i != partial.end()) 
	  i++;
	if (i == partial.end()) return false;

	// have start?
	if (i->first > start) return false;
	off_t pos = i->first + i->second.length();

	// have continuous to end?
	for (i++; i != partial.end(); i++) {
	  assert(pos <= i->first);
	  if (pos < i->first) return false;
	  assert(pos == i->first);
	  pos = i->first + i->second.length();
	  if (pos >= end) break;  // gone far enough
	}

	if (pos >= end) return true;
	return false;
  }

  bool partial_is_complete(off_t size) {
	return have_partial_range( (off_t)(start()*EBOFS_BLOCK_SIZE),
							   MIN( size, (off_t)(end()*EBOFS_BLOCK_SIZE) ) );
	/*
	map<off_t, bufferlist>::iterator i = partial.begin();
	if (i == partial.end()) return false;
	if (i->first != (off_t)(object_loc.start * EBOFS_BLOCK_SIZE)) return false;
	off_t pos = i->first + i->second.length();
	for (i++; i != partial.end(); i++) {
	  assert(pos <= i->first);
	  if (pos < i->first) return false;
	  assert(pos == i->first);
	  pos = i->first + i->second.length();
	}
	off_t upto = MIN( size, (off_t)(end()*EBOFS_BLOCK_SIZE) );
	if (pos == upto) return true;
	return false;
	*/
  }
  void apply_partial() {
	apply_partial(data, partial);
  }
  void apply_partial(bufferlist& bl, map<off_t, bufferlist>& pm) {
	const off_t bhstart = start() * EBOFS_BLOCK_SIZE;
	//assert(partial_is_complete());
	for (map<off_t, bufferlist>::iterator i = pm.begin();
		 i != pm.end();
		 i++) {
	  int pos = i->first - bhstart;
	  bl.copy_in(pos, i->second.length(), i->second);
	}
	pm.clear();
  }
  void add_partial(off_t off, bufferlist& p) {
	// trim overlap
	unsigned len = p.length();
	for (map<off_t, bufferlist>::iterator i = partial.begin();
		 i != partial.end();
		 ) {
	  if (i->first > off+len) break;   // past affected area.

	  // same?
	  if (i->first == off && i->second.length() == len) {
		// replace it.
		partial.erase(i);   
		break;
	  }
	  // overlap all?
	  if (off < i->first && i->first + i->second.length() < off+len) {
		// erase it and move on.
		off_t dead = i->first;
		i++;
		partial.erase(dead);  
		continue;
	  }
	  // overlap tail?
	  if (i->first < off && i->first + i->second.length() > off) {
		// shorten.
		unsigned newlen = off - i->first;
		bufferlist o = i->second;
		i->second.substr_of(o, 0, newlen);
	  }
	  // overlap head?
	  if (off < i->first && i->first < off+len) {
		// move.
		off_t oldoff = i->first;
		off_t newoff = off+len;
		unsigned trim = newoff - oldoff;
		partial[newoff].substr_of(i->second, trim, i->second.length()-trim);
		i++;  // should be at newoff!
		partial.erase( oldoff );
	  }
	  i++;
	}

	// insert
	partial[off] = p;
  }


};

inline ostream& operator<<(ostream& out, BufferHead& bh)
{
  out << "bufferhead(" << bh.start() << "~" << bh.length();
  out << " v" << bh.get_version() << "/" << bh.get_last_flushed();
  if (bh.is_missing()) out << " missing";
  if (bh.is_dirty()) out << " dirty";
  if (bh.is_clean()) out << " clean";
  if (bh.is_rx()) out << " rx";
  if (bh.is_tx()) out << " tx";
  if (bh.is_partial()) out << " partial";
  out << ")";
  return out;
}


class ObjectCache {
 public:
  object_t object_id;
  BufferCache *bc;

 private:
  map<block_t, BufferHead*>  data;

 public:
  ObjectCache(object_t o, BufferCache *b) : object_id(o), bc(b) {}
  
  object_t get_object_id() { return object_id; }

  void add_bh(BufferHead *bh, block_t at) {
	data[at] = bh;
  }
  void remove_bh(BufferHead *bh) {
	assert(data.count(bh->start()));
	data.erase(bh->start());
  }
  bool is_empty() { return data.empty(); }

  int map_read(Onode *on,
			   block_t start, block_t len, 
			   map<block_t, BufferHead*>& hits,     // hits
			   map<block_t, BufferHead*>& missing,  // read these from disk
			   map<block_t, BufferHead*>& rx,       // wait for these to finish reading from disk
			   map<block_t, BufferHead*>& partial); // (maybe) wait for these to read from disk
  
  int map_write(Onode *on,
				block_t start, block_t len,
				interval_set<block_t>& alloc,
				map<block_t, BufferHead*>& hits);   // can write to these.

  BufferHead *split(BufferHead *bh, block_t off);

  int scan_versions(block_t start, block_t len,
					version_t& low, version_t& high);

  void rx_finish(ioh_t ioh, block_t start, block_t length);
  void tx_finish(ioh_t ioh, block_t start, block_t length, version_t v, version_t epoch);
  void partial_tx_finish(version_t epoch);
};

class C_OC_RxFinish : public Context {
  ObjectCache *oc;
  block_t start, length;
public:
  C_OC_RxFinish(ObjectCache *o, block_t s, block_t l) :
	oc(o), start(s), length(l) {}
  void finish(int r) {
	ioh_t ioh = (ioh_t)r;
	if (ioh)
	  oc->rx_finish(ioh, start, length);
  }  
};

class C_OC_TxFinish : public Context {
  ObjectCache *oc;
  block_t start, length;
  version_t version;
  version_t epoch;
public:
  C_OC_TxFinish(ObjectCache *o, block_t s, block_t l, version_t v, version_t e) :
	oc(o), start(s), length(l), version(v), epoch(e) {}
  void finish(int r) {
	ioh_t ioh = (ioh_t)r;
	if (ioh) {
	  oc->tx_finish(ioh, start, length, version, epoch);
	}
  }  
};

class C_OC_PartialTxFinish : public Context {
  ObjectCache *oc;
  version_t epoch;
public:
  C_OC_PartialTxFinish(ObjectCache *o, version_t e) :
	oc(o), epoch(e) {}
  void finish(int r) {
	ioh_t ioh = (ioh_t)r;
	if (ioh) {
	  oc->partial_tx_finish(epoch);
	}
  }  
};


class BufferCache {
 public:
  Mutex             &ebofs_lock;          // hack: this is a ref to global ebofs_lock
  BlockDevice       &dev;
  AlignedBufferPool &bufferpool;

  set<BufferHead*> dirty_bh;

  LRU   lru_dirty, lru_rest;

 private:
  Cond  stat_cond;
  int   stat_waiter;

  off_t stat_clean;
  off_t stat_dirty;
  off_t stat_rx;
  off_t stat_tx;
  off_t stat_partial;
  off_t stat_missing;

  map<version_t, int> epoch_unflushed;

 public:
  BufferCache(BlockDevice& d, AlignedBufferPool& bp, Mutex& el) : 
	ebofs_lock(el), dev(d), bufferpool(bp),
	stat_waiter(0),
	stat_clean(0), stat_dirty(0), stat_rx(0), stat_tx(0), stat_partial(0), stat_missing(0)
	{}

  // bh's in cache
  void add_bh(BufferHead *bh) {
	if (bh->is_dirty()) {
	  lru_dirty.lru_insert_mid(bh);
	  dirty_bh.insert(bh);
	} else
	  lru_rest.lru_insert_mid(bh);
	stat_add(bh);
  }
  void touch(BufferHead *bh) {
	if (bh->is_dirty()) {
	  lru_dirty.lru_touch(bh);
	} else
	  lru_rest.lru_touch(bh);
  }
  void remove_bh(BufferHead *bh) {
	stat_sub(bh);
	if (bh->is_dirty()) {
	  lru_dirty.lru_remove(bh);
	  dirty_bh.erase(bh);
	} else
	  lru_rest.lru_remove(bh);
  }

  // stats
  void stat_add(BufferHead *bh) {
	switch (bh->get_state()) {
	case BufferHead::STATE_MISSING: stat_missing += bh->length(); break;
	case BufferHead::STATE_CLEAN: stat_clean += bh->length(); break;
	case BufferHead::STATE_DIRTY: stat_dirty += bh->length(); break;
	case BufferHead::STATE_TX: stat_tx += bh->length(); break;
	case BufferHead::STATE_RX: stat_rx += bh->length(); break;
	case BufferHead::STATE_PARTIAL: stat_partial += bh->length(); break;
	}
	if (stat_waiter) stat_cond.Signal();
  }
  void stat_sub(BufferHead *bh) {
	switch (bh->get_state()) {
	case BufferHead::STATE_MISSING: stat_missing -= bh->length(); break;
	case BufferHead::STATE_CLEAN: stat_clean -= bh->length(); break;
	case BufferHead::STATE_DIRTY: stat_dirty -= bh->length(); break;
	case BufferHead::STATE_TX: stat_tx -= bh->length(); break;
	case BufferHead::STATE_RX: stat_rx -= bh->length(); break;
	case BufferHead::STATE_PARTIAL: stat_partial -= bh->length(); break;
	}
  }
  off_t get_stat_tx() { return stat_tx; }
  off_t get_stat_rx() { return stat_rx; }
  off_t get_stat_dirty() { return stat_dirty; }
  off_t get_stat_clean() { return stat_clean; }
  off_t get_stat_partial() { return stat_partial; }

  int get_unflushed(version_t epoch) {
	return epoch_unflushed[epoch];
  }
  void inc_unflushed(version_t epoch) {
	epoch_unflushed[epoch]++;
  }
  void dec_unflushed(version_t epoch) {
	epoch_unflushed[epoch]--;
	if (stat_waiter && 
		epoch_unflushed[epoch] == 0) 
	  stat_cond.Signal();
  }

  void waitfor_stat() {
	stat_waiter++;
	stat_cond.Wait(ebofs_lock);
	stat_waiter--;
  }


  // bh state
  void set_state(BufferHead *bh, int s) {
	// move between lru lists?
	if (s == BufferHead::STATE_DIRTY && bh->get_state() != BufferHead::STATE_DIRTY) {
	  lru_rest.lru_remove(bh);
	  lru_dirty.lru_insert_top(bh);
	  dirty_bh.insert(bh);
	}
	if (s != BufferHead::STATE_DIRTY && bh->get_state() == BufferHead::STATE_DIRTY) {
	  lru_dirty.lru_remove(bh);
	  lru_rest.lru_insert_mid(bh);
	  dirty_bh.erase(bh);
	}

	// set state
	stat_sub(bh);
	bh->set_state(s);
	stat_add(bh);
  }	  

  void copy_state(BufferHead *bh1, BufferHead *bh2) { 
	set_state(bh2, bh1->get_state());
  }
  
  void mark_missing(BufferHead *bh) { set_state(bh, BufferHead::STATE_MISSING); };
  void mark_clean(BufferHead *bh) { set_state(bh, BufferHead::STATE_CLEAN); };
  void mark_rx(BufferHead *bh) { set_state(bh, BufferHead::STATE_RX); };
  void mark_partial(BufferHead *bh) { set_state(bh, BufferHead::STATE_PARTIAL); };
  void mark_tx(BufferHead *bh) { set_state(bh, BufferHead::STATE_TX); };
  void mark_dirty(BufferHead *bh) { 
	set_state(bh, BufferHead::STATE_DIRTY); 
	bh->set_dirty_stamp(g_clock.now());
  };


  // io
  void bh_read(Onode *on, BufferHead *bh);
  void bh_write(Onode *on, BufferHead *bh);
  void bh_queue_partial_write(Onode *on, BufferHead *bh);

  bool bh_cancel_read(BufferHead *bh);
  bool bh_cancel_write(BufferHead *bh);

  friend class C_E_FlushPartial;


  BufferHead *split(BufferHead *orig, block_t after);


};



#endif
