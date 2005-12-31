
#include "BufferCache.h"
#include "Onode.h"


/*********** BufferHead **************/


#undef dout
#define dout(x)  if (x <= g_conf.debug_ebofs) cout << "ebofs.bh."


void BufferHead::finish_partials()
{
  dout(10) << "finish_partials on " << *this << endl;

  block_t cur_block = 0;

  // submit partial writes
  for (map<block_t, PartialWrite>::iterator p = partial_write.begin();
	   p != partial_write.end();
	   p++) {
	dout(10) << "finish_partials submitting queued write to " << p->second.block << endl;

	// copy raw buffer; this may be a past write
	bufferlist bl;
	bl.push_back( oc->bc->bufferpool.alloc(EBOFS_BLOCK_SIZE) );
	bl.copy_in(0, EBOFS_BLOCK_SIZE, data);
	apply_partial( bl, p->second.partial );
	
	if (tx_ioh && tx_block == p->first) {
	  assert(is_tx());
	  oc->bc->bh_cancel_write(this);
	}

	if (p->second.epoch == epoch_modified) {  // FIXME: this'll break when we add journaling!  ick.
	  // current epoch!  make like a bh_write.
	  cur_block = p->first;
	  dout(10) << "finish_partials  same epoch, doing a bh_write" << endl;
	} else {
	  // past epoch.  just write.
	  oc->bc->dev.write( p->second.block, 1, bl,
						 new C_OC_PartialTxFinish( oc->bc, p->second.epoch ),
						 "finish_partials");
	  //oc->get();  // don't need OC for completion func!
	}

  }
  partial_write.clear();

  if (cur_block) {
	// same as epoch_modified, so do a normal bh_write.  
	// assert: this should match the current onode's block
	apply_partial();
	oc->bc->mark_dirty(this);
	oc->bc->bh_write(oc->on, this);
	oc->bc->dec_unflushed(epoch_modified);  // undo the queued partial inc
  }
}

void BufferHead::cancel_partials()
{
  dout(10) << "cancel_partials on " << *this << endl;
  for (map<block_t, PartialWrite>::iterator p = partial_write.begin();
	   p != partial_write.end();
	   p++) {
 	oc->bc->dec_unflushed( p->second.epoch );
  }
}

void BufferHead::queue_partial_write(block_t b)
{
  if (partial_write.count(b)) {
	// overwrite previous partial write
	// note that it better be same epoch if it's the same block!!
	assert( partial_write[b].epoch == epoch_modified );
	partial_write.erase(b);
  } else {
	oc->bc->inc_unflushed( epoch_modified );
  }
  
  partial_write[ b ].partial = partial;
  partial_write[ b ].block = b;
  partial_write[ b ].epoch = epoch_modified;
}






/************ ObjectCache **************/


#undef dout
#define dout(x)  if (x <= g_conf.debug_ebofs) cout << "ebofs.oc."



void ObjectCache::rx_finish(ioh_t ioh, block_t start, block_t length)
{
  list<Context*> waiters;

  dout(10) << "rx_finish " << start << "~" << length << endl;
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
	   p != data.end(); 
	   p++) {
	BufferHead *bh = p->second;
	dout(10) << "rx_finish ?" << *bh << endl;
	assert(p->first == bh->start());

	// past?
	if (p->first >= start+length) break;
	if (bh->end() > start+length) break;  // past
	
	assert(p->first >= start);
	assert(bh->end() <= start+length);

	dout(10) << "rx_finish !" << *bh << endl;

	if (bh->rx_ioh == ioh)
	  bh->rx_ioh = 0;

	if (bh->is_partial_writes())
	  bh->finish_partials();
	
	if (bh->is_rx()) {
	  assert(bh->get_version() == 0);
	  assert(bh->end() <= start+length);
	  dout(10) << "rx_finish  rx -> clean on " << *bh << endl;
	  bc->mark_clean(bh);
	}
	else if (bh->is_partial()) {
	  assert(0);
	  /*
	  dout(10) << "rx_finish  partial -> clean on " << *bh << endl;	  
	  bh->apply_partial();
	  bc->mark_clean(bh);
	  */
	}
	else {
	  dout(10) << "rx_finish  ignoring status on (dirty|tx|clean) " << *bh << endl;
	  assert(bh->is_dirty() ||  // was overwritten
			 bh->is_tx() ||     // was overwritten and queued
			 bh->is_clean());   // was overwritten, queued, _and_ flushed to disk
	}

	// trigger waiters
	for (map<block_t,list<Context*> >::iterator p = bh->waitfor_read.begin();
		 p != bh->waitfor_read.end();
		 p++) {
	  assert(p->first >= bh->start() && p->first < bh->end());
	  waiters.splice(waiters.begin(), p->second);
	}
	bh->waitfor_read.clear();
  }	

  finish_contexts(waiters);
}


void ObjectCache::tx_finish(ioh_t ioh, block_t start, block_t length, 
							version_t version, version_t epoch)
{
  //list<Context*> waiters;
  
  dout(10) << "tx_finish " << start << "~" << length << " v" << version << endl;
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
	   p != data.end(); 
	   p++) {
	BufferHead *bh = p->second;
	dout(30) << "tx_finish ?bh " << *bh << endl;
	assert(p->first == bh->start());

	// past?
	if (p->first >= start+length) break;

	if (bh->tx_ioh == ioh)
	  bh->tx_ioh = 0;

	if (!bh->is_tx()) {
	  dout(10) << "tx_finish  bh not marked tx, skipping" << endl;
	  continue;
	}

	assert(bh->is_tx());
	assert(bh->end() <= start+length);
	
	if (version == bh->version) {
	  dout(10) << "tx_finish  tx -> clean on " << *bh << endl;
	  bh->set_last_flushed(version);
	  bc->mark_clean(bh);

	  // trigger waiters
	  //waiters.splice(waiters.begin(), bh->waitfor_flush);
	} else {
	  dout(10) << "tx_finish  leaving tx, " << bh->version << " > " << version 
			   << " on " << *bh << endl;
	}
  }	

  //finish_contexts(waiters);
}


/*
 * map a range of blocks into buffer_heads.
 * - create missing buffer_heads as necessary.
 *  - fragment along disk extent boundaries
 */

int ObjectCache::map_read(Onode *on,
						  block_t start, block_t len, 
						  map<block_t, BufferHead*>& hits,
						  map<block_t, BufferHead*>& missing,
						  map<block_t, BufferHead*>& rx,
						  map<block_t, BufferHead*>& partial) {
  
  map<block_t, BufferHead*>::iterator p = data.lower_bound(start);

  block_t cur = start;
  block_t left = len;
  
  if (p != data.begin() && 
	  (p == data.end() || p->first > cur)) {
	p--;     // might overlap!
	if (p->first + p->second->length() <= cur) 
	  p++;   // doesn't overlap.
  }

  while (left > 0) {
	// at end?
	if (p == data.end()) {
	  // rest is a miss.
	  vector<Extent> exv;
	  on->map_extents(cur, left, exv);          // we might consider some prefetch here.
	  for (unsigned i=0; i<exv.size(); i++) {
		BufferHead *n = new BufferHead(this);
		n->set_start( cur );
		n->set_length( exv[i].length );
		bc->add_bh(n);
		missing[cur] = n;
		dout(20) << "map_read miss " << *n << endl;
		cur += exv[i].length;
		left -= exv[i].length;
	  }
	  assert(left == 0);
	  assert(cur == start+len);
	  break;
	}
	
	if (p->first <= cur) {
	  // have it (or part of it)
	  BufferHead *e = p->second;
	  
	  if (e->is_clean() ||
		  e->is_dirty() ||
		  e->is_tx()) {
		hits[cur] = e;     // readable!
		dout(20) << "map_read hit " << *e << endl;
	  } 
	  else if (e->is_rx()) {
		rx[cur] = e;       // missing, not readable.
		dout(20) << "map_read rx " << *e << endl;
	  }
	  else if (e->is_partial()) {
		partial[cur] = e;
		dout(20) << "map_read partial " << *e << endl;
	  }
	  else assert(0);
	  
	  block_t lenfromcur = MIN(e->end() - cur, left);
	  cur += lenfromcur;
	  left -= lenfromcur;
	  p++;
	  continue;  // more?
	} else if (p->first > cur) {
	  // gap.. miss
	  block_t next = p->first;
	  vector<Extent> exv;
	  on->map_extents(cur, MIN(next-cur, left), exv);   // we might consider some prefetch here

	  for (unsigned i=0; i<exv.size(); i++) {
		BufferHead *n = new BufferHead(this);
		n->set_start( cur );
		n->set_length( exv[i].length );
		bc->add_bh(n);
		missing[cur] = n;
		cur += n->length();
		left -= n->length();
		dout(20) << "map_read gap " << *n << endl;
	  }
	  continue;    // more?
	}
	else 
	  assert(0);
  }

  assert(left == 0);
  assert(cur == start+len);
  return 0;  
}


/*
 * map a range of pages on an object's buffer cache.
 *
 * - break up bufferheads that don't fall completely within the range
 * - cancel rx ops we obsolete.
 *   - resubmit rx ops if we split bufferheads
 *
 * - leave potentially obsoleted tx ops alone (for now)
 * - don't worry about disk extent boundaries (yet)
 */
int ObjectCache::map_write(Onode *on, 
						   block_t start, block_t len,
						   interval_set<block_t>& alloc,
						   map<block_t, BufferHead*>& hits)
{
  map<block_t, BufferHead*>::iterator p = data.lower_bound(start);

  dout(10) << "map_write " << *on << " " << start << "~" << len << " ... alloc " << alloc << endl;
  // p->first >= start
  
  block_t cur = start;
  block_t left = len;
  
  if (p != data.begin() && 
	  (p == data.end() || p->first > cur)) {
	p--;     // might overlap!
	if (p->first + p->second->length() <= cur) 
	  p++;   // doesn't overlap.
  }

  //dump();

  while (left > 0) {
	// max for this bh (bc of (re)alloc on disk)
	block_t max = left;
	bool newalloc = false;

	// based on alloc/no-alloc boundary ...
	if (alloc.contains(cur, left)) {
	  if (alloc.contains(cur)) {
		block_t ends = alloc.end_after(cur);
		max = MIN(left, ends-cur);
		newalloc = true;
	  } else {
		if (alloc.starts_after(cur)) {
		  block_t st = alloc.start_after(cur);
		  max = MIN(left, st-cur);
		} 
	  }
	} 

	// based on disk extent boundary ...
	vector<Extent> exv;
	on->map_extents(cur, max, exv);
	if (exv.size() > 1) 
	  max = exv[0].length;

	if (newalloc) {
	  dout(10) << "map_write " << cur << "~" << max << " is new alloc on disk" << endl;
	} else {
	  dout(10) << "map_write " << cur << "~" << max << " keeps old alloc on disk" << endl;
	}
	
	// at end?
	if (p == data.end()) {
	  BufferHead *n = new BufferHead(this);
	  n->set_start( cur );
	  n->set_length( max );
	  bc->add_bh(n);
	  hits[cur] = n;
	  left -= max;
	  cur += max;
	  continue;
	}
	
	dout(10) << "p is " << *p->second << endl;


	if (p->first <= cur) {
	  BufferHead *bh = p->second;
	  dout(10) << "map_write bh " << *bh << " intersected" << endl;

	  if (p->first < cur) {
		if (cur+max >= p->first+p->second->length()) {
		  // we want right bit (one splice)
		  if (bh->is_rx() && bc->bh_cancel_read(bh)) {
			BufferHead *right = bc->split(bh, cur);
			bc->bh_read(on, bh);          // reread left bit
			bh = right;
		  } else if (bh->is_tx() && !newalloc && bc->bh_cancel_write(bh)) {
			BufferHead *right = bc->split(bh, cur);
			bc->bh_write(on, bh);          // rewrite left bit
			bh = right;
		  } else {
			bh = bc->split(bh, cur);   // just split it
		  }
		  p++;
		  assert(p->second == bh);
		} else {
		  // we want middle bit (two splices)
		  if (bh->is_rx() && bc->bh_cancel_read(bh)) {
			BufferHead *middle = bc->split(bh, cur);
			bc->bh_read(on, bh);                       // reread left
			p++;
			assert(p->second == middle);
			BufferHead *right = bc->split(middle, cur+max);
			bc->bh_read(on, right);                    // reread right
			bh = middle;
		  } else if (bh->is_tx() && !newalloc && bc->bh_cancel_write(bh)) {
			BufferHead *middle = bc->split(bh, cur);
			bc->bh_write(on, bh);                       // redo left
			p++;
			assert(p->second == middle);
			BufferHead *right = bc->split(middle, cur+max);
			bc->bh_write(on, right);                    // redo right
			bh = middle;
		  } else {
			BufferHead *middle = bc->split(bh, cur);
			p++;
			assert(p->second == middle);
			bc->split(middle, cur+max);
			bh = middle;
		  }
		}
	  } else if (p->first == cur) {
		if (p->second->length() <= max) {
		  // whole bufferhead, piece of cake.
		} else {
		  // we want left bit (one splice)
		  if (bh->is_rx() && bc->bh_cancel_read(bh)) {
			BufferHead *right = bc->split(bh, cur+max);
			bc->bh_read(on, right);			  // re-rx the right bit
		  } else if (bh->is_tx() && !newalloc && bc->bh_cancel_write(bh)) {
			BufferHead *right = bc->split(bh, cur+max);
			bc->bh_write(on, right);			  // re-tx the right bit
		  } else {
			bc->split(bh, cur+max);        // just split
		  }
		}
	  }
	  
	  // try to cancel tx?
	  if (bh->is_tx() && !newalloc) bc->bh_cancel_write(bh);
	  	  
	  // put in our map
	  hits[cur] = bh;

	  // keep going.
	  block_t lenfromcur = bh->end() - cur;
	  cur += lenfromcur;
	  left -= lenfromcur;
	  p++;
	  continue; 
	} else {
	  // gap!
	  block_t next = p->first;
	  block_t glen = MIN(next-cur, max);
	  dout(10) << "map_write gap " << cur << "~" << glen << endl;
	  BufferHead *n = new BufferHead(this);
	  n->set_start( cur );
	  n->set_length( glen );
	  bc->add_bh(n);
	  hits[cur] = n;
	  
	  cur += glen;
	  left -= glen;
	  continue;    // more?
	}
  }

  assert(left == 0);
  assert(cur == start+len);
  return 0;
}

int ObjectCache::scan_versions(block_t start, block_t len,
							   version_t& low, version_t& high)
{
  map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
  // p->first >= start
  
  if (p != data.begin() && p->first > start) {
	p--;     // might overlap?
	if (p->first + p->second->length() <= start) 
	  p++;   // doesn't overlap.
  }
  if (p->first >= start+len) 
	return -1;  // to the right.  no hits.
  
  // start
  low = high = p->second->get_version();

  for (p++; p != data.end(); p++) {
	// past?
	if (p->first >= start+len) break;
	
	const version_t v = p->second->get_version();
	if (low > v) low = v;
	if (high < v) high = v;
  }	

  return 0;
}


void ObjectCache::tear_down()
{
  dout(15) << "tear_down " << hex << object_id << dec << endl;

  while (!data.empty()) {
	map<block_t, BufferHead*>::iterator it = data.begin();
	BufferHead *bh = it->second;

	// cancel any pending/queued io, if possible.
	if (bh->is_tx()) 
	  bc->bh_cancel_write(bh);
	if (bh->is_rx())
	  bc->bh_cancel_read(bh);
	if (bh->is_partial_writes()) 
	  bh->cancel_partials();
	
	for (map<block_t,list<Context*> >::iterator p = bh->waitfor_read.begin();
		 p != bh->waitfor_read.end();
		 p++) {
	  finish_contexts(p->second, -1);
	}
	//finish_contexts(bh->waitfor_flush, -1);
	
	bc->remove_bh(bh);
	delete bh;
  }
}



/************** BufferCache ***************/

#undef dout
#define dout(x)  if (x <= g_conf.debug_ebofs) cout << "ebofs.bc."



BufferHead *BufferCache::split(BufferHead *orig, block_t after) 
{
  dout(20) << "split " << *orig << " at " << after << endl;

  // split off right
  BufferHead *right = new BufferHead(orig->get_oc());
  right->set_version(orig->get_version());
  right->set_state(orig->get_state());
  block_t newleftlen = after - orig->start();
  right->set_start( after );
  right->set_length( orig->length() - newleftlen );

  // shorten left
  stat_sub(orig);
  orig->set_length( newleftlen );
  stat_add(orig);

  // add right
  add_bh(right);

  // split buffers too
  bufferlist bl;
  bl.claim(orig->data);
  if (bl.length()) {
	assert(bl.length() == (orig->length()+right->length())*EBOFS_BLOCK_SIZE);
	right->data.substr_of(bl, orig->length()*EBOFS_BLOCK_SIZE, right->length()*EBOFS_BLOCK_SIZE);
	orig->data.substr_of(bl, 0, orig->length()*EBOFS_BLOCK_SIZE);
  }

  // move read waiters
  if (!orig->waitfor_read.empty()) {
	map<block_t, list<Context*> >::iterator o, p = orig->waitfor_read.end();
	p--;
	while (p != orig->waitfor_read.begin()) {
	  if (p->first < right->start()) break;	  
	  dout(20) << "split  moving waiters at block " << p->first << " to right bh" << endl;
	  right->waitfor_read[p->first].swap( p->second );
	  o = p;
	  p--;
	  orig->waitfor_read.erase(o);
	}
  }
  
  dout(20) << "split    left is " << *orig << endl;
  dout(20) << "split   right is " << *right << endl;
  return right;
}


void BufferCache::bh_read(Onode *on, BufferHead *bh, block_t from)
{
  dout(10) << "bh_read " << *on << " on " << *bh << endl;

  if (bh->is_missing())	{
	mark_rx(bh);
  } else {
	assert(bh->is_partial());
  }
  
  // get extent.  there should be only one!
  vector<Extent> exv;
  on->map_extents(bh->start(), bh->length(), exv);
  assert(exv.size() == 1);
  Extent ex = exv[0];

  if (from) {  // force behavior, used for reading partials
	dout(10) << "bh_read  forcing read from block " << from << " (for a partial)" << endl;
	ex.start = from;
	ex.length = 1;
  }
  
  // alloc new buffer
  bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data);  // new buffers!
  
  // this should be empty!!
  assert(bh->rx_ioh == 0);
  
  dout(20) << "bh_read  " << *bh << " from " << ex << endl;
  
  bh->rx_ioh = dev.read(ex.start, ex.length, bh->data,
						new C_OC_RxFinish(ebofs_lock, on->oc, 
										  bh->start(), bh->length()));
  on->oc->get();

}

bool BufferCache::bh_cancel_read(BufferHead *bh)
{
  if (bh->rx_ioh && dev.cancel_io(bh->rx_ioh) >= 0) {
	dout(10) << "bh_cancel_read on " << *bh << endl;
	bh->rx_ioh = 0;
	mark_missing(bh);
	int l = bh->oc->put();
	assert(l);
	return true;
  }
  return false;
}

void BufferCache::bh_write(Onode *on, BufferHead *bh)
{
  dout(10) << "bh_write " << *on << " on " << *bh << endl;
  assert(bh->get_version() > 0);

  assert(bh->is_dirty());
  mark_tx(bh);
  
  // get extents
  vector<Extent> exv;
  on->map_extents(bh->start(), bh->length(), exv);
  assert(exv.size() == 1);
  Extent ex = exv[0];

  dout(20) << "bh_write  " << *bh << " to " << ex << endl;

  //assert(bh->tx_ioh == 0);

  bh->tx_block = ex.start;
  bh->tx_ioh = dev.write(ex.start, ex.length, bh->data,
						 new C_OC_TxFinish(ebofs_lock, on->oc, 
										   bh->start(), bh->length(),
										   bh->get_version(),
										   bh->epoch_modified),
						 "bh_write");

  on->oc->get();
  inc_unflushed( bh->epoch_modified );
}


bool BufferCache::bh_cancel_write(BufferHead *bh)
{
  if (bh->tx_ioh && dev.cancel_io(bh->tx_ioh) >= 0) {
	dout(10) << "bh_cancel_write on " << *bh << endl;
	bh->tx_ioh = 0;
	mark_dirty(bh);
	dec_unflushed( bh->epoch_modified );   // assert.. this should be the same epoch!
	int l = bh->oc->put();
	assert(l);
	return true;
  }
  return false;
}

void BufferCache::tx_finish(ObjectCache *oc, 
							ioh_t ioh, block_t start, block_t length, 
							version_t version, version_t epoch)
{
  ebofs_lock.Lock();

  // finish oc
  if (oc->put() == 0) {
	delete oc;
  } else
	oc->tx_finish(ioh, start, length, version, epoch);
  
  // update unflushed counter
  assert(get_unflushed(epoch) > 0);
  dec_unflushed(epoch);

  ebofs_lock.Unlock();
}

void BufferCache::rx_finish(ObjectCache *oc,
							ioh_t ioh, block_t start, block_t length)
{
  ebofs_lock.Lock();

  // oc
  if (oc->put() == 0) {
	delete oc;
  } else
	oc->rx_finish(ioh, start, length);

  // 
  ebofs_lock.Unlock();
}

void BufferCache::partial_tx_finish(version_t epoch)
{
  ebofs_lock.Lock();

  /* don't need oc!
  // oc?
  if (oc->put() == 0) 
	delete oc;
  */

  // update unflushed counter
  assert(get_unflushed(epoch) > 0);
  dec_unflushed(epoch);

  ebofs_lock.Unlock();
}




void BufferCache::bh_queue_partial_write(Onode *on, BufferHead *bh)
{
  assert(bh->get_version() > 0);

  assert(bh->is_partial());
  assert(bh->length() == 1);
  
  // get the block no
  vector<Extent> exv;
  on->map_extents(bh->start(), bh->length(), exv);
  assert(exv.size() == 1);
  block_t b = exv[0].start;
  assert(exv[0].length == 1);

  dout(10) << "bh_queue_partial_write " << *on << " on " << *bh << " block " << b << endl;


  // copy map state, queue for this block
  bh->queue_partial_write( b );
}




