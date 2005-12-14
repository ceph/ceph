
#include "BufferCache.h"
#include "Onode.h"


/*********** BufferHead **************/


#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "ebofs.bh."



void BufferHead::finish_partials()
{
  dout(10) << "finish_partials on " << *this << endl;

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
	
	oc->bc->dev.write( p->second.block, 1, bl,
					   new C_OC_PartialTxFinish( oc, p->second.epoch ));
  }
  partial_write.clear();
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
#define dout(x)  if (x <= g_conf.debug) cout << "ebofs.oc."



void ObjectCache::rx_finish(ioh_t ioh, block_t start, block_t length)
{
  list<Context*> waiters;

  bc->ebofs_lock.Lock();

  dout(10) << "rx_finish " << start << "~" << length << endl;
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
	   p != data.end(); 
	   p++) {
	dout(10) << "rx_finish ?" << *p->second << endl;

	// past?
	if (p->first >= start+length) break;
	if (p->second->end() > start+length) break;  // past
	
	assert(p->first >= start);
	assert(p->second->end() <= start+length);

	dout(10) << "rx_finish !" << *p->second << endl;

	if (p->second->rx_ioh == ioh)
	  p->second->rx_ioh = 0;

	if (p->second->is_partial_writes())
	  p->second->finish_partials();
	
	if (p->second->is_rx()) {
	  assert(p->second->get_version() == 0);
	  assert(p->second->end() <= start+length);
	  dout(10) << "rx_finish  rx -> clean on " << *p->second << endl;
	  bc->mark_clean(p->second);
	}
	else if (p->second->is_partial()) {
	  dout(10) << "rx_finish  partial -> clean on " << *p->second << endl;	  
	  p->second->apply_partial();
	  bc->mark_clean(p->second);
	}
	else {
	  dout(10) << "rx_finish  ignoring status on (dirty|tx) " << *p->second << endl;
	  assert(p->second->is_dirty() || p->second->is_tx());
	}

	// trigger waiters
	waiters.splice(waiters.begin(), p->second->waitfor_read);
  }	

  finish_contexts(waiters);

  bc->ebofs_lock.Unlock();
}


void ObjectCache::tx_finish(ioh_t ioh, block_t start, block_t length, 
							version_t version, version_t epoch)
{
  list<Context*> waiters;
  
  bc->ebofs_lock.Lock();
  
  dout(10) << "tx_finish " << start << "~" << length << " v" << version << endl;
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
	   p != data.end(); 
	   p++) {
	dout(20) << "tx_finish ?bh " << *p->second << endl;
	assert(p->first == p->second->start());

	// past?
	if (p->first >= start+length) break;

	if (p->second->tx_ioh == ioh)
	  p->second->tx_ioh = 0;

	if (!p->second->is_tx()) {
	  dout(10) << "tx_finish  bh not marked tx, skipping" << endl;
	  continue;
	}

	assert(p->second->is_tx());
	assert(p->second->end() <= start+length);
	
	if (version == p->second->version) {
	  dout(10) << "tx_finish  tx -> clean on " << *p->second << endl;
	  p->second->set_last_flushed(version);
	  bc->mark_clean(p->second);

	  // trigger waiters
	  waiters.splice(waiters.begin(), p->second->waitfor_flush);
	} else {
	  dout(10) << "tx_finish  leaving tx, " << p->second->version << " > " << version 
			   << " on " << *p->second << endl;
	}
  }	

  finish_contexts(waiters);

  // update unflushed counter
  assert(bc->get_unflushed(epoch) > 0);
  bc->dec_unflushed(epoch);

  bc->ebofs_lock.Unlock();
}

void ObjectCache::partial_tx_finish(version_t epoch) 
{
  bc->ebofs_lock.Lock();

  dout(10) << "partial_tx_finish in epoch " << epoch << endl;

  // update unflushed counter
  assert(bc->get_unflushed(epoch) > 0);
  bc->dec_unflushed(epoch);

  bc->ebofs_lock.Unlock();
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
	  (p->first > cur || p == data.end())) {
	p--;     // might overlap!
	if (p->first + p->second->length() <= cur) 
	  p++;   // doesn't overlap.
  }

  for (; left > 0; p++) {
	// at end?
	if (p == data.end()) {
	  // rest is a miss.
	  vector<Extent> exv;
	  on->map_extents(cur, left, exv);          // we might consider some prefetch here.
	  for (unsigned i=0; i<exv.size(); i++) {
		BufferHead *n = new BufferHead(this);
		bc->add_bh(n);
		n->set_start( cur );
		n->set_length( exv[i].length );
		data[cur] = n;
		missing[cur] = n;
		cur += exv[i].length;
	  }
	  break;
	}
	
	if (p->first <= cur) {
	  // have it (or part of it)
	  BufferHead *e = p->second;
	  
	  if (e->is_clean() ||
		  e->is_dirty() ||
		  e->is_tx()) {
		hits[cur] = e;     // readable!
	  } 
	  else if (e->is_rx()) {
		rx[cur] = e;       // missing, not readable.
	  }
	  else if (e->is_partial()) {
		partial[cur] = e;
	  }
	  else assert(0);
	  
	  block_t lenfromcur = e->length();
	  if (e->start() < cur)
		lenfromcur -= cur - e->start();
	  
	  if (lenfromcur < left) {
		cur += lenfromcur;
		left -= lenfromcur;
		continue;  // more!
	  } else {
		break;     // done.
	  }		
	} else if (p->first > cur) {
	  // gap.. miss
	  block_t next = p->first;
	  vector<Extent> exv;
	  on->map_extents(cur, MIN(next-cur, left), exv);   // we might consider some prefetch here

	  for (unsigned i=0; i<exv.size(); i++) {
		BufferHead *n = new BufferHead(this);
		bc->add_bh(n);
		n->set_start( cur );
		n->set_length( exv[i].length );
		data[cur] = n;
		missing[cur] = n;
		cur += n->length();
		left -= n->length();
	  }
	  continue;    // more?
	}
	else 
	  assert(0);
  }

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
  // p->first >= start
  
  block_t cur = start;
  block_t left = len;
  
  if (p != data.begin() && 
	  (p->first > cur || p == data.end())) {
	p--;     // might overlap!
	if (p->first + p->second->length() <= cur) 
	  p++;   // doesn't overlap.
  }

  for (; left > 0; p++) {
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
	  bc->add_bh(n);
	  n->set_start( cur );
	  n->set_length( max );
	  data[cur] = n;
	  hits[cur] = n;
	  break;
	}

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
	  
	  // put in our map
	  hits[cur] = bh;
	  	  
	  // keep going.
	  block_t lenfromcur = bh->length();
	  if (bh->start() < cur)
		lenfromcur -= cur - bh->start();

	  if (lenfromcur < left) {
		cur += lenfromcur;
		left -= lenfromcur;
		continue;  // more!
	  } else {
		break;     // done.
	  }		
	} else {
	  // gap!
	  block_t next = p->first;
	  block_t glen = MIN(next-cur, max);
	  dout(10) << "map_write gap " << cur << "~" << glen << endl;
	  BufferHead *n = new BufferHead(this);
	  bc->add_bh(n);
	  n->set_start( cur );
	  n->set_length( glen );
	  data[cur] = n;
	  hits[cur] = n;
	  
	  cur += glen;
	  left -= glen;
	  continue;    // more?
	}
  }

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
	if (p->first < start+len) break;
	
	const version_t v = p->second->get_version();
	if (low > v) low = v;
	if (high < v) high = v;
  }	

  return 0;
}




/************** BufferCache ***************/

#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "ebofs.bc."



BufferHead *BufferCache::split(BufferHead *orig, block_t after) 
{
  dout(20) << "split " << *orig << " at " << after << endl;

  BufferHead *right = new BufferHead(orig->get_oc());
  orig->get_oc()->add_bh(right, after);

  right->set_version(orig->get_version());
  right->set_state(orig->get_state());

  block_t mynewlen = after - orig->start();
  right->set_start( after );
  right->set_length( orig->length() - mynewlen );

  add_bh(right);

  stat_sub(orig);
  orig->set_length( mynewlen );
  stat_add(orig);

  // buffers!
  bufferlist bl;
  bl.claim(orig->data);
  if (bl.length()) {
	assert(bl.length() == (orig->length()+right->length())*EBOFS_BLOCK_SIZE);
	right->data.substr_of(bl, orig->length()*EBOFS_BLOCK_SIZE, right->length()*EBOFS_BLOCK_SIZE);
	orig->data.substr_of(bl, 0, orig->length()*EBOFS_BLOCK_SIZE);
  }

  // FIXME: waiters?
  
  return right;
}


void BufferCache::bh_read(Onode *on, BufferHead *bh)
{
  dout(5) << "bh_read " << *on << " on " << *bh << endl;

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
  
  // alloc new buffer
  bufferpool.alloc(EBOFS_BLOCK_SIZE*bh->length(), bh->data);  // new buffers!
  
  // this should be empty!!
  assert(bh->rx_ioh == 0);
  
  dout(20) << "bh_read  " << *bh << " from " << ex << endl;
  
  bh->rx_ioh = dev.read(ex.start, ex.length, bh->data,
						new C_OC_RxFinish(on->oc, 
										  bh->start(), bh->length()));
}

bool BufferCache::bh_cancel_read(BufferHead *bh)
{
  assert(bh->rx_ioh);
  if (dev.cancel_io(bh->rx_ioh) >= 0) {
	dout(10) << "bh_cancel_read on " << *bh << endl;
	bh->rx_ioh = 0;
	mark_missing(bh);
	return true;
  }
  return false;
}

void BufferCache::bh_write(Onode *on, BufferHead *bh)
{
  dout(5) << "bh_write " << *on << " on " << *bh << endl;
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

  bh->tx_ioh = dev.write(ex.start, ex.length, bh->data,
						 new C_OC_TxFinish(on->oc, 
										   bh->start(), bh->length(),
										   bh->get_version(),
										   bh->epoch_modified));

  epoch_unflushed[ bh->epoch_modified ]++;
}


bool BufferCache::bh_cancel_write(BufferHead *bh)
{
  assert(bh->tx_ioh);
  if (dev.cancel_io(bh->tx_ioh) >= 0) {
	dout(10) << "bh_cancel_write on " << *bh << endl;
	bh->tx_ioh = 0;
	mark_dirty(bh);
	epoch_unflushed[ bh->epoch_modified ]--;   // assert.. this should be the same epoch!
	return true;
  }
  return false;
}



void BufferCache::bh_queue_partial_write(Onode *on, BufferHead *bh)
{
  dout(5) << "bh_queue_partial_write " << *on << " on " << *bh << endl;
  assert(bh->get_version() > 0);

  assert(bh->is_partial());
  assert(bh->length() == 1);
  
  // get the block
  vector<Extent> exv;
  on->map_extents(bh->start(), bh->length(), exv);
  assert(exv.size() == 1);
  block_t b = exv[0].start;
  assert(exv[0].length == 1);

  // copy map state, queue for this block
  bh->queue_partial_write( b );
}

