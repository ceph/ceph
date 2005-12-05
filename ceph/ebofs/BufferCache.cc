
#include "BufferCache.h"

#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "bc."



/*********** BufferHead **************/











/************ ObjectCache **************/



void ObjectCache::rx_finish(ioh_t ioh, block_t start, block_t length)
{
  list<Context*> waiters;

  bc->lock.Lock();

  dout(10) << "rx_finish " << start << "+" << length << endl;
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
	   p != data.end(); 
	   p++) {
	// past?
	if (p->first >= start+length) break;
	
	if (p->second->is_rx()) {
	  if (p->second->get_version() == 0) {
		assert(p->second->end() <= start+length);
		dout(10) << "rx_finish  rx -> clean on " << *p->second << endl;
		bc->mark_clean(p->second);
	  }
	}
	else if (p->second->is_partial()) {
	  dout(10) << "rx_finish  partial -> dirty on " << *p->second << endl;	  
	  p->second->apply_partial();
	  bc->mark_dirty(p->second);
	}
	else {
	  dout(10) << "rx_finish  ignoring " << *p->second << endl;
	}
	
	if (p->second->ioh == ioh) p->second->ioh = 0;

	// trigger waiters
	waiters.splice(waiters.begin(), p->second->waitfor_read);
  }	

  finish_contexts(waiters);

  bc->lock.Unlock();
}


void ObjectCache::tx_finish(ioh_t ioh, block_t start, block_t length, version_t version)
{
  list<Context*> waiters;

  bc->lock.Lock();

  dout(10) << "tx_finish " << start << "+" << length << " v" << version << endl;
  for (map<block_t, BufferHead*>::iterator p = data.lower_bound(start);
	   p != data.end(); 
	   p++) {
	dout(20) << "tx_finish ?bh " << *p->second << endl;
	assert(p->first == p->second->start());
	//dout(10) << "tx_finish  bh " << *p->second << endl;

	// past?
	if (p->first >= start+length) break;

	if (!p->second->is_tx()) {
	  dout(10) << "tx_finish  bh not marked tx, skipping" << endl;
	  continue;
	}

	assert(p->second->is_tx());
	assert(p->second->end() <= start+length);
	
	dout(10) << "tx_finish  tx -> clean on " << *p->second << endl;
	p->second->set_last_flushed(version);
	bc->mark_clean(p->second);

	if (p->second->ioh == ioh) p->second->ioh = 0;

	// trigger waiters
	waiters.splice(waiters.begin(), p->second->waitfor_flush);
  }	

  finish_contexts(waiters);

  bc->lock.Unlock();
}


int ObjectCache::map_read(block_t start, block_t len, 
						  map<block_t, BufferHead*>& hits,
						  map<block_t, BufferHead*>& missing,
						  map<block_t, BufferHead*>& rx,
						  map<block_t, BufferHead*>& partial) {
  
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
	// at end?
	if (p == data.end()) {
	  // rest is a miss.
	  BufferHead *n = new BufferHead(this);
	  bc->add_bh(n);
	  n->set_start( cur );
	  n->set_length( left );
	  data[cur] = n;
	  missing[cur] = n;
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
	  BufferHead *n = new BufferHead(this);
	  bc->add_bh(n);
	  n->set_start( cur );
	  if (next - cur < left)
		n->set_length( next - cur );
	  else
		n->set_length( left );
	  data[cur] = n;
	  missing[cur] = n;
	  
	  cur += n->length();
	  left -= n->length();
	  continue;    // more?
	}
	else 
	  assert(0);
  }

  return 0;  
}

int ObjectCache::map_write(block_t start, block_t len,
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
	// at end?
	if (p == data.end()) {
	  BufferHead *n = new BufferHead(this);
	  bc->add_bh(n);
	  n->set_start( cur );
	  n->set_length( left );
	  data[cur] = n;
	  hits[cur] = n;
	  break;
	}
	
	if (p->first <= cur) {
	  // have it (or part of it)
	  BufferHead *e = p->second;
	  
	  if (p->first == cur && p->second->length() <= left) {
		// whole bufferhead, piece of cake.		
	  } else {
		if (e->is_clean()) {
		  // we'll need to cut the buffer!  :(
		  if (p->first == cur && p->second->length() > left) {
			// we want left bit (one splice)
			bc->split(e, cur+left);
		  }
		  else if (p->first < cur && cur+left >= p->first+p->second->length()) {
			// we want right bit (one splice)
			e = bc->split(e, cur);
			p++;
			assert(p->second == e);
		  } else {
			// we want middle bit (two splices)
			e = bc->split(e, cur);
			p++;
			assert(p->second == e);
			bc->split(e, cur+left);
		  }
		}		
	  }
	  
	  // FIXME
	  hits[cur] = e;
	  	  
	  // keep going.
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
	} else {
	  // gap!
	  block_t next = p->first;
	  BufferHead *n = new BufferHead(this);
	  bc->add_bh(n);
	  n->set_start( cur );
	  if (next - cur < left)
		n->set_length( next - cur );
	  else
		n->set_length( left );
	  data[cur] = n;
	  hits[cur] = n;
	  
	  cur += n->length();
	  left -= n->length();
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


BufferHead *BufferCache::split(BufferHead *orig, block_t after) 
{
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
  

  // FIXME: waiters?
  
  return right;
}




