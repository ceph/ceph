
#include "ObjectCacher.h"
#include "Objecter.h"

/*** ObjectCacher::BufferHead ***/


/*** ObjectCacher::Object ***/

BufferHead *ObjectCacher::Object::split(BufferHead *bh, off_t off)
{
  dout(20) << "split " << *bh << " at " << off << endl;
  
  // split off right
  ObjectCacher::BufferHead *right = new ObjectCacher::BufferHead();
  right->set_version(bh->get_version());
  right->set_state(bh->get_state());
  
  block_t newleftlen = off - bh->start();
  right->set_start( off );
  right->set_length( bh->length() - newleftlen );
  
  // shorten left
  stat_sub(bh);
  bh->set_length( newleftlen );
  stat_add(bh);
  
  // add right
  add_bh(right);
  
  // split buffers too
  bufferlist bl;
  bl.claim(bh->bl);
  if (bl.length()) {
	assert(bl.length() == (bh->length() + right->length()));
	right->bl.substr_of(bl, bh->length(), right->length());
	bh->bl.substr_of(bl, 0, bh->length());
  }
  
  // move read waiters
  if (!bh->waitfor_read.empty()) {
	map<block_t, list<Context*> >::iterator o, p = bh->waitfor_read.end();
	p--;
	while (p != bh->waitfor_read.begin()) {
	  if (p->first < right->start()) break;	  
	  dout(0) << "split  moving waiters at block " << p->first << " to right bh" << endl;
	  right->waitfor_read[p->first].swap( p->second );
	  o = p;
	  p--;
	  bh->waitfor_read.erase(o);
	}
  }
  
  dout(20) << "split    left is " << *bh << endl;
  dout(20) << "split   right is " << *right << endl;
  return right;
}

/*
 * map a range of blocks into buffer_heads.
 * - create missing buffer_heads as necessary.
 */
int ObjectCacher::Object::map_read(Objecter::OSDRead *rd,
                                   map<block_t, BufferHead*>& hits,
                                   map<block_t, BufferHead*>& missing,
                                   map<block_t, BufferHead*>& rx)
{
  for (list<ObjectExtent>::iterator ex_it = wr->extents.begin();
       ex_it != wr->extents.end();
       ex_it++) {
    
    if (ex_it->oid != oid) continue;
    
    dout(10) << "map_read " << ex_it->oid << " " << ex_it->offset << "~" << ex_it->len << endl;
    
    map<off_t, ObjectCacher::BufferHead*>::iterator p = data.lower_bound(start);
    // p->first >= start
    
    size_t cur = ex_it->offset;
    size_t left = ex_it->len;
    
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
		BufferHead *n = new ObjectCacher::BufferHead();
		n->set_start( cur );
		n->set_length( left );
		add_bh(n);
		missing[cur] = n;
		dout(20) << "map_read miss " << left << " left, " << *n << endl;
		cur += left;
		left -= left;
        assert(left == 0);
        assert(cur == ex_it->offset + ex_it->len);
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
        else assert(0);
        
        size_t lenfromcur = MIN(e->end() - cur, left);
        cur += lenfromcur;
        left -= lenfromcur;
        p++;
        continue;  // more?
        
      } else if (p->first > cur) {
        // gap.. miss
        size_t next = p->first;
		BufferHead *n = new ObjectCacher::BufferHead();
		n->set_start( cur );
		n->set_length( MIN(next - cur), left );
		add_bh(n);
		missing[cur] = n;
		cur += MIN(left, n->length());
		left -= MIN(left, n->length());
		dout(20) << "map_read gap " << *n << endl;
        continue;    // more?
      }
      else 
        assert(0);
    }
  }
  return(0);
}

/*
 * map a range of extents on an object's buffer cache.
 *
 * - break up bufferheads that don't fall completely within the range
 * - cancel rx ops we obsolete.
 *   - resubmit rx ops if we split bufferheads
 *
 * - leave potentially obsoleted tx ops alone (for now)
 */
int ObjectCacher::Object::map_write(Objecter::OSDWrite *wr)
{
  for (list<ObjectExtent>::iterator ex_it = wr->extents.begin();
       ex_it != wr->extents.end();
       ex_it++) {

    if (ex_it->oid != oid) continue;
    
    dout(10) << "map_write " << ex_it->oid << " " << ex_it->offset << "~" << ex_it->len << endl;
    
    map<off_t, ObjectCacher::BufferHead*>::iterator p = data.lower_bound(start);
    // p->first >= start
    
    size_t cur = ex_it->offset;
    size_t left = ex_it->len;
    
    if (p != data.begin() && 
        (p == data.end() || p->first > cur)) {
      p--;     // might overlap!
      if (p->first + p->second->length() <= cur) 
        p++;   // doesn't overlap.
    }    
    
    ObjectCacher::BufferHead *prev = NULL;
    while (left > 0) {
      size_t max = left;
      
      // at end ?
      if (p == data.end()) {
        if (prev == NULL) {
          ObjectCacher::BufferHead *n = new ObjectCacher::BufferHead();
          n->set_start( cur );
          n->set_length( max );
          add_bh(n);
          //hits[cur] = n;          
        } else {
          prev->set_length( prev->length() + max );
        }
        left -= max;
        cur += max;
        continue;
      }
      
      dout(10) << "p is " << *p->second << endl;

      if (p->first <= cur) {
        ObjectCacher::BufferHead *bh = p->second;
        dout(10) << "map_write bh " << *bh << " intersected" << endl;
        
        if (p->first < cur) {
          if (cur + max >= p->first + p->second->length()) {
            // we want right bit (one splice)
            if (bh->is_rx() && bh_cancel_read(bh)) {
              ObjectCacher::BufferHead *right = split(bh, cur);
              bh_read(this, bh);          // reread left bit
              bh = right;
            } else if (bh->is_tx() && bh_cancel_write(bh)) {
              ObjectCacher::BufferHead *right = split(bh, cur);
              bh_write(this, bh);          // rewrite left bit
              bh = right;
            } else {
              bh = split(bh, cur);   // just split it
            }
            prev = bh;  // maybe want to expand right buffer ...
            p++;
            assert(p->second == bh);
          } else {
            // we want middle bit (two splices)
            if (bh->is_rx() && bh_cancel_read(bh)) {
              ObjectCacher::BufferHead *middle = split(bh, cur);
              bh_read(this, bh);                       // reread left
              p++;
              assert(p->second == middle);
              ObjectCacher::BufferHead *right = split(middle, cur + max);
              bh_read(this(on, right);                    // reread right
              bh = middle;
            } else if (bh->is_tx() && bh_cancel_write(bh)) {
              ObjectCacher::BufferHead *middle = split(bh, cur);
              bh_write(this, bh);                       // redo left
              p++;
              assert(p->second == middle);
              ObjectCacher::BufferHead *right = split(middle, cur + max);
              bh_write(this, right);                    // redo right
              bh = middle;
            } else {
              ObjectCacher::BufferHead *middle = split(bh, cur);
              p++;
              assert(p->second == middle);
              split(middle, cur+max);
              bh = middle;
            }
          }
        } else if (p->first == cur) {
          if (p->second->length() <= max) {
            // whole bufferhead, piece of cake.
          } else {
            // we want left bit (one splice)
            if (bh->is_rx() && bh_cancel_read(bh)) {
              ObjectCacher::BufferHead *right = split(bh, cur + max);
              bh_read(this, right);			  // re-rx the right bit
            } else if (bh->is_tx() && bh_cancel_write(bh)) {
              ObjectCacher::BufferHead *right = split(bh, cur + max);
              bh_write(this, right);			  // re-tx the right bit
            } else {
              split(bh, cur + max);        // just split
            }
          }
        }
        
        // try to cancel tx?
        if (bh->is_tx()) bh_cancel_write(bh);
        
        // put in our map
        //hits[cur] = bh;
        
        // keep going.
        size_t lenfromcur = bh->end() - cur;
        cur += lenfromcur;
        left -= lenfromcur;
        p++;
        continue; 
      } else {
        // gap!
        size_t next = p->first;
        size_t glen = MIN(next - cur, max);
        dout(10) << "map_write gap " << cur << "~" << glen << endl;
        ObjectCacher::BufferHead *n = new ObjectCacher::BufferHead();
        n->set_start( cur );
        n->set_length( glen );
        add_bh(n);
        //hits[cur] = n;
        
        cur += glen;
        left -= glen;
        continue;    // more?
      }
    }
  }
  return(0);
}

/*** ObjectCacher ***/

/* private */

void bh_read(Object *ob, BufferHead *bh)
{
  assert(0);
  return(0);
}

void bh_write(Object *ob, BufferHead *bh)
{
  assert(0);
  return(0);
}

/* public */

int ObjectCacher::readx(Objecter::OSDRead *rd, inodeno_t ino, Context *onfinish)
{
  for (list<ObjectExtent>::iterator ex_it = rd->extents.begin();
       ex_it != wr->extents.end();
       ex_it++) {
    if (objects.count(ex_it->oid) == 0) {
      ObjectCache::Object *o = new ObjectCache::Object(ex_it->oid, ino);
      objects[ex_it->oid] = o;
      objects_by_ino[ino].add(o);
    }
    map<block_t, BufferHead*> hits, missing, rx;
    o->map_read(rd, hits, missing, rx);
    for (map<off_t, BufferHead*>::iterator bh_it = hits.begin();
         bh_it != hits.end();
         bh_it++) {
      rd->bl.substr_of(bh_it->second->bl, bh_it->first, bh_it->second->length());
    }
    for (map<off_t, BufferHead*>::iterator bh_it = missing.begin();
         bh_it != missing.end();
         bh_it++) {
      bh_read(o, bh_it->second);
    }
    for (map<off_t, BufferHead*>::iterator bh_it = rx.begin();
         bh_it != rx.end();
         bh_it++) {
      //FIXME: need to wait here?
      bh_read(o, bh_it->second);
    }    
  }  
  return(0);
}

int ObjectCacher::writex(Objecter::OSDWrite *wr, inodeno_t ino, Context *onack, Context *oncommit)
{
  for (list<ObjectExtent>::iterator ex_it = wr->extents.begin();
       ex_it != wr->extents.end();
       ex_it++) {
    if (objects.count(ex_it->oid) == 0) {
      ObjectCache::Object *o = new ObjectCache::Object(ex_it->oid, ino);
      objects[ex_it->oid] = o;
      objects_by_ino[ino].add(o);
    }
    o->map_write(wr);
    for (map<off_t, BufferHead*>::iterator bh_it = o->data.begin();
         bh_it != o->data.end();
         bh_it++) {
      bh_it->second->bl.substr_of(wr->bl, ex_it->offset, ex_it->len);
      bh_set_state(bh_it->second, BufferHead::STATE_DIRTY);
    }
    // FIXME: how to set up contexts for eventual writes?
  }
  return(0);
}
 
  
// blocking.  atomic+sync.
int ObjectCacher::atomic_sync_readx(Objecter::OSDRead *rd, inodeno_t ino, Context *onfinish)
{
  assert(0);
  return 0;
}

int ObjectCacher::atomic_sync_writex(Objecter::OSDWrite *wr, inodeno_t ino, Context *onack, Context *oncommit)
{
  assert(0);
  return 0;
}
 
