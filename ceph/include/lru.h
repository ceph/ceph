
#ifndef __LRU_H
#define __LRU_H


class LRUObject {
 protected:
  LRUObject *lru_next, *lru_prev;
  bool lru_in_top;
  bool lru_expireable;

 public:
  LRUObject() {
	lru_next = lru_prev = NULL;
	lru_in_top = false;
	lru_expireable = true;
  }

  // pin/unpin item in cache
  void lru_pin() {
	lru_expireable = false;
  }
  void lru_unpin() {
	lru_expireable = true;
  }

  friend class LRU;
};



class LRU {
 protected:
  LRUObject *lru_tophead, *lru_toptail, *lru_bothead, *lru_bottail;
  __uint32_t lru_ntop, lru_nbot, lru_num;
  double lru_midpoint;

 public:
  LRU() {
	lru_ntop = lru_nbot = lru_num = 0;
	lru_tophead = lru_toptail = NULL;
	lru_bothead = lru_bottail = NULL;
	lru_midpoint = .9;
  }

  // insert at top of lru
  void lru_insert_top(LRUObject *o) {
	o->lru_in_top = true;
	o->lru_next = lru_tophead;
	o->lru_prev = NULL;
	if (lru_tophead) {
	  lru_tophead->lru_prev = o;
	} else {
	  lru_toptail = o;
	}
	lru_ntop++;

	lru_adjust();
  }

  // insert at mid point in lru
  void lru_insert_mid(LRUObject *o) {
	o->lru_in_top = false;
	o->lru_next = lru_bothead;
	o->lru_prev = NULL;
	if (lru_bothead) {
	  lru_bothead->lru_prev = o;
	} else {
	  lru_bottail = o;
	}
	lru_nbot++;
  }




  // adjust top/bot balance, as necessary
  void lru_adjust() {
	__uint32_t topwant = (__uint32_t)(lru_midpoint * (double)lru_num);
	while (lru_ntop > topwant) {
	  // remove from tail of top, stick at head of bot
	  // FIXME: this could be way more efficient by moving a whole chain of items.
	  lru_insert_mid( lru_remove( lru_toptail) );
	}
  }


  // remove an item
  LRUObject *lru_remove(LRUObject *o) {
	if (o->lru_in_top) {
	  // top
	  if (o->lru_next)
		o->lru_next->lru_prev = o->lru_prev;
	  else
		lru_toptail = o->lru_prev;
	  if (o->lru_prev)
		o->lru_prev->lru_next = o->lru_next;
	  else
		lru_tophead = o->lru_next;
	  lru_ntop--;
	} else {
	  // bot
	  if (o->lru_next)
		o->lru_next->lru_prev = o->lru_prev;
	  else
		lru_bottail = o->lru_prev;
	  if (o->lru_prev)
		o->lru_prev->lru_next = o->lru_next;
	  else
		lru_bothead = o->lru_next;
	  lru_nbot--;
	}
	o->lru_next = o->lru_prev = NULL;
	return o;
  }

  // touch item -- move to head of lru
  bool lru_touch(LRUObject *o) {
	lru_remove(o);
	lru_insert_top(o);
	return true;
  }

  // touch item -- move to midpoint (unless already higher)
  bool lru_midtouch(LRUObject *o) {
	if (o->lru_in_top) return false;

	lru_remove(o);
	lru_insert_mid(o);
	return true;
  }


  // expire -- expire a single item
  LRUObject *expire() {
	LRUObject *p;

	// look through tail of bot
	p = lru_bottail;
	while (p) {
	  if (p->lru_expireable) 
		return lru_remove( lru_bottail );
	  p = p->lru_prev;
	}

	// ok, try head then
	p = lru_headtail;
	while (p) {
	  if (p->lru_expireable) 
		return lru_remove( lru_bottail );
	  p = p->lru_prev;
	}
	
	// no luck!
	return NULL;
  }


};


#endif
