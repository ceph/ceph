
#ifndef __LRU_H
#define __LRU_H

#include <assert.h>
#include <iostream>
using namespace std;

#include "include/config.h"


#define LRU_POS_NONE    0
#define LRU_POS_TOP     1
#define LRU_POS_BOT     2
#define LRU_POS_PINTAIL 3


class LRUObject {
 private:
  LRUObject *lru_next, *lru_prev;
  char lru_pos;
  bool lru_expireable;
  class LRU *lru;

 public:
  LRUObject() {
	lru_next = lru_prev = NULL;
	lru_pos = LRU_POS_NONE;
	lru_expireable = true;
	lru = 0;
  }

  bool lru_get_pos() { return lru_pos; }
  void lru_set_pos(char p) { lru_pos = p; }

  // pin/unpin item in cache
  void lru_pin(); 
  void lru_unpin();
  bool lru_is_expireable() { return lru_expireable; }

  friend class LRU;
  //friend class MDCache;
};



class LRU {
 protected:
  LRUObject *lru_tophead, *lru_toptail, *lru_bothead, *lru_bottail;
  LRUObject *lru_pintailhead, *lru_pintailtail;
  __uint32_t lru_ntop, lru_nbot, lru_num, lru_num_pinned;
  double lru_midpoint;
  __uint32_t lru_max;   // max items

  friend class LRUObject;
  //friend class MDCache; // hack
 public:
  LRU() {
	lru_ntop = lru_nbot = lru_num = 0;
	lru_num_pinned = 0;
	lru_tophead = lru_toptail = NULL;
	lru_bothead = lru_bottail = NULL;
	lru_pintailhead = lru_pintailtail = NULL;
	lru_midpoint = .9;
	lru_max = 0;
  }
  LRU(int max) {
	LRU();
	lru_max = max;
  }

  __uint32_t lru_get_size() {
	return lru_num;
  }
  __uint32_t lru_get_max() {
	return lru_max;
  }
  __uint32_t lru_get_num_pinned() {
	return lru_num_pinned;
  }
  void lru_set_max(__uint32_t m) { lru_max = m; }
  void lru_set_midpoint(float f) { lru_midpoint = f; }
  

  // insert at top of lru
  void lru_insert_top(LRUObject *o) {
	//assert(!o->lru_in_lru);
	//o->lru_in_lru = true;
	assert(!o->lru);
	o->lru = this;

	o->lru_set_pos( LRU_POS_TOP );
	o->lru_next = lru_tophead;
	o->lru_prev = NULL;
	if (lru_tophead) {
	  lru_tophead->lru_prev = o;
	} else {
	  lru_toptail = o;
	}
	lru_tophead = o;
	lru_ntop++;
	lru_num++;
	lru_num_pinned += !o->lru_expireable;
	lru_adjust();
  }

  // insert at mid point in lru
  void lru_insert_mid(LRUObject *o) {
	//assert(!o->lru_in_lru);
	//o->lru_in_lru = true;
	assert(!o->lru);
	o->lru = this;

	o->lru_set_pos( LRU_POS_BOT );
	o->lru_next = lru_bothead;
	o->lru_prev = NULL;
	if (lru_bothead) {
	  lru_bothead->lru_prev = o;
	} else {
	  lru_bottail = o;
	}
	lru_bothead = o;
	lru_nbot++;
	lru_num++;
	lru_num_pinned += !o->lru_expireable;
  }

  // insert at bottom of lru
  void lru_insert_bot(LRUObject *o) {
	assert(!o->lru);
	o->lru = this;

	o->lru_set_pos( LRU_POS_BOT );
	o->lru_next = NULL;
	o->lru_prev = lru_bottail;
	if (lru_bottail) {
	  lru_bottail->lru_next = o;
	} else {
	  lru_bothead = o;
	}
	lru_bottail = o;
	lru_nbot++;
	lru_num++;
	lru_num_pinned += !o->lru_expireable;
  }

  


  // adjust top/bot balance, as necessary
  void lru_adjust() {
	if (!lru_max) return;

	__uint32_t topwant = (__uint32_t)(lru_midpoint * (double)lru_max);
	while (lru_ntop > topwant && lru_toptail) {
	  // remove from tail of top, stick at head of bot
	  // FIXME: this could be way more efficient by moving a whole chain of items.
	  lru_insert_mid( lru_remove( lru_toptail) );
	}
  }


  // remove an item
  LRUObject *lru_remove(LRUObject *o) {
	// not in list
	//assert(o->lru_in_lru);
	//if (!o->lru_in_lru) return o;  // might have expired and been removed that way.
	if (!o->lru) return o;


	if (o->lru_get_pos() == LRU_POS_TOP) {
	  //cout << "removing " << o << " from top" << endl;
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
	} 
	else if (o->lru_get_pos() == LRU_POS_BOT) {
	  //cout << "removing " << o << " from bot" << endl;
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
	else {
	  assert(o->lru_get_pos() == LRU_POS_PINTAIL);
	  if (o->lru_next)
		o->lru_next->lru_prev = o->lru_prev;
	  else
		lru_pintailtail = o->lru_prev;
	  if (o->lru_prev)
		o->lru_prev->lru_next = o->lru_next;
	  else
		lru_pintailhead = o->lru_next;
	  lru_nbot--;
	}
	lru_num--;
	lru_num_pinned -= !o->lru_expireable;
	o->lru_next = o->lru_prev = NULL;
	o->lru_set_pos(LRU_POS_NONE);
	o->lru = 0;
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
	if (o->lru_get_pos() == LRU_POS_TOP) return false;
	
	lru_remove(o);
	lru_insert_mid(o);
	return true;
  }

  // touch item -- move to bottom
  bool lru_bottouch(LRUObject *o) {
	lru_remove(o);
	lru_insert_bot(o);
	return true;
  }


  // expire -- expire a single item
  LRUObject *lru_expire() {
	LRUObject *p;

	// look through tail of bot
	while (lru_bottail) {
	  p = lru_remove( lru_bottail );
	  if (p->lru_expireable) 
		return p;   // yay.
	  
	  // pinned, move to pintail
	  p->lru_next = lru_pintailhead;
	  p->lru_prev = NULL;
	  if (lru_pintailhead) {
		lru_pintailhead->lru_prev = p;
	  } else {
		lru_pintailtail = p;
	  }
	  lru_pintailhead = p;
	  
	  p->lru_set_pos( LRU_POS_PINTAIL );
	}

	// ok, try head then
	p = lru_toptail;
	while (p) {
	  if (p->lru_expireable) 
		return lru_remove( p );
	  //cout << "p " << p << " no expireable" << endl;
	  p = p->lru_prev;
	}
	
	// no luck!
	return NULL;
  }


  void lru_status() {
	dout(10) << "lru: " << lru_num << " items, " << lru_ntop << " top, " << lru_nbot << " bot" << endl;
  }

};


inline void LRUObject::lru_pin() 
{
  lru_expireable = false;
  if (lru) lru->lru_num_pinned++;
}
inline void LRUObject::lru_unpin() {
  lru_expireable = true;
  if (lru) {
	lru->lru_num_pinned--;

	// move out of tail?
	if (lru_get_pos() == LRU_POS_PINTAIL) {
	  lru->lru_remove(this);
	  lru->lru_insert_bot(this);
	}
  }
}

#endif
