
#ifndef __LRU_H
#define __LRU_H

#include <assert.h>
#include <iostream>
using namespace std;

#include "include/config.h"


class LRUObject {
 protected:
  LRUObject *lru_next, *lru_prev;
  bool lru_in_top;
  bool lru_in_lru;
  bool lru_expireable;

 public:
  LRUObject() {
	lru_next = lru_prev = NULL;
	lru_in_top = false;
	lru_in_lru = false;
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
  __uint32_t lru_max;   // max items

 public:
  LRU() {
	lru_ntop = lru_nbot = lru_num = 0;
	lru_tophead = lru_toptail = NULL;
	lru_bothead = lru_bottail = NULL;
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
  void lru_set_max(__uint32_t m) { lru_max = m; }
  void lru_set_midpoint(float f) { lru_midpoint = f; }
  

  // insert at top of lru
  void lru_insert_top(LRUObject *o) {
	assert(!o->lru_in_lru);
	o->lru_in_lru = true;

	o->lru_in_top = true;
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

	lru_adjust();
  }

  // insert at mid point in lru
  void lru_insert_mid(LRUObject *o) {
	assert(!o->lru_in_lru);
	o->lru_in_lru = true;

	o->lru_in_top = false;
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
  }




  // adjust top/bot balance, as necessary
  void lru_adjust() {
	if (!lru_max) return;

	__uint32_t topwant = (__uint32_t)(lru_midpoint * (double)lru_max);
	while (lru_ntop > topwant) {
	  // remove from tail of top, stick at head of bot
	  // FIXME: this could be way more efficient by moving a whole chain of items.
	  lru_insert_mid( lru_remove( lru_toptail) );
	}
  }


  // remove an item
  LRUObject *lru_remove(LRUObject *o) {
	// not in list
	//assert(o->lru_in_lru);
	if (!o->lru_in_lru) return o;  // might have expired and been removed that way.

	if (o->lru_in_top) {
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
	} else {
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
	lru_num--;
	o->lru_next = o->lru_prev = NULL;
	o->lru_in_lru = false;
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
  LRUObject *lru_expire() {
	LRUObject *p;

	// look through tail of bot
	p = lru_bottail;
	while (p) {
	  if (p->lru_expireable) 
		return lru_remove( p );
	  //cout << "p " << p << " no expireable" << endl;
	  p = p->lru_prev;
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


#endif
