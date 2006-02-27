#ifndef __MDSTYPES_H
#define __MDSTYPES_H


#include <math.h>
#include <ostream>
using namespace std;

#include "config.h"
#include "common/DecayCounter.h"


/* meta_load_t
 * hierarchical load for an inode/dir and it's children
 */
#define META_POP_RD    0
#define META_POP_WR    1
#define META_POP_LOG   2
#define META_POP_FDIR  3
#define META_POP_CDIR  4
#define META_NPOP      5

class meta_load_t {
 public:
  DecayCounter pop[META_NPOP];

  double meta_load() {
	return pop[META_POP_RD].get() + pop[META_POP_WR].get();
  }

  void take(meta_load_t& other) {
	for (int i=0; i<META_NPOP; i++) {
	  pop[i] = other.pop[i];
	  other.pop[i].reset();
	}
  }
};

inline ostream& operator<<( ostream& out, meta_load_t& load )
{
  return out << "metaload<rd " << load.pop[META_POP_RD].get()
			 << ", wr " << load.pop[META_POP_WR].get()
			 << ">";
}


inline meta_load_t& operator-=(meta_load_t& l, meta_load_t& r)
{
  for (int i=0; i<META_NPOP; i++)
	l.pop[i].adjust(- r.pop[i].get());
  return l;
}

inline meta_load_t& operator+=(meta_load_t& l, meta_load_t& r)
{
  for (int i=0; i<META_NPOP; i++)
	l.pop[i].adjust(r.pop[i].get());
  return l;
}



/* mds_load_t
 * mds load
 */

// popularity classes
#define MDS_POP_JUSTME  0   // just me (this dir or inode)
#define MDS_POP_NESTED  1   // me + children, auth or not
#define MDS_POP_CURDOM  2   // me + children in current auth domain
#define MDS_POP_ANYDOM  3   // me + children in any (nested) auth domain
//#define MDS_POP_DIRMOD  4   // just this dir, modifications only
#define MDS_NPOP        4

class mds_load_t {
 public:
  meta_load_t root;

  double req_rate;
  double cache_hit_rate;
  double queue_len;

  mds_load_t() : 
	req_rate(0), cache_hit_rate(0), queue_len(0) { }	

  double mds_load() {
	switch(g_conf.mds_bal_mode) {
	case 0: 
	  return root.pop[META_POP_RD].get() 
		+ 2.0*root.pop[META_POP_WR].get()
		+ 10.0*queue_len;

	case 1:
	  return req_rate + 10.0*queue_len;

	}
	return 0;
  }

};


inline ostream& operator<<( ostream& out, mds_load_t& load )
{
  return out << "mdsload<" << load.root
			 << ", req " << load.req_rate 
			 << ", hr " << load.cache_hit_rate
			 << ", qlen " << load.queue_len
			 << ">";
}

/*
inline mds_load_t& operator+=( mds_load_t& l, mds_load_t& r ) 
{
  l.root_pop += r.root_pop;
  l.req_rate += r.req_rate;
  l.queue_len += r.queue_len;
  return l;
}

inline mds_load_t operator/( mds_load_t& a, double d ) 
{
  mds_load_t r;
  r.root_pop = a.root_pop / d;
  r.req_rate = a.req_rate / d;
  r.queue_len = a.queue_len / d;
  return r;
}
*/


#endif
