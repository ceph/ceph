#ifndef __LOCK_H
#define __LOCK_H

#include <assert.h>
#include <set>
using namespace std;

#include "include/bufferlist.h"

#include "Capability.h"

// states and such.
//  C = cache reads, R = read, W = write, B = buffer writes

// basic lock                    -----auth----   ---replica---
#define LOCK_SYNC     0  // AR   R . / C R . .   R . / C R . .   stat()
#define LOCK_LOCK     1  // AR   R W / C . . .   . . / C . . .   truncate()
#define LOCK_GLOCKR   2  // AR   R . / C . . .   . . / C . . .

// file lock states
#define LOCK_GLOCKW   3  // A    . . / . . . . 
#define LOCK_GLOCKM   4  // A    . . / . . . .
#define LOCK_MIXED    5  // AR   . . / . R W .   . . / . R . .
#define LOCK_GMIXEDR  6  // AR   R . / . R . .   . . / . R . . 
#define LOCK_GMIXEDW  7  // A    . . / . . W .   

#define LOCK_WRONLY   8  // A    . . / . . W B      (lock)
#define LOCK_GWRONLYR 9  // A    . . / . . . .
#define LOCK_GWRONLYM 10 // A    . . / . . W .

#define LOCK_GSYNCW   11 // A    . . / . . . .
#define LOCK_GSYNCM   12 // A    . . / . R . .

//   4 stable
//  +9 transition
//  13 total


// -- lock... hard or file

class CLock {
 protected:
  // lock state
  char     state;
  set<int> gather_set;  // auth
  int      nread, nwrite;

  
 public:
  CLock() : 
	state(LOCK_LOCK), 
	nread(0), 
	nwrite(0) {
  }
  
  // encode/decode
  void encode_state(bufferlist& bl) {
	bl.append((char*)&state, sizeof(state));
	bl.append((char*)&nread, sizeof(nread));
	bl.append((char*)&nwrite, sizeof(nwrite));

	_encode(gather_set, bl);
  }
  void decode_state(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(state), (char*)&state);
	off += sizeof(state);
	bl.copy(off, sizeof(nread), (char*)&nread);
	off += sizeof(nread);
	bl.copy(off, sizeof(nwrite), (char*)&nwrite);
	off += sizeof(nwrite);

	_decode(gather_set, bl, off);
  }

  char get_state() { return state; }
  char set_state(char s) { 
	state = s; 
	assert(!is_stable() || gather_set.size() == 0);  // gather should be empty in stable states.
	return s;
  };

  char get_replica_state() {
	switch (state) {
	case LOCK_LOCK:
	case LOCK_GLOCKM:
	case LOCK_GLOCKW:
	case LOCK_GLOCKR: 
	case LOCK_WRONLY:
	case LOCK_GWRONLYR:
	case LOCK_GWRONLYM:
	  return LOCK_LOCK;
	case LOCK_MIXED:
	case LOCK_GMIXEDR:
	  return LOCK_MIXED;
	case LOCK_SYNC:
	  return LOCK_SYNC;

	  // after gather auth will bc LOCK_AC_MIXED or whatever
	case LOCK_GSYNCM:
	  return LOCK_MIXED;
	case LOCK_GSYNCW:
	case LOCK_GMIXEDW:     // ** LOCK isn't exact right state, but works.
	  return LOCK_LOCK;

	default: 
	  assert(0);
	}
	return 0;
  }

  // gather set
  set<int>& get_gather_set() { return gather_set; }
  void init_gather(set<int>& i) {
	gather_set = i;
  }
  bool is_gathering(int i) {
	return gather_set.count(i);
  }
  void clear_gather() {
	gather_set.clear();
  }

  // ref counting
  int get_read() { return ++nread; }
  int put_read() {
	assert(nread>0);
	return --nread;
  }
  int get_nread() { return nread; }

  int get_write() { return ++nwrite; }
  int put_write() {
	assert(nwrite>0);
	return --nwrite;
  }
  int get_nwrite() { return nwrite; }
  bool is_used() {
	return (nwrite+nread)>0 ? true:false;
  }

  
  // stable
  bool is_stable() {
	return (state == LOCK_SYNC) || 
	  (state == LOCK_LOCK) || 
	  (state == LOCK_MIXED) || 
	  (state == LOCK_WRONLY);
  }

  // read/write access
  bool can_read(bool auth) {
	if (auth)
	  return (state == LOCK_SYNC) || (state == LOCK_GMIXEDR) 
		|| (state == LOCK_GLOCKR) || (state == LOCK_LOCK);
	else
	  return (state == LOCK_SYNC);
  }
  bool can_read_soon(bool auth) {
	if (auth)
	  return (state == LOCK_GLOCKW);
	else
	  return false;
  }

  bool can_write(bool auth) {
	if (auth) 
	  return (state == LOCK_LOCK);
	else
	  return false;
  }
  bool can_write_soon(bool auth) {
	if (auth)
	  return (state == LOCK_GLOCKR) || (state == LOCK_GLOCKW)
		|| (state == LOCK_GLOCKM);
	else
	  return false;
  }

  // client caps allowed
  int caps_allowed_ever(bool auth) {
	if (auth)
	  return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_WR | CAP_FILE_WRBUFFER;
	else
	  return CAP_FILE_RDCACHE | CAP_FILE_RD;
  }
  int caps_allowed(bool auth) {
	if (auth)
	  switch (state) {
	  case LOCK_SYNC:
		return CAP_FILE_RDCACHE | CAP_FILE_RD;
	  case LOCK_GMIXEDR:
	  case LOCK_GSYNCM:
		return CAP_FILE_RD;
	  case LOCK_MIXED:
		return CAP_FILE_RD | CAP_FILE_WR;
	  case LOCK_GMIXEDW:
	  case LOCK_GWRONLYM:
		return CAP_FILE_WR;
	  case LOCK_WRONLY:
		return CAP_FILE_WR | CAP_FILE_WRBUFFER;
	  case LOCK_LOCK:
	  case LOCK_GLOCKR:
		return CAP_FILE_RDCACHE;
	  case LOCK_GLOCKW:
	  case LOCK_GLOCKM:
	  case LOCK_GWRONLYR:
	  case LOCK_GSYNCW:
		return 0;
	  }
	else
	  switch (state) {
	  case LOCK_SYNC:
		return CAP_FILE_RDCACHE | CAP_FILE_RD;
	  case LOCK_GMIXEDR:
	  case LOCK_MIXED:
		return CAP_FILE_RD;
	  case LOCK_LOCK:
	  case LOCK_GLOCKR:
		return CAP_FILE_RDCACHE;
	  }
	assert(0);
	return 0;
  }
  int caps_allowed_soon(bool auth) {
	if (auth)
	  switch (state) {
	  case LOCK_GSYNCM:
	  case LOCK_GSYNCW:
	  case LOCK_SYNC:
		return CAP_FILE_RDCACHE | CAP_FILE_RD;
	  case LOCK_GMIXEDR:
	  case LOCK_GMIXEDW:
	  case LOCK_MIXED:
		return CAP_FILE_RD | CAP_FILE_WR;
	  case LOCK_GWRONLYM:
	  case LOCK_GWRONLYR:
	  case LOCK_WRONLY:
		return CAP_FILE_WR | CAP_FILE_WRBUFFER;
	  case LOCK_LOCK:
	  case LOCK_GLOCKR:
	  case LOCK_GLOCKW:
	  case LOCK_GLOCKM:
		return 0;
	  }
	else
	  switch (state) {
	  case LOCK_SYNC:
		return CAP_FILE_RDCACHE | CAP_FILE_RD;
	  case LOCK_GMIXEDR:
	  case LOCK_MIXED:
		return CAP_FILE_RD;
	  case LOCK_LOCK:
	  case LOCK_GLOCKR:
		return 0;
	  }
	assert(0);
	return 0;
  }
  

  friend class MDCache;
};

//ostream& operator<<(ostream& out, CLock& l);
inline ostream& operator<<(ostream& out, CLock& l) 
{
  static char* __lock_states[] = {
	"sync",
	"lock",
	"glockr",
	"glockw",
	"glockm",
	"mixed",
	"gmixedr",
	"gmixedw",
	"wronly",
	"gwronlyr",
	"gwronlym",
	"gsyncw",
	"gsyncm"
  }; 

  out << "(" << __lock_states[(int)l.get_state()];

  if (!l.get_gather_set().empty()) out << " g=" << l.get_gather_set();

  if (l.get_nread()) 
	out << " " << l.get_nread() << "r";
  if (l.get_nwrite())
	out << " " << l.get_nwrite() << "w";

  // rw?
  /*
  out << " ";
  if (l.can_read(true)) out << "r[" << l.get_nread() << "]";
  if (l.can_write(true)) out << "w[" << l.get_nwrite() << "]";
  out << "/";
  if (l.can_read(false)) out << "r[" << l.get_nread() << "]";
  if (l.can_write(false)) out << "w[" << l.get_nwrite() << "]";  
  */
  out << ")";
  return out;
}

#endif
