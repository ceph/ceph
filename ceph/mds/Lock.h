#ifndef __LOCK_H
#define __LOCK_H

#include <assert.h>
#include <set>
using namespace std;

#include "include/bufferlist.h"


// STATES
// basic lock
#define LOCK_SYNC     0
#define LOCK_PRELOCK  1
#define LOCK_LOCK     2
#define LOCK_DELETING 3  // auth only
#define LOCK_DELETED  4

// async lock
#define LOCK_ASYNC    5
#define LOCK_GSYNC    6  // gather to sync
#define LOCK_GLOCK    7  // gather to lock
#define LOCK_GASYNC   8  // gather to async

// waits (on replica)
#define LOCK_WLOCKR   9
#define LOCK_WLOCKW  10
#define LOCK_WGASYNC 11
#define LOCK_WGSYNC  12

#define LOCK_TYPE_BASIC  0
#define LOCK_TYPE_ASYNC  1

#define LOCK_MODE_SYNC     0  // return to sync when writes finish (or read requested)
#define LOCK_MODE_ASYNC    1  // return to async when reads finish (or write requested)


// -- basic lock

class CLock {
 protected:
  // lock state
  char     type;
  char     state;
  char     mode;
  set<int> gather_set;  // auth
  int      nread, nwrite;

  // dual meaning: 
  //  on replicas, whether we've requested; 
  //  on auth, whether others have requested.
  //bool     req_read, req_write;       // FIXME: roll these into state, use a mask, whatever.
  
 public:
  CLock() {}
  CLock(char t) : 
	type(t),
	state(LOCK_LOCK), 
	mode(LOCK_MODE_SYNC),
	nread(0), 
	nwrite(0) {
  }
  
  // encode/decode
  void encode_state(bufferlist& bl) {
	bl.append((char*)&type, sizeof(char));
	bl.append((char*)&state, sizeof(state));
	bl.append((char*)&mode, sizeof(mode));
	bl.append((char*)&nread, sizeof(nread));
	bl.append((char*)&nwrite, sizeof(nwrite));
	//r.append((char*)&req_read, sizeof(req_read));
	//r.append((char*)&req_write, sizeof(req_write));

	_encode(gather_set, bl);
  }
  void decode_state(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(type), (char*)&type);
	off += sizeof(type);
	bl.copy(off, sizeof(state), (char*)&state);
	off += sizeof(state);
	bl.copy(off, sizeof(mode), (char*)&mode);
	off += sizeof(mode);
	bl.copy(off, sizeof(nread), (char*)&nread);
	off += sizeof(nread);
	bl.copy(off, sizeof(nwrite), (char*)&nwrite);
	off += sizeof(nwrite);
	//r.copy(off, sizeof(req_read), (char*)&req_read);
	//off += sizeof(req_read);
	//r.copy(off, sizeof(req_write), (char*)&req_write);
	//off += sizeof(req_write);

	_decode(gather_set, bl, off);
  }

  char get_state() { return state; }
  char set_state(char s) { 
	state = s; 
	assert(!is_stable() || gather_set.size() == 0);  // gather should be empty in stable states.
	return s;
  };

  char get_mode() { return mode; }
  char set_mode(char m) {
	return mode = m;
  }

  char get_replica_state() {
	if (state == LOCK_PRELOCK) return LOCK_LOCK;
	if (state == LOCK_GLOCK) return LOCK_LOCK;
	return state;  // SYNC, LOCK, GASYNC, GSYNC
  }

  // gather set
  set<int>& get_gather_set() { return gather_set; }
  void init_gather(set<int>& i) {
	gather_set = i;
  }
  bool is_gathering(int i) {
	return gather_set.count(i);
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

  //bool get_req_read() { return req_read; }
  //bool get_req_write() { return req_write; }
  //void set_req_read(bool b) { req_read = b; }
  //void set_req_write(bool b) { req_write = b; }

  void twiddle_export() {  // was auth, now replica
	gather_set.clear();
	if (state == LOCK_PRELOCK ||
		state == LOCK_GLOCK) state = LOCK_LOCK;
  }
  void twiddle_import() {  // was replica, now auth
	
  }
  
  // stable
  bool is_stable() {
	return (state == LOCK_SYNC) || (state == LOCK_LOCK) || (state == LOCK_ASYNC);
  }

  // read/write access
  bool could_read(bool auth) {
	if (auth)
	  return false;
	else
	  return (state == LOCK_WLOCKR) || (state == LOCK_WGASYNC);
  }
  bool can_read(bool auth) {
	if (auth)
	  return (state == LOCK_SYNC) || (state == LOCK_PRELOCK) 
		|| (state == LOCK_LOCK) || (state == LOCK_GASYNC);
	else
	  return (state == LOCK_SYNC);
  }
  bool can_read_soon(bool auth) {
	if (auth)
	  return (state == LOCK_GSYNC) || (state == LOCK_GLOCK);
	else
	  return (state == LOCK_GSYNC);
  }
  
  bool could_write(bool auth) {
	if (auth)
	  return false;
	else
	  return (state == LOCK_WGSYNC) || (state == LOCK_WLOCKW);
  }
  bool can_write(bool auth) {
	if (auth) 
	  return (state == LOCK_LOCK) || (state == LOCK_ASYNC) || 
		(state == LOCK_GLOCK) || (state == LOCK_GSYNC);
	else
	  return (state == LOCK_ASYNC);
  }
  bool can_write_soon(bool auth) {
	if (auth)
	  return (state == LOCK_PRELOCK) || (state == LOCK_GASYNC);
	else
	  return (state == LOCK_GASYNC);
  }

  friend class MDCache;
};

//ostream& operator<<(ostream& out, CLock& l);
inline ostream& operator<<(ostream& out, CLock& l) 
{
  static char* __lock_states[] = {
	"sync",
	"prelock",
	"lock",
	"deleting",
	"deleted",
	"async",
	"gsync",
	"glock",
	"gasync",
	"wlockr",
	"wlockw",
	"wgasync",
	"wgsync"
  }; 

  out << "(" << __lock_states[(int)l.get_state()];

  if (!l.get_gather_set().empty()) out << " g=" << l.get_gather_set();

  if (l.get_nread()) 
	out << " " << l.get_nread() << "r";
  if (l.get_nwrite())
	out << " " << l.get_nwrite() << "w";

  if (l.get_mode() == LOCK_MODE_SYNC)
	out << " Sm";
  if (l.get_mode() == LOCK_MODE_ASYNC)
	out << " Am";

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
