#ifndef __CAPABILITY_H
#define __CAPABILITY_H

#include "include/bufferlist.h"

#include <map>
using namespace std;

#include "config.h"


// definite caps
#define CAP_FILE_RDCACHE   1
#define CAP_FILE_RD        2
#define CAP_FILE_WR        4
#define CAP_FILE_WRBUFFER  8
//#define CAP_INODE_STAT    16

// heuristics
//#define CAP_FILE_DELAYFLUSH  32

inline string cap_string(int cap)
{
  string s;
  s = "[";
  if (cap & CAP_FILE_RDCACHE) s += " rdcache";
  if (cap & CAP_FILE_RD) s += " rd";
  if (cap & CAP_FILE_WR) s += " wr";
  if (cap & CAP_FILE_WRBUFFER) s += " wrbuffer";
  s += " ]";
  return s;
}


class Capability {
  int wanted_caps;     // what the client wants

  map<long, int>  cap_history;  // seq -> cap
  long last_sent, last_recv;
    
public:
  Capability(int want=0) :
	wanted_caps(want),
	last_sent(0),
	last_recv(0) { 
	cap_history[last_sent] = 0;
  }


  // most recently issued caps.
  int pending()   { 
	return cap_history[ last_sent ];
  }
  
  // caps client has confirmed receipt of
  int confirmed() { 
	return cap_history[ last_recv ];
  }

  // caps potentially issued
  int issued() { 
	int c = 0;
	for (long seq = last_recv; seq <= last_sent; seq++) {
	  c |= cap_history[seq];
	  dout(10) << "cap issued: " << seq << " " << cap_history[seq] << " -> " << c << endl;
	}
	return c;
  }

  // caps this client wants to hold
  int wanted()    { return wanted_caps; }
  void set_wanted(int w) {
	wanted_caps = w;
  }

  // conflicts
  int conflicts(int from) {
	int c = 0;
	if (from & CAP_FILE_WR) c |= CAP_FILE_RDCACHE;
	if (from & CAP_FILE_RD) c |= CAP_FILE_WRBUFFER;
	return c;
  }
  int wanted_conflicts() { 
	return conflicts(wanted_caps);
  }
  int issued_conflicts() {
	return conflicts(issued());
  }

  // issue caps; return seq number.
  long issue(int c) {
	++last_sent;
	cap_history[last_sent] = c;
	return last_sent;
  }
  long get_last_seq() { return last_sent; }

  void confirm_receipt(long seq) {
	while (last_recv < seq) {
	  cap_history.erase(last_recv);
	  ++last_recv;
	}
  }

  // serializers
  void _encode(bufferlist& bl) {
	bl.append((char*)&wanted_caps, sizeof(wanted_caps));
	bl.append((char*)&last_sent, sizeof(last_sent));
	bl.append((char*)&last_recv, sizeof(last_recv));
	for (long seq = last_recv; seq <= last_sent; seq++) {
	  int c = cap_history[seq];
	  bl.append((char*)&c, sizeof(c));
	}
  }
  void _decode(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(wanted_caps), (char*)&wanted_caps);
	off += sizeof(wanted_caps);
	bl.copy(off, sizeof(last_sent), (char*)&last_sent);
	off += sizeof(last_sent);
	bl.copy(off, sizeof(last_recv), (char*)&last_recv);
	off += sizeof(last_recv);
	for (long seq=last_recv; seq<=last_sent; seq++) {
	  int c;
	  bl.copy(off, sizeof(c), (char*)&c);
	  off += sizeof(c);
	  cap_history[seq] = c;
	}
  }
  
};




/*
// capability helper functions!
inline int file_mode_want_caps(int mode) {
  int wants = 0;
  if (mode == CFILE_MODE_W ) {
	wants |= CFILE_CAP_WR | CFILE_CAP_WRBUFFER;
  }
  else if (mode == CFILE_MODE_RW) {
	wants |= CFILE_CAP_RDCACHE | CFILE_CAP_RD | CFILE_CAP_WR | CFILE_CAP_WRBUFFER;
  }
  else if (mode == CFILE_MODE_R ) {
	wants |= CFILE_CAP_RDCACHE | CFILE_CAP_RD;
  } 
  else assert(0);
  return wants;
}
*/



#endif
