#ifndef __CFILE_H
#define __CFILE_H

// client open modes
#define CFILE_MODE_R    1
#define CFILE_MODE_RW   2
#define CFILE_MODE_W    3

// client capabilities (what client is allowed to be doing)
#define CFILE_CAP_RDCACHE  1
#define CFILE_CAP_RD       2
#define CFILE_CAP_WR       4
#define CFILE_CAP_WRBUFFER 8


class CFile {
 public:
  int            client;
  fileh_t        fh;    // file handle
  int            mode;  // mode the file was opened in
  int            pending_caps;
  long           last_sent;
  int            confirmed_caps;  // known to be client capabilities
  long           last_recv;

  CFile() :
	client(0),
	fh(0),
	mode(0),
	pending_caps(0),
	last_sent(0),
	confirmed_caps(0),
	last_recv(0) { }
};


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

inline int file_mode_conflict_caps(int mode) {
  int conflicts = 0;
  if (mode == CFILE_MODE_W ) {
	conflicts |= CFILE_CAP_RDCACHE;
  }
  else if (mode == CFILE_MODE_RW) {
	conflicts |= CFILE_CAP_RDCACHE | CFILE_CAP_WRBUFFER;
  }
  else if (mode == CFILE_MODE_R ) {
	conflicts |= CFILE_CAP_WRBUFFER;
  } 
  else assert(0);
  return conflicts;
}


#endif
