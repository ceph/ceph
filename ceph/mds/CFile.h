#ifndef __CFILE_H
#define __CFILE_H

#define CFILE_MODE_R    1
#define CFILE_MODE_RW   2
#define CFILE_MODE_W    3

#define CFILE_LOCK_NONE   0  // e.g. all readers
#define CFILE_LOCK_LOCK   1  // e.g. truncate (all writers/readers stop)
//#define CFILE_LOCK_ASYNC  0  // e.g. writers


struct CFile {
  int            client;
  fileh_t        fh;    // file handle
  unsigned char  mode;  // mode opened in
  unsigned char  state; // lock state
};

#endif
