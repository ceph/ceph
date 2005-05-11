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

struct CFile {
  int            client;
  fileh_t        fh;    // file handle
  unsigned char  mode;  // mode the file was opened in
  unsigned char  caps;  // client capabilities
};

#endif
