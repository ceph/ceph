

#define FILE_MODE_R    1
#define FILE_MODE_RW   2
#define FILE_MODE_W    3


struct CFile {
  client_addr_t client;
  fh_t      fh;    // file handle
  byte      mode;  // mode opened in..
  byte      state; // ??
};
