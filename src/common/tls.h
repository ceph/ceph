#ifndef __TLS_H
#define __TLS_H

struct TlsData {
  int disable_assert;
};


void *tls_get_ptr();
int tls_init();
void tls_finalize();


#endif
