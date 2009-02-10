#ifndef __TLS_H
#define __TLS_H

struct TlsData {
  int disable_assert;
};


struct TlsData *tls_get_val();
int tls_init();
void tls_finalize();


#endif
