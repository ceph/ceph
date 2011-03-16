#include "ceph_crypto.h"

#ifdef USE_CRYPTOPP
// nothing
#elif USE_NSS

void ceph::crypto::init() {
  NSS_NoDB_Init(NULL);
}

#else
# error "No supported crypto implementation found."
#endif
