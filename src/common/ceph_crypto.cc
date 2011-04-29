#include "ceph_crypto.h"

#ifdef USE_CRYPTOPP
// nothing
ceph::crypto::HMACSHA1::~HMACSHA1()
{
}

#elif USE_NSS

void ceph::crypto::init() {
  SECStatus s;
  s = NSS_NoDB_Init(NULL);
  assert(s == SECSuccess);
}

ceph::crypto::HMACSHA1::~HMACSHA1()
{
  PK11_DestroyContext(ctx, PR_TRUE);
  PK11_FreeSymKey(symkey);
  PK11_FreeSlot(slot);
}

#else
# error "No supported crypto implementation found."
#endif
