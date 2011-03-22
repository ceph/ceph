#ifndef CEPH_CRYPTO_H
#define CEPH_CRYPTO_H

#include "acconfig.h"

#ifdef USE_CRYPTOPP
# define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <string.h>
# include <cryptopp/md5.h>
# include <cryptopp/sha.h>
# include <cryptopp/hmac.h>
namespace ceph {
  namespace crypto {
    static inline void init() {
      // nothing
    }
    using CryptoPP::Weak::MD5;

    class HMACSHA1: public CryptoPP::HMAC<CryptoPP::SHA1> {
    public:
      HMACSHA1 (const byte *key, size_t length)
	: CryptoPP::HMAC<CryptoPP::SHA1>(key, length)
	{
	}
    };
  }
}
#elif USE_NSS
// you *must* use CRYPTO_CXXFLAGS in Makefile.am for including this include
# include <nss.h>
# include <pk11pub.h>

// NSS thinks a lot of fairly fundamental operations might potentially
// fail, because it has been written to support e.g. smartcards doing all
// the crypto operations. We don't want to contaminate too much code
// with error checking, and just say these really should never fail.
// This assert MUST NOT be compiled out, even on non-debug builds.
# include "assert.h"

// ugly bit of CryptoPP that we have to emulate here :(
typedef unsigned char byte;

namespace ceph {
  namespace crypto {
    void init();

    class MD5 {
    private:
      PK11Context *ctx;
    public:
      static const int DIGESTSIZE = 16;
      MD5 () {
	ctx = PK11_CreateDigestContext(SEC_OID_MD5);
	assert(ctx);
	Restart();
      }
      ~MD5 () {
	PK11_DestroyContext(ctx, PR_TRUE);
      }
      void Restart() {
	SECStatus s;
	s = PK11_DigestBegin(ctx);
	assert(s == SECSuccess);
      }
      void Update (const byte *input, size_t length) {
	SECStatus s;
	s = PK11_DigestOp(ctx, input, length);
	assert(s == SECSuccess);
      }
      void Final (byte *digest) {
	SECStatus s;
	unsigned int dummy;
	s = PK11_DigestFinal(ctx, digest, &dummy, DIGESTSIZE);
	assert(s == SECSuccess);
	assert(dummy == (unsigned int)DIGESTSIZE);
	Restart();
      }
    };

    class HMACSHA1 {
    private:
      PK11SlotInfo *slot;
      PK11SymKey *symkey;
      PK11Context *ctx;
    public:
      static const int DIGESTSIZE = 20;
      HMACSHA1 (const byte *key, size_t length) {
	slot = PK11_GetBestSlot(CKM_SHA_1_HMAC, NULL);
	assert(slot);
	SECItem keyItem;
	keyItem.type = siBuffer;
	keyItem.data = (unsigned char*)key;
	keyItem.len = length;
	symkey = PK11_ImportSymKey(slot, CKM_SHA_1_HMAC, PK11_OriginUnwrap,
				   CKA_SIGN,  &keyItem, NULL);
	assert(symkey);
	SECItem param;
	param.type = siBuffer;
	param.data = NULL;
	param.len = 0;
	ctx = PK11_CreateContextBySymKey(CKM_SHA_1_HMAC, CKA_SIGN, symkey, &param);
	assert(ctx);
	Restart();
      }
      ~HMACSHA1 () {
	PK11_DestroyContext(ctx, PR_TRUE);
	PK11_FreeSymKey(symkey);
	PK11_FreeSlot(slot);
      }
      void Restart() {
	SECStatus s;
	s = PK11_DigestBegin(ctx);
	assert(s == SECSuccess);
      }
      void Update (const byte *input, size_t length) {
	SECStatus s;
	s = PK11_DigestOp(ctx, input, length);
	assert(s == SECSuccess);
      }
      void Final (byte *digest) {
	SECStatus s;
	unsigned int dummy;
	s = PK11_DigestFinal(ctx, digest, &dummy, DIGESTSIZE);
	assert(s == SECSuccess);
	assert(dummy == (unsigned int)DIGESTSIZE);
	Restart();
      }
    };
  }
}

#else
# error "No supported crypto implementation found."
#endif

#endif
