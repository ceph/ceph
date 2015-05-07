#ifndef CEPH_CRYPTO_H
#define CEPH_CRYPTO_H

#include "acconfig.h"

#define CEPH_CRYPTO_MD5_DIGESTSIZE 16
#define CEPH_CRYPTO_HMACSHA1_DIGESTSIZE 20
#define CEPH_CRYPTO_SHA1_DIGESTSIZE 20
#define CEPH_CRYPTO_SHA256_DIGESTSIZE 32

#ifdef USE_CRYPTOPP
# define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <string.h>
#include <cryptopp/md5.h>
#include <cryptopp/sha.h>
#include <cryptopp/hmac.h>

// reinclude our assert to clobber the system one
# include "include/assert.h"

namespace ceph {
  namespace crypto {
    void assert_init();
    void init(CephContext *cct);
    void shutdown();

    using CryptoPP::Weak::MD5;
    using CryptoPP::SHA1;
    using CryptoPP::SHA256;

    class HMACSHA1: public CryptoPP::HMAC<CryptoPP::SHA1> {
    public:
      HMACSHA1 (const byte *key, size_t length)
	: CryptoPP::HMAC<CryptoPP::SHA1>(key, length)
	{
	}
      ~HMACSHA1();
    };
  }
}
#elif defined(USE_NSS)
// you *must* use CRYPTO_CXXFLAGS in Makefile.am for including this include
# include <nss.h>
# include <pk11pub.h>

// NSS thinks a lot of fairly fundamental operations might potentially
// fail, because it has been written to support e.g. smartcards doing all
// the crypto operations. We don't want to contaminate too much code
// with error checking, and just say these really should never fail.
// This assert MUST NOT be compiled out, even on non-debug builds.
# include "include/assert.h"

// ugly bit of CryptoPP that we have to emulate here :(
typedef unsigned char byte;

namespace ceph {
  namespace crypto {
    void assert_init();
    void init(CephContext *cct);
    void shutdown();
    class Digest {
    private:
      PK11Context *ctx;
      SECOidTag sec_type;
      size_t digest_size;
    public:
      Digest (SECOidTag _type, size_t _digest_size) : sec_type(_type), digest_size(_digest_size) {
	ctx = PK11_CreateDigestContext(_type);
	assert(ctx);
	Restart();
      }
      ~Digest () {
	PK11_DestroyContext(ctx, PR_TRUE);
      }
      void Restart() {
	SECStatus s;
	s = PK11_DigestBegin(ctx);
	assert(s == SECSuccess);
      }
      void Update (const byte *input, size_t length) {
        if (length) {
	  SECStatus s;
	  s = PK11_DigestOp(ctx, input, length);
	  assert(s == SECSuccess);
        }
      }
      void Final (byte *digest) {
	SECStatus s;
	unsigned int dummy;
	s = PK11_DigestFinal(ctx, digest, &dummy, digest_size);
	assert(s == SECSuccess);
	assert(dummy == digest_size);
	Restart();
      }
    };
    class MD5 : public Digest {
    public:
      MD5 () : Digest(SEC_OID_MD5, CEPH_CRYPTO_MD5_DIGESTSIZE) { }
    };

    class SHA1 : public Digest {
    public:
      SHA1 () : Digest(SEC_OID_SHA1, CEPH_CRYPTO_SHA1_DIGESTSIZE) { }
    };

    class SHA256 : public Digest {
    public:
      SHA256 () : Digest(SEC_OID_SHA256, CEPH_CRYPTO_SHA256_DIGESTSIZE) { }
    };

    class HMACSHA1 {
    private:
      PK11SlotInfo *slot;
      PK11SymKey *symkey;
      PK11Context *ctx;
    public:
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
      ~HMACSHA1 ();
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
	s = PK11_DigestFinal(ctx, digest, &dummy, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
	assert(s == SECSuccess);
	assert(dummy == CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
	Restart();
      }
    };
  }
}

#else
# error "No supported crypto implementation found."
#endif

#endif
