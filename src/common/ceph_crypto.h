#ifndef CEPH_CRYPTO_H
#define CEPH_CRYPTO_H

#include "acconfig.h"

#ifdef USE_CRYPTOPP
# define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
# include <cryptopp/md5.h>
namespace ceph {
  namespace crypto {
    static inline void init() {
      // nothing
    }
    using CryptoPP::Weak::MD5;
  }
}
#elif USE_NSS
# error "TODO NSS support for md5"
#else
# error "No supported crypto implementation found."
#endif

#endif
