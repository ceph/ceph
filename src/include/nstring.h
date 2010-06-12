#ifndef CEPH_NSTRING_D44FAD468A084A2D994D171C04DE05BD
#define CEPH_NSTRING_D44FAD468A084A2D994D171C04DE05BD

#if 0
# include "tstring.h"
typedef tstring nstring;
#else
# include "cstring.h"
typedef cstring nstring;
#endif

#include "ceph_hash.h"

static inline bool operator==(const nstring &l, const char *s) {
  return strcmp(l.c_str(), s) == 0;
}

static inline bool operator==(const nstring &l, const nstring &r) {
  return l.length() == r.length() && memcmp(l.data(), r.data(), l.length()) == 0;
}
static inline bool operator!=(const nstring &l, const nstring &r) {
  return l.length() != r.length() || memcmp(l.data(), r.data(), l.length()) != 0;
}
static inline bool operator<(const nstring &l, const nstring &r) {
  return strcmp(l.c_str(), r.c_str()) < 0;
}
static inline bool operator<=(const nstring &l, const nstring &r) {
  return strcmp(l.c_str(), r.c_str()) <= 0;
}
static inline bool operator>(const nstring &l, const nstring &r) {
  return strcmp(l.c_str(), r.c_str()) > 0;
}
static inline bool operator>=(const nstring &l, const nstring &r) {
  return strcmp(l.c_str(), r.c_str()) >= 0;
}

static inline ostream& operator<<(ostream &out, const nstring &s) {
  return out << s.c_str();
}

namespace __gnu_cxx {
  template<> struct hash< nstring >
  {
    size_t operator()( const nstring& x ) const
    {
      //static hash<const char*> H;
      //return H(x.c_str());
      return ceph_str_hash_linux(x.c_str(), x.length());
    }
  };
}

#endif
