#ifndef CEPH_HASH_H
#define CEPH_HASH_H

#include "acconfig.h"

// Robert Jenkins' function for mixing 32-bit values
// http://burtleburtle.net/bob/hash/evahash.html
// a, b = random bits, c = input and output

#define hashmix(a,b,c) \
	a=a-b;  a=a-c;  a=a^(c>>13); \
	b=b-c;  b=b-a;  b=b^(a<<8);  \
	c=c-a;  c=c-b;  c=c^(b>>13); \
	a=a-b;  a=a-c;  a=a^(c>>12); \
	b=b-c;  b=b-a;  b=b^(a<<16); \
	c=c-a;  c=c-b;  c=c^(b>>5);  \
	a=a-b;  a=a-c;  a=a^(c>>3); \
	b=b-c;  b=b-a;  b=b^(a<<10); \
	c=c-a;  c=c-b;  c=c^(b>>15);


//namespace ceph {

template <class _Key> struct rjhash { };

inline uint64_t rjhash64(uint64_t key) {
  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
  key = key ^ (key >> 24);
  key = (key + (key << 3)) + (key << 8); // key * 265
  key = key ^ (key >> 14);
  key = (key + (key << 2)) + (key << 4); // key * 21
  key = key ^ (key >> 28);
  key = key + (key << 31);
  return key;
}

inline uint32_t rjhash32(uint32_t a) {
  a = (a+0x7ed55d16) + (a<<12);
  a = (a^0xc761c23c) ^ (a>>19);
  a = (a+0x165667b1) + (a<<5);
  a = (a+0xd3a2646c) ^ (a<<9);
  a = (a+0xfd7046c5) + (a<<3);
  a = (a^0xb55a4f09) ^ (a>>16);
  return a;
}


template<> struct rjhash<uint32_t> {
  inline size_t operator()(const uint32_t x) const {
    return rjhash32(x);
  }
};

template<> struct rjhash<uint64_t> {
  inline size_t operator()(const uint64_t x) const {
    return rjhash64(x);
  }
};

//}



#endif
