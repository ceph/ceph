// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


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

namespace crush {
  
  class Hash {
    int seed;

  public:
    int get_seed() { return seed; }
    void set_seed(int s) { seed = s; }

    Hash(int s) {
      unsigned int hash = 1315423911;
      int x = 231232;
      int y = 1232;
      hashmix(s, x, hash);
      hashmix(y, s, hash);
      seed = s;
    }

    inline int operator()(int a) {
      unsigned int hash = seed ^ a;
      int b = a;
      int x = 231232;
      int y = 1232;
      hashmix(b, x, hash);
      hashmix(y, a, hash);
      return (hash & 0x7FFFFFFF);
    }

    inline int operator()(int a, int b) {
      unsigned int hash = seed ^ a ^ b;
      int x = 231232;
      int y = 1232;
      hashmix(a, b, hash);
      hashmix(x, a, hash);
      hashmix(b, y, hash);
      return (hash & 0x7FFFFFFF);
    }

    inline int operator()(int a, int b, int c) {
      unsigned int hash = seed ^ a ^ b ^ c;
      int x = 231232;
      int y = 1232;
      hashmix(a, b, hash);
      hashmix(c, x, hash);
      hashmix(y, a, hash);
      hashmix(b, x, hash);
      hashmix(y, c, hash);
      return (hash & 0x7FFFFFFF);
    }

    inline int operator()(int a, int b, int c, int d) {
      unsigned int hash = seed ^a ^ b ^ c ^ d;
      int x = 231232;
      int y = 1232;
      hashmix(a, b, hash);
      hashmix(c, d, hash);
      hashmix(a, x, hash);
      hashmix(y, b, hash);
      hashmix(c, x, hash);
      hashmix(y, d, hash);
      return (hash & 0x7FFFFFFF);
    }

    inline int operator()(int a, int b, int c, int d, int e) {
      unsigned int hash = seed ^ a ^ b ^ c ^ d ^ e;
      int x = 231232;
      int y = 1232;
      hashmix(a, b, hash);
      hashmix(c, d, hash);
      hashmix(e, x, hash);
      hashmix(y, a, hash);
      hashmix(b, x, hash);
      hashmix(y, c, hash);
      hashmix(d, x, hash);
      hashmix(y, e, hash);
      return (hash & 0x7FFFFFFF);
    }
  };

}



#if 0


      //return myhash(a) ^ seed;
      return myhash(a, seed);
    }
    int operator()(int a, int b) {
      //return myhash( myhash(a) ^ myhash(b) ^ seed );
      return myhash(a, b, seed);
    }
    int operator()(int a, int b, int c) {
      //return myhash( myhash(a ^ seed) ^ myhash(b ^ seed) ^ myhash(c ^ seed) ^ seed );
      return myhash(a, b, c, seed);
    }
    int operator()(int a, int b, int c, int d) {
      //return myhash( myhash(a ^ seed) ^ myhash(b ^ seed) ^ myhash(c ^ seed) ^ myhash(d ^ seed) ^ seed );
      return myhash(a, b, c, d, seed);
    }

      // ethan's rush hash?
      if (0) 
        return (n ^ 0xdead1234) * (884811920 * 3  + 1);

      if (1) {

        // before
        hash ^= ((hash << 5) + (n&255) + (hash >> 2));
        hashmix(a, b, hash);
        n = n >> 8;
        hash ^= ((hash << 5) + (n&255) + (hash >> 2));
        hashmix(a, b, hash);
        n = n >> 8;
        hash ^= ((hash << 5) + (n&255) + (hash >> 2));
        hashmix(a, b, hash);
        n = n >> 8;
        hash ^= ((hash << 5) + (n&255) + (hash >> 2));
        hashmix(a, b, hash);
        n = n >> 8;

        //return hash;
        return (hash & 0x7FFFFFFF);
      }

      // JS
      //  a little better than RS
      //  + jenkin's mixing thing (which sucks on its own but helps tons here)
      //  best so far
      if (1) {
        unsigned int hash = 1315423911;
        int a = 231232;
        int b = 1232;
        
        for(unsigned int i = 0; i < 4; i++)
          {
            hash ^= ((hash << 5) + (n&255) + (hash >> 2));
            hashmix(a, b, hash);
            n = n >> 8;
          }
        
        return (hash & 0x7FFFFFFF);
      }

      
      // Robert jenkins' 96 bit mix
      //  sucks
      if (0) {
        int c = n;
        int a = 12378912;
        int b = 2982827;
        a=a-b;  a=a-c;  a=a^(c>>13); 
        b=b-c;  b=b-a;  b=b^(a<<8);  
        c=c-a;  c=c-b;  c=c^(b>>13); 
        a=a-b;  a=a-c;  a=a^(c>>12); 
        b=b-c;  b=b-a;  b=b^(a<<16); 
        c=c-a;  c=c-b;  c=c^(b>>5);  
        a=a-b;  a=a-c;  a=a^(c>>3); 
        b=b-c;  b=b-a;  b=b^(a<<10); 
        c=c-a;  c=c-b;  c=c^(b>>15); 
        return c;
      }
      // robert jenkins 32-bit
      //  sucks
      if (0) {
        n += (n << 12);
        n ^= (n >> 22);
        n += (n << 4);
        n ^= (n >> 9);
        n += (n << 10);
        n ^= (n >> 2);
        n += (n << 7);
        n ^= (n >> 12);
        return n;
      }

      // djb2
      if (0) {
        unsigned int hash = 5381;
        for (int i=0; i<4; i++) {
          hash = ((hash << 5) + hash) + ((n&255) ^ 123);
          n = n >> 8;
        }
        return hash;
      }


      // SDBM
      if (1) {
        unsigned int hash = 0;
        
        for(unsigned int i = 0; i < 4; i++)
          {
            hash = (n&255) + (hash << 6) + (hash << 16) - hash;
            n = n >> 8;
          }
        
        return (hash & 0x7FFFFFFF);
      }

      // PJW
      //  horrid
      if (0) {
        unsigned int BitsInUnsignedInt = (unsigned int)(sizeof(unsigned int) * 8);
        unsigned int ThreeQuarters     = (unsigned int)((BitsInUnsignedInt  * 3) / 4);
        unsigned int OneEighth         = (unsigned int)(BitsInUnsignedInt / 8);
        unsigned int HighBits          = (unsigned int)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
        unsigned int hash              = 0;
        unsigned int test              = 0;
        
        for(unsigned int i = 0; i < 4; i++)
          {
            hash = (hash << OneEighth) + (n&255);
            
            if((test = hash & HighBits)  != 0)
              {
                hash = (( hash ^ (test >> ThreeQuarters)) & (~HighBits));
              }
            n = n >> 8;
          }
        
        return (hash & 0x7FFFFFFF);
      }

      // RS Hash function, from Robert Sedgwicks Algorithms in C book, w/ some changes.
      if (0) {
        unsigned int b    = 378551;
        unsigned int a    = 63689;
        unsigned int hash = 0;
        
        for(unsigned int i=0; i<4; i++)
          {
            hash = hash * a + (n&0xff);
            a    = a * b;
            n = n >> 8;
          }
        
        return (hash & 0x7FFFFFFF);
      }

      // DJB
      //  worse than rs
      if (0) {
        unsigned int hash = 5381;
        
        for(unsigned int i = 0; i < 4; i++)
          {
            hash = ((hash << 5) + hash) + (n&255);
            n = n >> 8;
          }
        
        return (hash & 0x7FFFFFFF);
      }

      // AP
      //  even worse
      if (1) {
        unsigned int hash = 0;
        
        for(unsigned int i = 0; i < 4; i++)
          {
            hash ^= ((i & 1) == 0) ? (  (hash <<  7) ^ (n&255) ^ (hash >> 3)) :
              (~((hash << 11) ^ (n&255) ^ (hash >> 5)));
            n = n >> 8;
          }
        
        return (hash & 0x7FFFFFFF);
      }


#endif
