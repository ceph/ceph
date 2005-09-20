
#include <ext/hash_map>
using namespace __gnu_cxx;

namespace crush {
  
  class Hash {
	hash<int> H;
	int seed;
  public:
	Hash(int s) : seed( myhash(s) ) {}

	unsigned myhash(unsigned n) {
	  // ethan's rush hash?
	  if (0) 
		return (n ^ 0xdead1234) * (884811920 * 3  + 1);
	  
	  // djb2
	  if (0) {
		unsigned int hash = 5381;
		for (int i=0; i<4; i++) {
		  hash = ((hash << 5) + hash) + ((n&255) ^ 123);
		  n = n >> 8;
		}
		return hash;
	  }

	  // JS
	  //  a little better than RS
	  if (1) {
		unsigned int hash = 1315423911;
		
		for(unsigned int i = 0; i < 4; i++)
		  {
			hash ^= ((hash << 5) + (n&255) + (hash >> 2));
			n = n >> 8;
		  }
		
		return (hash & 0x7FFFFFFF);
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


	}
	


	
	int operator()(int a) {
	  return myhash(a) ^ seed;
	}
	int operator()(int a, int b) {
	  return myhash( myhash(a) ^ myhash(b) ^ seed );
	}
	int operator()(int a, int b, int c) {
	  return myhash( myhash(a ^ seed) ^ myhash(b ^ seed) ^ myhash(c ^ seed) ^ seed );
	}
	int operator()(int a, int b, int c, int d) {
	  return myhash( myhash(a ^ seed) ^ myhash(b ^ seed) ^ myhash(c ^ seed) ^ myhash(d ^ seed) ^ seed );
	}
  };

}
