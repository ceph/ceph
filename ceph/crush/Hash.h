
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
	  
	  // RS Hash function, from Robert Sedgwicks Algorithms in C book, w/ some changes.
	  if (1) {
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

	}
	


	
	int operator()(int a) {
	  return myhash(a) ^ seed;
	}
	int operator()(int a, int b) {
	  return myhash(a) ^ myhash(b) ^ seed;
	}
	int operator()(int a, int b, int c) {
	  return myhash(a) ^ myhash(b) ^ myhash(c) ^ seed;
	}
  };

}
