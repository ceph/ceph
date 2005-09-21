
#include <ext/hash_map>
using namespace __gnu_cxx;

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
	hash<int> H;
	int seed;
  public:
	Hash(int s) : seed( myhash(s) ) {}

	unsigned myhash(unsigned n) {
	  // ethan's rush hash?
	  if (0) 
		return (n ^ 0xdead1234) * (884811920 * 3  + 1);
	  
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

	  // JS (+ mixing -sage)
	  //  a little better than RS
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
