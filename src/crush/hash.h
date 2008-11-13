#ifndef _CRUSH_HASH_H
#define _CRUSH_HASH_H

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

#define crush_hash_seed 1315423911

static inline unsigned crush_hash32(unsigned a) {
	unsigned hash = crush_hash_seed ^ a;
	unsigned b = a;
	unsigned x = 231232;
	unsigned y = 1232;
	hashmix(b, x, hash);
	hashmix(y, a, hash);
	return (hash & 0xFFFFFFFF);
}

static inline unsigned crush_hash32_2(unsigned a, unsigned b) {
	unsigned hash = crush_hash_seed ^ a ^ b;
	unsigned x = 231232;
	unsigned y = 1232;
	hashmix(a, b, hash);
	hashmix(x, a, hash);
	hashmix(b, y, hash);
	return (hash & 0xFFFFFFFF);
}

static inline unsigned crush_hash32_3(unsigned a, unsigned b, unsigned c) {
	unsigned int hash = crush_hash_seed ^ a ^ b ^ c;
	unsigned x = 231232;
	unsigned y = 1232;
	hashmix(a, b, hash);
	hashmix(c, x, hash);
	hashmix(y, a, hash);
	hashmix(b, x, hash);
	hashmix(y, c, hash);
	return (hash & 0xFFFFFFFF);
}

static inline unsigned crush_hash32_4(unsigned a, unsigned b, unsigned c, unsigned d) {
	unsigned int hash = crush_hash_seed ^a ^ b ^ c ^ d;
	unsigned x = 231232;
	unsigned y = 1232;
	hashmix(a, b, hash);
	hashmix(c, d, hash);
	hashmix(a, x, hash);
	hashmix(y, b, hash);
	hashmix(c, x, hash);
	hashmix(y, d, hash);
	return (hash & 0xFFFFFFFF);
}

static inline unsigned crush_hash32_5(unsigned a, unsigned b, unsigned c, unsigned d, unsigned e) {
	unsigned int hash = crush_hash_seed ^ a ^ b ^ c ^ d ^ e;
	unsigned x = 231232;
	unsigned y = 1232;
	hashmix(a, b, hash);
	hashmix(c, d, hash);
	hashmix(e, x, hash);
	hashmix(y, a, hash);
	hashmix(b, x, hash);
	hashmix(y, c, hash);
	hashmix(d, x, hash);
	hashmix(y, e, hash);
	return (hash & 0xFFFFFFFF);
}

#endif
