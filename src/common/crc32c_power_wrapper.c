#include "include/int_types.h"
#define CRC_TABLE
#include "crc32c_power_constants.h"
#include "sctp_crc32.h"
#define VMX_ALIGN	16
#define VMX_ALIGN_MASK	(VMX_ALIGN-1)

#ifdef REFLECT
static unsigned int crc32_align(unsigned int crc, unsigned char const *p,
			       unsigned long len)
{
	while (len--)
		crc = crc_table[(crc ^ *p++) & 0xff] ^ (crc >> 8);
	return crc;
}
#else
static unsigned int crc32_align(unsigned int crc, unsigned char const *p,
				unsigned long len)
{
	while (len--)
		crc = crc_table[((crc >> 24) ^ *p++) & 0xff] ^ (crc << 8);
	return crc;
}
#endif

unsigned int __crc32_vpmsum(unsigned int crc, unsigned char const *p,
			    unsigned long len);

uint32_t ceph_crc32c_power(unsigned int crc, unsigned char const *p,
			  unsigned len)
{
	unsigned int prealign;
	unsigned int tail;

#ifdef CRC_XOR
	crc ^= 0xffffffff;
#endif

        if(p)
        {
	    if (len < VMX_ALIGN + VMX_ALIGN_MASK) {
	             crc = crc32_align(crc, p, len);
		     goto out;
	    }

	    if ((unsigned long)p & VMX_ALIGN_MASK) {
	            prealign = VMX_ALIGN - ((unsigned long)p & VMX_ALIGN_MASK);
		    crc = crc32_align(crc, p, prealign);
		    len -= prealign;
		    p += prealign;
	    }

	    crc = __crc32_vpmsum(crc, p, len & ~VMX_ALIGN_MASK);

	    tail = len & VMX_ALIGN_MASK;
	    if (tail) {
		    p += len & ~VMX_ALIGN_MASK;
		    crc = crc32_align(crc, p, tail);
	    }
        }
        else
        {
           crc = ceph_crc32c_sctp(crc, 0, len); 
        }

out:
#ifdef CRC_XOR
	crc ^= 0xffffffff;
#endif

	return crc;
}
