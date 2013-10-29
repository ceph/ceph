#include <inttypes.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>


#ifdef __x86_64__

/*
 *  * Based on a posting to lkml by Austin Zhang <austin.zhang@intel.com>
 *   * Further based on the fio crc32c-intel.c implementation by Jens Axboe.
 *    *
 *     * Using hardware provided CRC32 instruction to accelerate the CRC32 disposal.
 *      * CRC32C polynomial:0x1EDC6F41(BE)/0x82F63B78(LE)
 *       * CRC32 is a new instruction in Intel SSE4.2, the reference can be found at:
 *        * http://www.intel.com/products/processor/manuals/
 *         * Intel(R) 64 and IA-32 Architectures Software Developer's Manual
 *          * Volume 2A: Instruction Set Reference, A-M
 *           */

#if BITS_PER_LONG == 64
#define REX_PRE "0x48, "
#define SCALE_F 8
#else
#define REX_PRE
#define SCALE_F 4
#endif

static uint32_t crc32c_intel_le_hw_byte(uint32_t crc, unsigned char const *data,
                                        unsigned length)
{
        while (length--) {
                __asm__ __volatile__(
                        ".byte 0xf2, 0xf, 0x38, 0xf0, 0xf1"
                        :"=S"(crc)
                        :"0"(crc), "c"(*data)
                );
                data++;
        }

        return crc;
}

uint32_t ceph_crc32c_le_intel(uint32_t crc, unsigned char const *data, unsigned length)
{
        unsigned int iquotient = length / SCALE_F;
        unsigned int iremainder = length % SCALE_F;
#if BITS_PER_LONG == 64
        uint64_t *ptmp = (uint64_t *) data;
#else
        uint32_t *ptmp = (uint32_t *) data;
#endif

        while (iquotient--) {
                __asm__ __volatile__(
                        ".byte 0xf2, " REX_PRE "0xf, 0x38, 0xf1, 0xf1;"
                        :"=S"(crc)
                        :"0"(crc), "c"(*ptmp)
                );
                ptmp++;
        }

        if (iremainder)
                crc = crc32c_intel_le_hw_byte(crc, (unsigned char *)ptmp,
                                 iremainder);

        return crc;
}


static void do_cpuid(unsigned int *eax, unsigned int *ebx, unsigned int *ecx,
                     unsigned int *edx)
{
        int id = *eax;

        asm("movl %4, %%eax;"
            "cpuid;"
            "movl %%eax, %0;"
            "movl %%ebx, %1;"
            "movl %%ecx, %2;"
            "movl %%edx, %3;"
                : "=r" (*eax), "=r" (*ebx), "=r" (*ecx), "=r" (*edx)
                : "r" (id)
                : "eax", "ebx", "ecx", "edx");
}

int ceph_have_crc32c_intel(void)
{
	/* i know how to check this on x86_64... */
	unsigned int eax = 1, ebx, ecx, edx;
	do_cpuid(&eax, &ebx, &ecx, &edx);
	if ((ecx & (1 << 20)) != 0)
		return 1;
	return 0;
}

#else /* __LP64__ */

uint32_t ceph_crc32c_le_intel(uint32_t crc, unsigned char const *data, unsigned length)
{
	return 0;  /* this shouldn't get called! */
}

int ceph_have_crc32c_intel(void)
{
	return 0;  	/* clearly not x86_64 */
}

#endif
