#include "acconfig.h"
#include "include/int_types.h"
#include "common/crc32c_aarch64.h"

#ifndef HAVE_ARMV8_CRC_CRYPTO_INTRINSICS
/* Request crc extension capabilities from the assembler */
asm(".arch_extension crc");

#ifdef HAVE_ARMV8_CRYPTO
/* Request crypto extension capabilities from the assembler */
asm(".arch_extension crypto");
#endif

#define CRC32CX(crc, value) __asm__("crc32cx %w[c], %w[c], %x[v]":[c]"+r"(crc):[v]"r"(value))
#define CRC32CW(crc, value) __asm__("crc32cw %w[c], %w[c], %w[v]":[c]"+r"(crc):[v]"r"(value))
#define CRC32CH(crc, value) __asm__("crc32ch %w[c], %w[c], %w[v]":[c]"+r"(crc):[v]"r"(value))
#define CRC32CB(crc, value) __asm__("crc32cb %w[c], %w[c], %w[v]":[c]"+r"(crc):[v]"r"(value))

#define CRC32C3X8(ITR) \
	__asm__("crc32cx %w[c1], %w[c1], %x[v]":[c1]"+r"(crc1):[v]"r"(*((const uint64_t *)buffer + 42*1 + (ITR))));\
	__asm__("crc32cx %w[c2], %w[c2], %x[v]":[c2]"+r"(crc2):[v]"r"(*((const uint64_t *)buffer + 42*2 + (ITR))));\
	__asm__("crc32cx %w[c0], %w[c0], %x[v]":[c0]"+r"(crc0):[v]"r"(*((const uint64_t *)buffer + 42*0 + (ITR))));

#define CRC32C3X8_ZERO \
	__asm__("crc32cx %w[c0], %w[c0], xzr":[c0]"+r"(crc0));

#else /* HAVE_ARMV8_CRC_CRYPTO_INTRINSICS */

#include <arm_acle.h>
#include <arm_neon.h>

#define CRC32CX(crc, value) (crc) = __crc32cd((crc), (value))
#define CRC32CW(crc, value) (crc) = __crc32cw((crc), (value))
#define CRC32CH(crc, value) (crc) = __crc32ch((crc), (value))
#define CRC32CB(crc, value) (crc) = __crc32cb((crc), (value))

#define CRC32C3X8(ITR) \
	crc1 = __crc32cd(crc1, *((const uint64_t *)buffer + 42*1 + (ITR)));\
	crc2 = __crc32cd(crc2, *((const uint64_t *)buffer + 42*2 + (ITR)));\
	crc0 = __crc32cd(crc0, *((const uint64_t *)buffer + 42*0 + (ITR)));

#define CRC32C3X8_ZERO \
	crc0 = __crc32cd(crc0, (const uint64_t)0);

#endif /* HAVE_ARMV8_CRC_CRYPTO_INTRINSICS */

#define CRC32C7X3X8(ITR) do {\
	CRC32C3X8((ITR)*7+0) \
	CRC32C3X8((ITR)*7+1) \
	CRC32C3X8((ITR)*7+2) \
	CRC32C3X8((ITR)*7+3) \
	CRC32C3X8((ITR)*7+4) \
	CRC32C3X8((ITR)*7+5) \
	CRC32C3X8((ITR)*7+6) \
	} while(0)

#define CRC32C7X3X8_ZERO do {\
	CRC32C3X8_ZERO \
	CRC32C3X8_ZERO \
	CRC32C3X8_ZERO \
	CRC32C3X8_ZERO \
	CRC32C3X8_ZERO \
	CRC32C3X8_ZERO \
	CRC32C3X8_ZERO \
	} while(0)

#define PREF4X64L1(PREF_OFFSET, ITR) \
	__asm__("PRFM PLDL1KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 0)*64));\
	__asm__("PRFM PLDL1KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 1)*64));\
	__asm__("PRFM PLDL1KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 2)*64));\
	__asm__("PRFM PLDL1KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 3)*64));

#define PREF1KL1(PREF_OFFSET) \
	PREF4X64L1((PREF_OFFSET), 0) \
	PREF4X64L1((PREF_OFFSET), 4) \
	PREF4X64L1((PREF_OFFSET), 8) \
	PREF4X64L1((PREF_OFFSET), 12)

#define PREF4X64L2(PREF_OFFSET, ITR) \
	__asm__("PRFM PLDL2KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 0)*64));\
	__asm__("PRFM PLDL2KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 1)*64));\
	__asm__("PRFM PLDL2KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 2)*64));\
	__asm__("PRFM PLDL2KEEP, [%x[v],%[c]]"::[v]"r"(buffer), [c]"I"((PREF_OFFSET) + ((ITR) + 3)*64));

#define PREF1KL2(PREF_OFFSET) \
	PREF4X64L2((PREF_OFFSET), 0) \
	PREF4X64L2((PREF_OFFSET), 4) \
	PREF4X64L2((PREF_OFFSET), 8) \
	PREF4X64L2((PREF_OFFSET), 12)


uint32_t ceph_crc32c_aarch64(uint32_t crc, unsigned char const *buffer, unsigned len)
{
	int64_t length = len;
	uint32_t crc0, crc1, crc2;

	if (buffer) {
#ifdef HAVE_ARMV8_CRYPTO
#ifdef HAVE_ARMV8_CRC_CRYPTO_INTRINSICS
		/* Calculate reflected crc with PMULL Instruction */
		const poly64_t k1 = 0xe417f38a, k2 = 0x8f158014;
		uint64_t t0, t1;

		/* crc done "by 3" for fixed input block size of 1024 bytes */
		while ((length -= 1024) >= 0) {
			/* Prefetch data for following block to avoid cache miss */
			PREF1KL2(1024*3);
			/* Do first 8 bytes here for better pipelining */
			crc0 = __crc32cd(crc, *(const uint64_t *)buffer);
			crc1 = 0;
			crc2 = 0;
			buffer += sizeof(uint64_t);

			/* Process block inline
			Process crc0 last to avoid dependency with above */
			CRC32C7X3X8(0);
			CRC32C7X3X8(1);
			CRC32C7X3X8(2);
			CRC32C7X3X8(3);
			CRC32C7X3X8(4);
			CRC32C7X3X8(5);

			buffer += 42*3*sizeof(uint64_t);
			/* Prefetch data for following block to avoid cache miss */
			PREF1KL1(1024);

			/* Merge crc0 and crc1 into crc2
			   crc1 multiply by K2
			   crc0 multiply by K1 */

			t1 = (uint64_t)vmull_p64(crc1, k2);
			t0 = (uint64_t)vmull_p64(crc0, k1);
			crc = __crc32cd(crc2, *(const uint64_t *)buffer);
			crc1 = __crc32cd(0, t1);
			crc ^= crc1;
			crc0 = __crc32cd(0, t0);
			crc ^= crc0;

			buffer += sizeof(uint64_t);
		}
#else /* !HAVE_ARMV8_CRC_CRYPTO_INTRINSICS */
		__asm__("mov    x16,            #0xf38a         \n\t"
			"movk   x16,            #0xe417, lsl 16 \n\t"
			"mov    v1.2d[0],       x16             \n\t"
			"mov    x16,            #0x8014         \n\t"
			"movk   x16,            #0x8f15, lsl 16 \n\t"
			"mov    v0.2d[0],       x16             \n\t"
			:::"x16");

		while ((length -= 1024) >= 0) {
			PREF1KL2(1024*3);
			__asm__("crc32cx %w[c0], %w[c], %x[v]\n\t"
				:[c0]"=r"(crc0):[c]"r"(crc), [v]"r"(*(const uint64_t *)buffer):);
			crc1 = 0;
			crc2 = 0;
			buffer += sizeof(uint64_t);

			CRC32C7X3X8(0);
			CRC32C7X3X8(1);
			CRC32C7X3X8(2);
			CRC32C7X3X8(3);
			CRC32C7X3X8(4);
			CRC32C7X3X8(5);

			buffer += 42*3*sizeof(uint64_t);
			PREF1KL1(1024);
			__asm__("mov            v2.2d[0],       %x[c1]          \n\t"
				"pmull          v2.1q,          v2.1d,  v0.1d   \n\t"
				"mov            v3.2d[0],       %x[c0]          \n\t"
				"pmull          v3.1q,          v3.1d,  v1.1d   \n\t"
				"crc32cx        %w[c],          %w[c2], %x[v]   \n\t"
				"mov            %x[c1],         v2.2d[0]        \n\t"
				"crc32cx        %w[c1],         wzr,    %x[c1]  \n\t"
				"eor            %w[c],          %w[c],  %w[c1]  \n\t"
				"mov            %x[c0],         v3.2d[0]        \n\t"
				"crc32cx        %w[c0],         wzr,    %x[c0]  \n\t"
				"eor            %w[c],          %w[c],  %w[c0]  \n\t"
				:[c1]"+r"(crc1), [c0]"+r"(crc0), [c2]"+r"(crc2), [c]"+r"(crc)
				:[v]"r"(*((const uint64_t *)buffer)));
			buffer += sizeof(uint64_t);
		}
#endif /* HAVE_ARMV8_CRC_CRYPTO_INTRINSICS */

		if(!(length += 1024))
			return crc;

#endif /* HAVE_ARMV8_CRYPTO */
		while ((length -= sizeof(uint64_t)) >= 0) {
			CRC32CX(crc, *(uint64_t *)buffer);
			buffer += sizeof(uint64_t);
		}

		/* The following is more efficient than the straight loop */
		if (length & sizeof(uint32_t)) {
			CRC32CW(crc, *(uint32_t *)buffer);
			buffer += sizeof(uint32_t);
		}
		if (length & sizeof(uint16_t)) {
			CRC32CH(crc, *(uint16_t *)buffer);
			buffer += sizeof(uint16_t);
		}
		if (length & sizeof(uint8_t))
			CRC32CB(crc, *buffer);
	} else {
#ifdef HAVE_ARMV8_CRYPTO
#ifdef HAVE_ARMV8_CRC_CRYPTO_INTRINSICS
		const poly64_t k1 = 0xe417f38a;
		uint64_t t0;

		while ((length -= 1024) >= 0) {
			crc0 = __crc32cd(crc, 0);

			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;

			/* Merge crc0 into crc: crc0 multiply by K1 */

			t0 = (uint64_t)vmull_p64(crc0, k1);
			crc = __crc32cd(0, t0);
		}
#else /* !HAVE_ARMV8_CRC_CRYPTO_INTRINSICS */
		__asm__("mov    x16,            #0xf38a         \n\t"
			"movk   x16,            #0xe417, lsl 16 \n\t"
			"mov    v1.2d[0],       x16             \n\t"
			:::"x16");

		while ((length -= 1024) >= 0) {
			__asm__("crc32cx %w[c0], %w[c], xzr\n\t"
				:[c0]"=r"(crc0):[c]"r"(crc));

			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;
			CRC32C7X3X8_ZERO;

			__asm__("mov            v3.2d[0],       %x[c0]          \n\t"
				"pmull          v3.1q,          v3.1d,  v1.1d   \n\t"
				"mov            %x[c0],         v3.2d[0]        \n\t"
				"crc32cx        %w[c],          wzr,    %x[c0]  \n\t"
				:[c]"=r"(crc)
				:[c0]"r"(crc0));
		}
#endif /* HAVE_ARMV8_CRC_CRYPTO_INTRINSICS */

		if(!(length += 1024))
			return crc;

#endif /* HAVE_ARMV8_CRYPTO */
		while ((length -= sizeof(uint64_t)) >= 0)
			CRC32CX(crc, 0);

		/* The following is more efficient than the straight loop */
		if (length & sizeof(uint32_t))
			CRC32CW(crc, 0);

		if (length & sizeof(uint16_t))
			CRC32CH(crc, 0);

		if (length & sizeof(uint8_t))
			CRC32CB(crc, 0);
	}
	return crc;
}
