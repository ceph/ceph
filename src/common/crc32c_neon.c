/*
 * This code is lifted from a pending patch to Hadoop, found here:
 *
 *   http://lists.linaro.org/pipermail/linaro-toolchain/2013-April/003282.html
 *
 * to the bulk_crc32.c file, which is licensed:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * The author is:
 *
 *   Steve Capper <steve.capper at linaro.org>
 *
 */

#include <inttypes.h>

#ifdef __arm__

#include <arm_neon.h>
#include "include/crc32c.h"
#include "common/sctp_crc32.h"

#define crc32c_fallback ceph_crc32c_sctp

/*
 * Functions to reduce the size of the input buffer (fold) on ARM
 * NEON. The smaller buffer has the same CRC32c checksum as the
 * original.
 *
 * Most of the NEON buffer folding work takes place in the function
 * below. We do the following:
 * 1) 4 sets of vmull.p8's
 * 2) Combine these to give a "vmull.p32" (lf3)
 * 3) Shift left 1 bit to account for the endianess of multiplication.
 *
 * The folding and multiplication logic can be found documented at:
 * https://wiki.linaro.org/LEG/Engineering/CRC
 */
static inline uint64x1_t crc32c_neon_proc_part(poly8x8_t lhs, poly8x8_t rhs1,
	poly8x8_t rhs2, poly8x8_t rhs3, poly8x8_t rhs4)
{
	poly16x8_t lm1, lm2, lm3, lm4;
	poly16x4x2_t lz1, lz2;
	uint16x4_t le1, le2;
	uint32x2_t le3;
	uint32x4_t ls1, ls2, lf1, lf2;
	uint64x2_t ls3, le4;
	uint64x1_t lf3, lf4;

	lm1 = vmull_p8(lhs, rhs1);
	lm2 = vmull_p8(lhs, rhs2);
	lz1 = vuzp_p16(vget_low_p16(lm2), vget_high_p16(lm2));
	le1 = veor_u16(vreinterpret_u16_p16(lz1.val[0]),
			vreinterpret_u16_p16(lz1.val[1]));
	ls1 = vshll_n_u16(le1, 8);
	lf1 = veorq_u32(ls1, vreinterpretq_u32_p16(lm1));

	lm3 = vmull_p8(lhs, rhs3);
	lm4 = vmull_p8(lhs, rhs4);
	lz2 = vuzp_p16(vget_low_p16(lm4), vget_high_p16(lm4));
	le2 = veor_u16(vreinterpret_u16_p16(lz2.val[0]),
			vreinterpret_u16_p16(lz2.val[1]));
	ls2 = vshll_n_u16(le2, 8);
	lf2 = veorq_u32(ls2, vreinterpretq_u32_p16(lm3));

	le3 = veor_u32(vget_low_u32(lf2), vget_high_u32(lf2));
	ls3 = vshll_n_u32(le3, 16);
	le4 = veorq_u64(ls3, vreinterpretq_u64_u32(lf1));
	lf3 = vreinterpret_u64_u32(veor_u32(vget_low_u32(vreinterpretq_u32_u64(le4)),
					vget_high_u32(vreinterpretq_u32_u64(le4))));
	lf4 = vshl_n_u64(lf3, 1);
	return lf4;
}

uint32_t ceph_crc32c_neon(uint32_t crc, const uint8_t *buf, size_t length)
{
	poly8x8_t xor_constant, lhs1, lhs2, lhs3, lhs4, rhs1, rhs2, rhs3, rhs4;
	poly8x16_t lhl1, lhl2;

	uint64_t residues[4];
	uint32_t loop;

	if (length % 32)
		return crc32c_fallback(crc, buf, length);

	/*
	 * because crc32c has an initial crc value of 0xffffffff, we need to
	 * pre-fold the buffer before folding begins proper.
	 * The following constant is computed by:
	 * 1) finding a 8x32 bit value that gives a 0xffffffff crc (with initial value 0)
	 *    (this will be 7x32 bit 0s and 1x32 bit constant)
	 * 2) run a buffer fold (with 0 xor_constant) on this 8x32 bit value to get the
	 *    xor_constant.
	 */
	xor_constant = vcreate_p8(0x3E43E474A2870290);

	if (crc != 0xffffffff)
		return crc32c_fallback(crc, buf, length);

	/* k1 = x^288 mod P(x) - bit reversed */
	/* k2 = x^256 mod P(x) - bit reversed */

	rhs1 = vcreate_p8(0x510AC59A9C25531D);	/* k2:k1 */
	rhs2 = vcreate_p8(0x0A519AC5259C1D53);	/* byte swap */
	rhs3 = vcreate_p8(0xC59A510A531D9C25);	/* half word swap */
	rhs4 = vcreate_p8(0x9AC50A511D53259C);	/* byte swap of half word swap */

	lhl1 = vld1q_p8((const poly8_t *) buf);
	lhl2 = vld1q_p8((const poly8_t *) buf + 16);

	lhs1 = vget_low_p8(lhl1);
	lhs2 = vget_high_p8(lhl1);
	lhs3 = vget_low_p8(lhl2);
	lhs4 = vget_high_p8(lhl2);

	/* pre-fold lhs4 */
	lhs4 = vreinterpret_p8_u16(veor_u16(vreinterpret_u16_p8(lhs4),
		vreinterpret_u16_p8(xor_constant)));

	for(loop = 0; loop < (length - 32)/32; ++loop) {
		uint64x1_t l1f4, l2f4, l3f4, l4f4;

		l1f4 = crc32c_neon_proc_part(lhs1, rhs1, rhs2, rhs3, rhs4);
		l2f4 = crc32c_neon_proc_part(lhs2, rhs1, rhs2, rhs3, rhs4);
		l3f4 = crc32c_neon_proc_part(lhs3, rhs1, rhs2, rhs3, rhs4);
		l4f4 = crc32c_neon_proc_part(lhs4, rhs1, rhs2, rhs3, rhs4);

		lhl1 = vld1q_p8((const poly8_t *) (buf + 32 * (loop + 1)));
		lhl2 = vld1q_p8((const poly8_t *) (buf + 32 * (loop + 1) + 16));

		__builtin_prefetch(buf + 32 * (loop + 2));

		lhs1 = vget_low_p8(lhl1);
		lhs2 = vget_high_p8(lhl1);
		lhs3 = vget_low_p8(lhl2);
		lhs4 = vget_high_p8(lhl2);

		lhs1 = vreinterpret_p8_u64(veor_u64(vreinterpret_u64_p8(lhs1), l1f4));
		lhs2 = vreinterpret_p8_u64(veor_u64(vreinterpret_u64_p8(lhs2), l2f4));
		lhs3 = vreinterpret_p8_u64(veor_u64(vreinterpret_u64_p8(lhs3), l3f4));
		lhs4 = vreinterpret_p8_u64(veor_u64(vreinterpret_u64_p8(lhs4), l4f4));
	}

	vst1q_p8((poly8_t *) &residues[0], vcombine_p8(lhs1, lhs2));
	vst1q_p8((poly8_t *) &residues[2], vcombine_p8(lhs3, lhs4));

	return crc32c_fallback(0, (const uint8_t *)residues, 32);
}

#else  /* __arm__ */

uint32_t ceph_crc32c_neon(uint32_t crc, unsigned char const *data, unsigned length)
{
	return 0;
}

#endif /* __arm__ */
