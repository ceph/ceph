/*
 * Copyright (c) 2020, Alfred Klomp <git@alfredklomp.com>
 * Author: Alfred Klomp <git@alfredklomp.com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and documentation are those
 * of the authors and should not be interpreted as representing official policies,
 * either expressed or implied, of the FreeBSD Project.
 */

static inline void
enc_loop_avx2_inner_first (const uint8_t **s, uint8_t **o)
{
	// First load is done at s - 0 to not get a segfault:
	__m256i src = _mm256_loadu_si256((__m256i *) *s);

	// Shift by 4 bytes, as required by enc_reshuffle:
	src = _mm256_permutevar8x32_epi32(src, _mm256_setr_epi32(0, 0, 1, 2, 3, 4, 5, 6));

	// Reshuffle, translate, store:
	src = enc_reshuffle(src);
	src = enc_translate(src);
	_mm256_storeu_si256((__m256i *) *o, src);

	// Subsequent loads will be done at s - 4, set pointer for next round:
	*s += 20;
	*o += 32;
}

static inline void
enc_loop_avx2_inner (const uint8_t **s, uint8_t **o)
{
	// Load input:
	__m256i src = _mm256_loadu_si256((__m256i *) *s);

	// Reshuffle, translate, store:
	src = enc_reshuffle(src);
	src = enc_translate(src);
	_mm256_storeu_si256((__m256i *) *o, src);

	*s += 24;
	*o += 32;
}

static inline void
enc_loop_avx2 (const uint8_t **s, size_t *slen, uint8_t **o, size_t *olen)
{
	if (*slen < 32) {
		return;
	}

	// Process blocks of 24 bytes at a time. Because blocks are loaded 32
	// bytes at a time an offset of -4, ensure that there will be at least
	// 4 remaining bytes after the last round, so that the final read will
	// not pass beyond the bounds of the input buffer:
	size_t rounds = (*slen - 4) / 24;

	*slen -= rounds * 24;   // 24 bytes consumed per round
	*olen += rounds * 32;   // 32 bytes produced per round

	// The first loop iteration requires special handling to ensure that
	// the read, which is done at an offset, does not underflow the buffer:
	enc_loop_avx2_inner_first(s, o);
	rounds--;

	while (rounds > 0) {
		if (rounds >= 8) {
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			rounds -= 8;
			continue;
		}
		if (rounds >= 4) {
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			rounds -= 4;
			continue;
		}
		if (rounds >= 2) {
			enc_loop_avx2_inner(s, o);
			enc_loop_avx2_inner(s, o);
			rounds -= 2;
			continue;
		}
		enc_loop_avx2_inner(s, o);
		break;
	}

	// Add the offset back:
	*s += 4;
}
