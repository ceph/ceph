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

static inline __m256i
dec_reshuffle (const __m256i in)
{
	// in, lower lane, bits, upper case are most significant bits, lower
	// case are least significant bits:
	// 00llllll 00kkkkLL 00jjKKKK 00JJJJJJ
	// 00iiiiii 00hhhhII 00ggHHHH 00GGGGGG
	// 00ffffff 00eeeeFF 00ddEEEE 00DDDDDD
	// 00cccccc 00bbbbCC 00aaBBBB 00AAAAAA

	const __m256i merge_ab_and_bc = _mm256_maddubs_epi16(in, _mm256_set1_epi32(0x01400140));
	// 0000kkkk LLllllll 0000JJJJ JJjjKKKK
	// 0000hhhh IIiiiiii 0000GGGG GGggHHHH
	// 0000eeee FFffffff 0000DDDD DDddEEEE
	// 0000bbbb CCcccccc 0000AAAA AAaaBBBB

	__m256i out = _mm256_madd_epi16(merge_ab_and_bc, _mm256_set1_epi32(0x00011000));
	// 00000000 JJJJJJjj KKKKkkkk LLllllll
	// 00000000 GGGGGGgg HHHHhhhh IIiiiiii
	// 00000000 DDDDDDdd EEEEeeee FFffffff
	// 00000000 AAAAAAaa BBBBbbbb CCcccccc

	// Pack bytes together in each lane:
	out = _mm256_shuffle_epi8(out, _mm256_setr_epi8(
		2, 1, 0, 6, 5, 4, 10, 9, 8, 14, 13, 12, -1, -1, -1, -1,
		2, 1, 0, 6, 5, 4, 10, 9, 8, 14, 13, 12, -1, -1, -1, -1));
	// 00000000 00000000 00000000 00000000
	// LLllllll KKKKkkkk JJJJJJjj IIiiiiii
	// HHHHhhhh GGGGGGgg FFffffff EEEEeeee
	// DDDDDDdd CCcccccc BBBBbbbb AAAAAAaa

	// Pack lanes:
	return _mm256_permutevar8x32_epi32(out, _mm256_setr_epi32(0, 1, 2, 4, 5, 6, -1, -1));
}
