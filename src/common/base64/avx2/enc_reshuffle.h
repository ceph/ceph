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
enc_reshuffle (const __m256i input)
{
	// Translation of the SSSE3 reshuffling algorithm to AVX2. This one
	// works with shifted (4 bytes) input in order to be able to work
	// efficiently in the two 128-bit lanes.

	// Input, bytes MSB to LSB:
	// 0 0 0 0 x w v u t s r q p o n m
	// l k j i h g f e d c b a 0 0 0 0

	const __m256i in = _mm256_shuffle_epi8(input, _mm256_set_epi8(
		10, 11,  9, 10,
		 7,  8,  6,  7,
		 4,  5,  3,  4,
		 1,  2,  0,  1,

		14, 15, 13, 14,
		11, 12, 10, 11,
		 8,  9,  7,  8,
		 5,  6,  4,  5));
	// in, bytes MSB to LSB:
	// w x v w
	// t u s t
	// q r p q
	// n o m n
	// k l j k
	// h i g h
	// e f d e
	// b c a b

	const __m256i t0 = _mm256_and_si256(in, _mm256_set1_epi32(0x0FC0FC00));
	// bits, upper case are most significant bits, lower case are least
	// significant bits.
	// 0000wwww XX000000 VVVVVV00 00000000
	// 0000tttt UU000000 SSSSSS00 00000000
	// 0000qqqq RR000000 PPPPPP00 00000000
	// 0000nnnn OO000000 MMMMMM00 00000000
	// 0000kkkk LL000000 JJJJJJ00 00000000
	// 0000hhhh II000000 GGGGGG00 00000000
	// 0000eeee FF000000 DDDDDD00 00000000
	// 0000bbbb CC000000 AAAAAA00 00000000

	const __m256i t1 = _mm256_mulhi_epu16(t0, _mm256_set1_epi32(0x04000040));
	// 00000000 00wwwwXX 00000000 00VVVVVV
	// 00000000 00ttttUU 00000000 00SSSSSS
	// 00000000 00qqqqRR 00000000 00PPPPPP
	// 00000000 00nnnnOO 00000000 00MMMMMM
	// 00000000 00kkkkLL 00000000 00JJJJJJ
	// 00000000 00hhhhII 00000000 00GGGGGG
	// 00000000 00eeeeFF 00000000 00DDDDDD
	// 00000000 00bbbbCC 00000000 00AAAAAA

	const __m256i t2 = _mm256_and_si256(in, _mm256_set1_epi32(0x003F03F0));
	// 00000000 00xxxxxx 000000vv WWWW0000
	// 00000000 00uuuuuu 000000ss TTTT0000
	// 00000000 00rrrrrr 000000pp QQQQ0000
	// 00000000 00oooooo 000000mm NNNN0000
	// 00000000 00llllll 000000jj KKKK0000
	// 00000000 00iiiiii 000000gg HHHH0000
	// 00000000 00ffffff 000000dd EEEE0000
	// 00000000 00cccccc 000000aa BBBB0000

	const __m256i t3 = _mm256_mullo_epi16(t2, _mm256_set1_epi32(0x01000010));
	// 00xxxxxx 00000000 00vvWWWW 00000000
	// 00uuuuuu 00000000 00ssTTTT 00000000
	// 00rrrrrr 00000000 00ppQQQQ 00000000
	// 00oooooo 00000000 00mmNNNN 00000000
	// 00llllll 00000000 00jjKKKK 00000000
	// 00iiiiii 00000000 00ggHHHH 00000000
	// 00ffffff 00000000 00ddEEEE 00000000
	// 00cccccc 00000000 00aaBBBB 00000000

	return _mm256_or_si256(t1, t3);
	// 00xxxxxx 00wwwwXX 00vvWWWW 00VVVVVV
	// 00uuuuuu 00ttttUU 00ssTTTT 00SSSSSS
	// 00rrrrrr 00qqqqRR 00ppQQQQ 00PPPPPP
	// 00oooooo 00nnnnOO 00mmNNNN 00MMMMMM
	// 00llllll 00kkkkLL 00jjKKKK 00JJJJJJ
	// 00iiiiii 00hhhhII 00ggHHHH 00GGGGGG
	// 00ffffff 00eeeeFF 00ddEEEE 00DDDDDD
	// 00cccccc 00bbbbCC 00aaBBBB 00AAAAAA
}
