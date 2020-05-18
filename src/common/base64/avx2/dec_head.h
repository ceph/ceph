/*
 * Copyright (c) 2020, Alfred Klomp <git@alfredklomp.com>
 * Alfred Klomp <git@alfredklomp.com>
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

int ret = 0;
const uint8_t *s = (const uint8_t *) src;
uint8_t *o = (uint8_t *) out;
uint8_t q;

// Use local temporaries to avoid cache thrashing:
size_t olen = 0;
size_t slen = srclen;
struct base64_state st;
st.eof = state->eof;
st.bytes = state->bytes;
st.carry = state->carry;

// If we previously saw an EOF or an invalid character, bail out:
if (st.eof) {
	*outlen = 0;
	ret = 0;
	// If there was a trailing '=' to check, check it:
	if (slen && (st.eof == BASE64_AEOF)) {
		state->bytes = 0;
		state->eof = BASE64_EOF;
		ret = ((base64_table_dec_8bit[*s++] == 254) && (slen == 1)) ? 1 : 0;
	}
	return ret;
}

// Turn four 6-bit numbers into three bytes:
// out[0] = 11111122
// out[1] = 22223333
// out[2] = 33444444

// Duff's device again:
switch (st.bytes)
{
	for (;;)
	{
	case 0:
