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

		if (slen-- == 0) {
			ret = 1;
			break;
		}
		if ((q = base64_table_dec_8bit[*s++]) >= 254) {
			st.eof = BASE64_EOF;
			// Treat character '=' as invalid for byte 0:
			break;
		}
		st.carry = q << 2;
		st.bytes++;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 1:	if (slen-- == 0) {
			ret = 1;
			break;
		}
		if ((q = base64_table_dec_8bit[*s++]) >= 254) {
			st.eof = BASE64_EOF;
			// Treat character '=' as invalid for byte 1:
			break;
		}
		*o++ = st.carry | (q >> 4);
		st.carry = q << 4;
		st.bytes++;
		olen++;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 2:	if (slen-- == 0) {
			ret = 1;
			break;
		}
		if ((q = base64_table_dec_8bit[*s++]) >= 254) {
			st.bytes++;
			// When q == 254, the input char is '='.
			// Check if next byte is also '=':
			if (q == 254) {
				if (slen-- != 0) {
					st.bytes = 0;
					// EOF:
					st.eof = BASE64_EOF;
					q = base64_table_dec_8bit[*s++];
					ret = ((q == 254) && (slen == 0)) ? 1 : 0;
					break;
				}
				else {
					// Almost EOF
					st.eof = BASE64_AEOF;
					ret = 1;
					break;
				}
			}
			// If we get here, there was an error:
			break;
		}
		*o++ = st.carry | (q >> 2);
		st.carry = q << 6;
		st.bytes++;
		olen++;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 3:	if (slen-- == 0) {
			ret = 1;
			break;
		}
		if ((q = base64_table_dec_8bit[*s++]) >= 254) {
			st.bytes = 0;
			st.eof = BASE64_EOF;
			// When q == 254, the input char is '='. Return 1 and EOF.
			// When q == 255, the input char is invalid. Return 0 and EOF.
			ret = ((q == 254) && (slen == 0)) ? 1 : 0;
			break;
		}
		*o++ = st.carry | q;
		st.carry = 0;
		st.bytes = 0;
		olen++;
	}
}

state->eof = st.eof;
state->bytes = st.bytes;
state->carry = st.carry;
*outlen = olen;
return ret;
