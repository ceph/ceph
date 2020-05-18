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
			break;
		}
		*o++ = base64_table_enc_6bit[*s >> 2];
		st.carry = (*s++ << 4) & 0x30;
		st.bytes++;
		olen += 1;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 1:	if (slen-- == 0) {
			break;
		}
		*o++ = base64_table_enc_6bit[st.carry | (*s >> 4)];
		st.carry = (*s++ << 2) & 0x3C;
		st.bytes++;
		olen += 1;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 2:	if (slen-- == 0) {
			break;
		}
		*o++ = base64_table_enc_6bit[st.carry | (*s >> 6)];
		*o++ = base64_table_enc_6bit[*s++ & 0x3F];
		st.bytes = 0;
		olen += 2;
	}
}
state->bytes = st.bytes;
state->carry = st.carry;
*outlen = olen;
