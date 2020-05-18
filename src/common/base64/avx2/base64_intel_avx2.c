/*
 * Copyright (c) 2020, Alfred Klomp <git@alfredklomp.com>
 * Author: Alfred Klomp <git@alfredklomp.com>
 * Author: Liu Changcheng <changcheng.liu@aliyun.com>
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

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#if defined(__linux__)
#include <linux/errno.h>
#else
#include <sys/errno.h>
#endif


#include "tables.h"
#include "env.h"

#include "base64_intel_avx2.h"

struct base64_state {
        int eof;
        int bytes;
        unsigned char carry;
};

#include <immintrin.h>

#include "dec_reshuffle.h"
#include "dec_loop.h"
#include "enc_translate.h"
#include "enc_reshuffle.h"
#include "enc_loop.h"

void base64_stream_encode_avx2(struct base64_state *state, const char *src, size_t srclen, char *out, size_t *outlen) {
	#include "enc_head.h"
	enc_loop_avx2(&s, &slen, &o, &olen);
	#include "enc_tail.h"
}

int base64_stream_decode_avx2(struct base64_state *state, const char *src, size_t srclen, char *out, size_t *outlen) {
	#include "dec_head.h"
	dec_loop_avx2(&s, &slen, &o, &olen);
	#include "dec_tail.h"
}

void base64_stream_encode_final( struct base64_state *state, char *out, size_t *outlen) {
    uint8_t *o = (uint8_t *)out;

    if (state->bytes == 1) {
        *o++ = base64_table_enc_6bit[state->carry];
        *o++ = '=';
        *o++ = '=';
        *outlen = 3;
        return;
    }
    if (state->bytes == 2) {
        *o++ = base64_table_enc_6bit[state->carry];
        *o++ = '=';
        *outlen = 2;
        return;
    }
    *outlen = 0;
}

int arch_intel_avx2_encode_base64(char *dst, const char *dst_end, const char *src, const char* src_end) {
    struct base64_state state = {0, 0, 0};
    size_t start_len = 0, tail_len = 0;
    ((void)(dst_end));
    base64_stream_encode_avx2(&state, src, (size_t)(src_end - src), dst, &start_len);
    base64_stream_encode_final(&state, dst + start_len, &tail_len);
    return start_len + tail_len;
}

int arch_intel_avx2_decode_base64(char *dst, const char *dst_end, const char *src, const char* src_end) {
    size_t out_len;
    struct base64_state state = {0, 0, 0};

    ((void)(dst_end));
    base64_stream_decode_avx2(&state, src, (size_t)(src_end - src), dst, &out_len);
    if (state.bytes == 0) {
       return out_len;
    }
    return -EINVAL;
}

int base64_intel_avx2_exists(void) {
    return 1;
}
