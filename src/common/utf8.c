// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/utf8.h"

#include <stdio.h>
#include <string.h>

static int high_bits_set(int c)
{
	int ret = 0;
	while (1) {
		if ((c & 0x80) != 0x080)
			break;
		c <<= 1;
		++ret;
	}
	return ret;
}

/* Encode a 31-bit UTF8 code point to 'buf'.
 * Assumes buf is of size MAX_UTF8_SZ
 * Returns -1 on failure; number of bytes in the encoded value otherwise.
 */
int encode_utf8(unsigned long u, unsigned char *buf)
{
	int i;
	unsigned long max_val[MAX_UTF8_SZ] = {
		0x0000007ful, 0x000007fful, 0x0000fffful,
		0x001ffffful, 0x03fffffful, 0x7ffffffful
	};
	static const int MAX_VAL_SZ = sizeof(max_val) / sizeof(max_val[0]);

	for (i = 0; i < MAX_VAL_SZ; ++i) {
		if (u <= max_val[i])
			break;
	}
	if (i == MAX_VAL_SZ) {
		// This code point is too big to encode.
		return -1;
	}

	if (i == 0) {
		buf[0] = u;
	}
	else {
		signed int j;
		for (j = i; j > 0; --j) {
			buf[j] = 0x80 | (u & 0x3f);
			u >>= 6;
		}

		unsigned char mask = ~(0xFF >> (i + 1));
		buf[0] = mask | u;
	}

	return i + 1;
}

/*
 * Decode a UTF8 character from an array of bytes. Return character code.
 * Upon error, return INVALID_UTF8_CHAR.
 */
unsigned long decode_utf8(unsigned char *buf, int nbytes)
{
	unsigned long code;
	int i, j;

	if (nbytes <= 0)
		return INVALID_UTF8_CHAR;

	if (nbytes == 1) {
		if (buf[0] >= 0x80)
			return INVALID_UTF8_CHAR;
		return buf[0];
	}

	i = high_bits_set(buf[0]);
	if (i != nbytes)
		return INVALID_UTF8_CHAR;
	code = buf[0] & (0xff >> i);
	for (j = 1; j < nbytes; ++j) {
		if ((buf[j] & 0xc0) != 0x80)
			    return INVALID_UTF8_CHAR;
		code = (code << 6) | (buf[j] & 0x3f);
	}

	// Check for invalid code points
	if (code == 0xFFFE)
	    return INVALID_UTF8_CHAR;
	if (code == 0xFFFF)
	    return INVALID_UTF8_CHAR;
	if (code >= 0xD800 && code <= 0xDFFF)
	    return INVALID_UTF8_CHAR;

	return code;
}

int check_utf8(const char *buf, int len)
{
	unsigned char u[MAX_UTF8_SZ];
	int enc_len = 0;
	int i = 0;
	while (1) {
		unsigned int c = buf[i];
		if (i >= len || c < 0x80 || (c & 0xC0) != 0x80) {
			// the start of a new character. Process what we have
			// in the buffer.
			if (enc_len > 0) {
				int re_encoded_len;
				unsigned char re_encoded[MAX_UTF8_SZ];
				unsigned long code = decode_utf8(u, enc_len);
				if (code == INVALID_UTF8_CHAR) {
					//printf("decoded to invalid utf8");
					return i + 1;
				}
				re_encoded_len = encode_utf8(code, re_encoded);
				if (enc_len != re_encoded_len) {
					//printf("originally encoded as %d bytes, "
					//	"but was re-encoded to %d!\n",
					//	enc_len, re_encoded_len);
					return i + 1;
				}
				if (memcmp(u, re_encoded, enc_len) != 0) {
					//printf("re-encoded to a different "
					//	"byte stream!");
					return i + 1;
				}
				//printf("code_point %lu\n", code);
			}
			enc_len = 0;
			if (i >= len)
				break;
			// start collecting again?
			if (c >= 0x80)
				u[enc_len++] = c;
		} else {
			if (enc_len == MAX_UTF8_SZ) {
				//printf("too many enc_len in utf character!\n");
				return i + 1;
			}
			//printf("continuation byte...\n");
			u[enc_len++] = c;
		}
		++i;
	}
	return 0;
}

int check_utf8_cstr(const char *buf)
{
	return check_utf8(buf, strlen(buf));
}

int is_control_character(int c)
{
	return (((c != 0) && (c < 0x20)) || (c == 0x7f));
}

int check_for_control_characters(const char *buf, int len)
{
	int i;
	for (i = 0; i < len; ++i) {
		if (is_control_character((int)(unsigned char)buf[i])) {
 			return i + 1;
		}
	}
	return 0;
}

int check_for_control_characters_cstr(const char *buf)
{
	return check_for_control_characters(buf, strlen(buf));
}
