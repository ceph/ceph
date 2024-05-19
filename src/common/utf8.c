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

#include <string.h>

/*
 * http://www.unicode.org/versions/Unicode6.0.0/ch03.pdf - page 94
 *
 * Table 3-7. Well-Formed UTF-8 Byte Sequences
 *
 * +--------------------+------------+-------------+------------+-------------+
 * | Code Points        | First Byte | Second Byte | Third Byte | Fourth Byte |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+0000..U+007F     | 00..7F     |             |            |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+0080..U+07FF     | C2..DF     | 80..BF      |            |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+0800..U+0FFF     | E0         | A0..BF      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+1000..U+CFFF     | E1..EC     | 80..BF      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+D000..U+D7FF     | ED         | 80..9F      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+E000..U+FFFF     | EE..EF     | 80..BF      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+10000..U+3FFFF   | F0         | 90..BF      | 80..BF     | 80..BF      |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+40000..U+FFFFF   | F1..F3     | 80..BF      | 80..BF     | 80..BF      |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+100000..U+10FFFF | F4         | 80..8F      | 80..BF     | 80..BF      |
 * +--------------------+------------+-------------+------------+-------------+
 */

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
	/* Unroll loop for common code points  */
	if (u <= 0x0000007F) {
		buf[0] = u;
		return 1;
	} else if (u <= 0x000007FF) {
		buf[0] = 0xC0 | (u >> 6);
		buf[1] = 0x80 | (u & 0x3F);
		return 2;
	} else if (u <= 0x0000FFFF) {
		buf[0] = 0xE0 | (u >> 12);
		buf[1] = 0x80 | ((u >> 6) & 0x3F);
		buf[2] = 0x80 | (u & 0x3F);
		return 3;
	} else if (u <= 0x001FFFFF) {
		buf[0] = 0xF0 | (u >> 18);
		buf[1] = 0x80 | ((u >> 12) & 0x3F);
		buf[2] = 0x80 | ((u >> 6) & 0x3F);
		buf[3] = 0x80 | (u & 0x3F);
		return 4;
	} else {
		/* Rare/illegal code points */
		if (u <= 0x03FFFFFF) {
			for (int i = 4; i >= 1; --i) {
				buf[i] = 0x80 | (u & 0x3F);
				u >>= 6;
			}
			buf[0] = 0xF8 | u;
			return 5;
		} else if (u <= 0x7FFFFFFF) {
			for (int i = 5; i >= 1; --i) {
				buf[i] = 0x80 | (u & 0x3F);
				u >>= 6;
			}
			buf[0] = 0xFC | u;
			return 6;
		}
		return -1;
	}
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
	/*
	 * "char" is "signed" on x86 but "unsigned" on aarch64 by default.
	 * Below code depends on signed/unsigned comparisons, define an
	 * unsigned buffer explicitly to fix the gap.
	 */
	const unsigned char *bufu = (const unsigned char *)buf;
	int err_pos = 1;

	while (len) {
		int nbytes;
		unsigned char byte1 = bufu[0];

		/* 00..7F */
		if (byte1 <= 0x7F) {
			nbytes = 1;
		/* C2..DF, 80..BF */
		} else if (len >= 2 && byte1 >= 0xC2 && byte1 <= 0xDF &&
				(signed char)bufu[1] <= (signed char)0xBF) {
			nbytes = 2;
		} else if (len >= 3) {
			unsigned char byte2 = bufu[1];

			/* Is byte2, byte3 between 0x80 ~ 0xBF */
			int byte2_ok = (signed char)byte2 <= (signed char)0xBF;
			int byte3_ok = (signed char)bufu[2] <= (signed char)0xBF;

			if (byte2_ok && byte3_ok &&
					/* E0, A0..BF, 80..BF */
					((byte1 == 0xE0 && byte2 >= 0xA0) ||
					 /* E1..EC, 80..BF, 80..BF */
					 (byte1 >= 0xE1 && byte1 <= 0xEC) ||
					 /* ED, 80..9F, 80..BF */
					 (byte1 == 0xED && byte2 <= 0x9F) ||
					 /* EE..EF, 80..BF, 80..BF */
					 (byte1 >= 0xEE && byte1 <= 0xEF))) {
				nbytes = 3;
			} else if (len >= 4) {
				/* Is byte4 between 0x80 ~ 0xBF */
				int byte4_ok = (signed char)bufu[3] <= (signed char)0xBF;

				if (byte2_ok && byte3_ok && byte4_ok &&
						/* F0, 90..BF, 80..BF, 80..BF */
						((byte1 == 0xF0 && byte2 >= 0x90) ||
						 /* F1..F3, 80..BF, 80..BF, 80..BF */
						 (byte1 >= 0xF1 && byte1 <= 0xF3) ||
						 /* F4, 80..8F, 80..BF, 80..BF */
						 (byte1 == 0xF4 && byte2 <= 0x8F))) {
					nbytes = 4;
				} else {
					return err_pos;
				}
			} else {
				return err_pos;
			}
		} else {
			return err_pos;
		}

		len -= nbytes;
		err_pos += nbytes;
		bufu += nbytes;
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
