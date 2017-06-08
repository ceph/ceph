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

#include <errno.h>
#include <stdio.h>
#include <string.h>

int mime_encode_as_qp(const char *input, char *output, int outlen)
{
	int ret = 1;
	char *o = output;
	const unsigned char *i = (const unsigned char*)input;
	while (1) {
		int c = *i;
		if (c == '\0') {
			break;
		}
		else if ((c & 0x80) || (c == '=') || (is_control_character(c))) {
			if (outlen >= 3) {
				snprintf(o, outlen, "=%02X", c);
				outlen -= 3;
				o += 3;
			}
			else
				outlen = 0;
			ret += 3;
		}
		else {
			if (outlen >= 1) {
				snprintf(o, outlen, "%c", c);
				outlen -= 1;
				o += 1;
			}
			ret += 1;
		}
		++i;
	}
	return ret;
}

static inline signed int hexchar_to_int(unsigned int c)
{
	switch(c) {
	case '0':
		return 0;
	case '1':
		return 1;
	case '2':
		return 2;
	case '3':
		return 3;
	case '4':
		return 4;
	case '5':
		return 5;
	case '6':
		return 6;
	case '7':
		return 7;
	case '8':
		return 8;
	case '9':
		return 9;
	case 'A':
	case 'a':
		return 10;
	case 'B':
	case 'b':
		return 11;
	case 'C':
	case 'c':
		return 12;
	case 'D':
	case 'd':
		return 13;
	case 'E':
	case 'e':
		return 14;
	case 'F':
	case 'f':
		return 15;
	case '\0':
	default:
	    return -EDOM;
	}
}

int mime_decode_from_qp(const char *input, char *output, int outlen)
{
	int ret = 1;
	char *o = output;
	const unsigned char *i = (const unsigned char*)input;
	while (1) {
		unsigned int c = *i;
		if (c == '\0') {
			break;
		}
		else if (c & 0x80) {
			/* The high bit is never set in quoted-printable encoding! */
			return -EDOM;
		}
		else if (c == '=') {
			int high = hexchar_to_int(*++i);
			if (high < 0)
				return -EINVAL;
			int low = hexchar_to_int(*++i);
			if (low < 0)
				return -EINVAL;
			c = (high << 4) + low;
		}
		++i;

		if (outlen >= 1) {
			snprintf(o, outlen, "%c", c);
			outlen -= 1;
			o += 1;
		}
		ret += 1;
	}
	return ret;
}
