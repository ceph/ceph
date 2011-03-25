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

#include "rgw/rgw_escape.h"

#include <stdio.h>
#include <string.h>

/*
 * Some functions for escaping RGW responses
 */

/* Static string length */
#define SSTRL(x) ((sizeof(x)/sizeof(x[0])) - 1)

#define LESS_THAN_ESCAPE		"&lt;"
#define AMPERSAND_ESCAPE		"&amp;"
#define GREATER_THAN_ESCAPE		"&gt;"
#define SGL_QUOTE_ESCAPE		"&apos;"
#define DBL_QUOTE_ESCAPE		"&quot;"

int escape_xml_attr_len(const char *buf)
{
	const char *b;
	int ret = 0;
	for (b = buf; *b; ++b) {
		char c = *b;
		switch (c) {
		case '<':
			ret += SSTRL(LESS_THAN_ESCAPE);
			break;
		case '&':
			ret += SSTRL(AMPERSAND_ESCAPE);
			break;
		case '>':
			ret += SSTRL(GREATER_THAN_ESCAPE);
			break;
		case '\'':
			ret += SSTRL(SGL_QUOTE_ESCAPE);
			break;
		case '"':
			ret += SSTRL(DBL_QUOTE_ESCAPE);
			break;
		default:
			// Escape control characters.
			if (((c < 0x20) && (c != 0x09) && (c != 0x0a)) ||
				    (c == 0x7f)) {
				ret += 6;
			}
			else {
				ret++;
			}
		}
	}
	// leave room for null terminator
	ret++;
	return ret;
}

void escape_xml_attr(const char *buf, char *out)
{
	char *o = out;
	const char *b;
	for (b = buf; *b; ++b) {
		char c = *b;
		switch (c) {
		case '<':
			memcpy(o, LESS_THAN_ESCAPE, SSTRL(LESS_THAN_ESCAPE));
			o += SSTRL(LESS_THAN_ESCAPE);
			break;
		case '&':
			memcpy(o, AMPERSAND_ESCAPE, SSTRL(AMPERSAND_ESCAPE));
			o += SSTRL(AMPERSAND_ESCAPE);
			break;
		case '>':
			memcpy(o, GREATER_THAN_ESCAPE, SSTRL(GREATER_THAN_ESCAPE));
			o += SSTRL(GREATER_THAN_ESCAPE);
			break;
		case '\'':
			memcpy(o, SGL_QUOTE_ESCAPE, SSTRL(SGL_QUOTE_ESCAPE));
			o += SSTRL(SGL_QUOTE_ESCAPE);
			break;
		case '"':
			memcpy(o, DBL_QUOTE_ESCAPE, SSTRL(DBL_QUOTE_ESCAPE));
			o += SSTRL(DBL_QUOTE_ESCAPE);
			break;
		default:
			// Escape control characters.
			if (((c < 0x20) && (c != 0x09) && (c != 0x0a)) ||
				    (c == 0x7f)) {
				sprintf(o, "&#x%02x;", c);
				o += 6;
			}
			else {
				*o++ = c;
			}
			break;
		}
	}
	// null terminator
	*o = '\0';
}
