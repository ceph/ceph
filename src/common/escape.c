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

#include "common/escape.h"

#include <stdio.h>
#include <string.h>

/*
 * Some functions for escaping RGW responses
 */

/* Static string length */
#define SSTRL(x) ((sizeof(x)/sizeof(x[0])) - 1)

#define LESS_THAN_XESCAPE		"&lt;"
#define AMPERSAND_XESCAPE		"&amp;"
#define GREATER_THAN_XESCAPE		"&gt;"
#define SGL_QUOTE_XESCAPE		"&apos;"
#define DBL_QUOTE_XESCAPE		"&quot;"

int escape_xml_attr_len(const char *buf)
{
	const char *b;
	int ret = 0;
	for (b = buf; *b; ++b) {
		unsigned char c = *b;
		switch (c) {
		case '<':
			ret += SSTRL(LESS_THAN_XESCAPE);
			break;
		case '&':
			ret += SSTRL(AMPERSAND_XESCAPE);
			break;
		case '>':
			ret += SSTRL(GREATER_THAN_XESCAPE);
			break;
		case '\'':
			ret += SSTRL(SGL_QUOTE_XESCAPE);
			break;
		case '"':
			ret += SSTRL(DBL_QUOTE_XESCAPE);
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
		unsigned char c = *b;
		switch (c) {
		case '<':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, LESS_THAN_XESCAPE, SSTRL(LESS_THAN_XESCAPE));
			o += SSTRL(LESS_THAN_XESCAPE);
			break;
		case '&':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, AMPERSAND_XESCAPE, SSTRL(AMPERSAND_XESCAPE));
			o += SSTRL(AMPERSAND_XESCAPE);
			break;
		case '>':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, GREATER_THAN_XESCAPE, SSTRL(GREATER_THAN_XESCAPE));
			o += SSTRL(GREATER_THAN_XESCAPE);
			break;
		case '\'':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, SGL_QUOTE_XESCAPE, SSTRL(SGL_QUOTE_XESCAPE));
			o += SSTRL(SGL_QUOTE_XESCAPE);
			break;
		case '"':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, DBL_QUOTE_XESCAPE, SSTRL(DBL_QUOTE_XESCAPE));
			o += SSTRL(DBL_QUOTE_XESCAPE);
			break;
		default:
			// Escape control characters.
			if (((c < 0x20) && (c != 0x09) && (c != 0x0a)) ||
				    (c == 0x7f)) {
				snprintf(o, 7, "&#x%02x;", c);
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

#define DBL_QUOTE_JESCAPE "\\\""
#define BACKSLASH_JESCAPE "\\\\"
#define SLASH_JESCAPE "\\/"
#define TAB_JESCAPE "\\t"
#define NEWLINE_JESCAPE "\\n"

int escape_json_attr_len(const char *buf, int src_len)
{
	const char *b;
	int ret = 0;
	int i;
	for (i = 0, b = buf; i < src_len; ++i, ++b) {
		unsigned char c = *b;
		switch (c) {
		case '"':
			ret += SSTRL(DBL_QUOTE_JESCAPE);
			break;
		case '\\':
			ret += SSTRL(BACKSLASH_JESCAPE);
			break;
		case '/':
			ret += SSTRL(SLASH_JESCAPE);
			break;
		case '\t':
			ret += SSTRL(TAB_JESCAPE);
			break;
		case '\n':
			ret += SSTRL(NEWLINE_JESCAPE);
			break;
		default:
			// Escape control characters.
			if ((c < 0x20) || (c == 0x7f)) {
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

void escape_json_attr(const char *buf, int src_len, char *out)
{
	char *o = out;
	const char *b;
	int i;
	for (i = 0, b = buf; i < src_len; ++i, ++b) {
		unsigned char c = *b;
		switch (c) {
		case '"':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, DBL_QUOTE_JESCAPE, SSTRL(DBL_QUOTE_JESCAPE));
			o += SSTRL(DBL_QUOTE_JESCAPE);
			break;
		case '\\':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, BACKSLASH_JESCAPE, SSTRL(BACKSLASH_JESCAPE));
			o += SSTRL(BACKSLASH_JESCAPE);
			break;
		case '/':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, SLASH_JESCAPE, SSTRL(SLASH_JESCAPE));
			o += SSTRL(SLASH_JESCAPE);
			break;
		case '\t':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, TAB_JESCAPE, SSTRL(TAB_JESCAPE));
			o += SSTRL(TAB_JESCAPE);
			break;
		case '\n':
			// cppcheck-suppress sizeofDivisionMemfunc
			memcpy(o, NEWLINE_JESCAPE, SSTRL(NEWLINE_JESCAPE));
			o += SSTRL(NEWLINE_JESCAPE);
			break;
		default:
			// Escape control characters.
			if ((c < 0x20) || (c == 0x7f)) {
				snprintf(o, 7, "\\u%04x", c);
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

