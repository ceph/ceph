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

#ifndef CEPH_COMMON_UTF8_H
#define CEPH_COMMON_UTF8_H

#define MAX_UTF8_SZ 6
#define INVALID_UTF8_CHAR 0xfffffffful

#ifdef __cplusplus
extern "C" {
#endif

/* Checks if a buffer is valid UTF-8.
 * Returns 0 if it is, and one plus the offset of the first invalid byte
 * if it is not.
 */
int check_utf8(const char *buf, int len);

/* Checks if a null-terminated string is valid UTF-8.
 * Returns 0 if it is, and one plus the offset of the first invalid byte
 * if it is not.
 */
int check_utf8_cstr(const char *buf);

/* Returns true if 'ch' is a control character.
 * We do count newline as a control character, but not NULL.
 */
int is_control_character(int ch);

/* Checks if a buffer contains control characters.
 */
int check_for_control_characters(const char *buf, int len);

/* Checks if a null-terminated string contains control characters.
 */
int check_for_control_characters_cstr(const char *buf);

/* Encode a 31-bit UTF8 code point to 'buf'.
 * Assumes buf is of size MAX_UTF8_SZ
 * Returns -1 on failure; number of bytes in the encoded value otherwise.
 */
int encode_utf8(unsigned long u, unsigned char *buf);

/*
 * Decode a UTF8 character from an array of bytes. Return character code.
 * Upon error, return INVALID_UTF8_CHAR.
 */
unsigned long decode_utf8(unsigned char *buf, int nbytes);

#ifdef __cplusplus
}
#endif

#endif
