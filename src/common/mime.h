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

#ifndef CEPH_COMMON_MIME_H
#define CEPH_COMMON_MIME_H

#ifdef __cplusplus
extern "C" {
#endif

/* Encode a buffer as quoted-printable.
 *
 * Returns the length of the buffer we would need to do the encoding.
 * If we don't have enough buffer space, the output will be truncated.
 *
 * You may call mime_encode_as_qp(input, NULL, 0) to find the size of the
 * buffer you will need.
 */
signed int mime_encode_as_qp(const char *input, char *output, int outlen);

/* Decode a quoted-printable buffer.
 *
 * Returns a negative error code if the input is not a valid quoted-printable
 * buffer.
 * Returns the length of the buffer we would need to do the encoding.
 * If we don't have enough buffer space, the output will be truncated.
 *
 * You may call mime_encode_as_qp(input, NULL, 0) to find the size of the
 * buffer you will need.
 */
signed int mime_decode_from_qp(const char *input, char *output, int outlen);

#ifdef __cplusplus
}
#endif

#endif
