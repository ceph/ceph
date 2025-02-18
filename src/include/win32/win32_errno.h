// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

// We're going to preserve the error numbers defined by the Windows SDK but not
// by Mingw headers. For others, we're going to use numbers greater than 256 to
// avoid unintended overlaps.

#ifndef WIN32_ERRNO_H
#define WIN32_ERRNO_H 1

#include <errno.h>

#include "include/int_types.h"

#ifndef EBADMSG
#define EBADMSG 104
#endif

#ifndef ENODATA
#define ENODATA 120
#endif

#ifndef ENOLINK
#define ENOLINK 121
#endif

#ifndef ENOMSG
#define ENOMSG 122
#endif

#ifndef ENOTRECOVERABLE
#define ENOTRECOVERABLE 127
#endif

#ifndef ETIME
#define ETIME 137
#endif

#ifndef ETXTBSY
#define ETXTBSY 139
#endif

#ifndef ENODATA
#define ENODATA 120
#endif

#define ESTALE 256
#define EREMOTEIO 257

#ifndef EBADE
#define EBADE 258
#endif

#define EUCLEAN 259
#define EREMCHG 260
#define EKEYREJECTED 261
#define EREMOTE 262

// Not used at moment. Full coverage ensures that remote errors will be
// converted and handled properly.
#define EADV 263
#define EBADFD 264
#define EBADR 265
#define EBADRQC 266
#define EBADSLT 267
#define EBFONT 268
#define ECHRNG 269
#define ECOMM 270
#define EDOTDOT 271
#define EHOSTDOWN 272
#define EHWPOISON 273
// Defined by Boost.
#ifndef EIDRM
#define EIDRM 274
#endif
#define EISNAM 275
#define EKEYEXPIRED 276
#define EKEYREVOKED 277
#define EL2HLT 278
#define EL2NSYNC 279
#define EL3HLT 280
#define EL3RST 281
#define ELIBACC 282
#define ELIBBAD 283
#define ELIBEXEC 284
#define ELIBMAX 285
#define ELIBSCN 286
#define ELNRNG 287
#define EMEDIUMTYPE 288
#define EMULTIHOP 289
#define ENAVAIL 290
#define ENOANO 291
#define ENOCSI 292
#define ENOKEY 293
#define ENOMEDIUM 294
#define ENONET 295
#define ENOPKG 296
#ifndef ENOSR
#define ENOSR 297
#endif
#ifndef ENOSTR
#define ENOSTR 298
#endif
#define ENOTNAM 299
#define ENOTUNIQ 300
#define EPFNOSUPPORT 301
#define ERFKILL 302
#define ESOCKTNOSUPPORT 303
#define ESRMNT 304
#define ESTRPIPE 305
#define ETOOMANYREFS 306
#define EUNATCH 307
#define EUSERS 308
#define EXFULL 309
#define ENOTBLK 310

#ifndef EDQUOT
#define EDQUOT 311
#endif

#define ESHUTDOWN 312

#ifdef __cplusplus
extern "C" {
#endif

__s32 wsae_to_errno(__s32 r);
__u32 errno_to_ntstatus(__s32 r);

#ifdef __cplusplus
}
#endif

#endif // WIN32_ERRNO_H
