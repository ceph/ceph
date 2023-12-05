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

#include <errno.h>
#include "include/types.h"
#include "include/compat.h"

static __u32 ceph_to_hostos_errno_unsigned(__u32 r)
{
  // using an array like like freebsd_errno.cc might be more readable but
  // we have some large values defined by Boost.
  switch(r) {
    case 1: return EPERM;
    case 2: return ENOENT;
    case 3: return ESRCH;
    case 4: return EINTR;
    case 5: return EIO;
    case 6: return ENXIO;
    case 7: return E2BIG;
    case 8: return ENOEXEC;
    case 9: return EBADF;
    case 10: return ECHILD;
    case 11: return EAGAIN;
    case 12: return ENOMEM;
    case 13: return EACCES;
    case 14: return EFAULT;
    case 15: return ENOTBLK;
    case 16: return EBUSY;
    case 17: return EEXIST;
    case 18: return EXDEV;
    case 19: return ENODEV;
    case 20: return ENOTDIR;
    case 21: return EISDIR;
    case 22: return EINVAL;
    case 23: return ENFILE;
    case 24: return EMFILE;
    case 25: return ENOTTY;
    case 26: return ETXTBSY;
    case 27: return EFBIG;
    case 28: return ENOSPC;
    case 29: return ESPIPE;
    case 30: return EROFS;
    case 31: return EMLINK;
    case 32: return EPIPE;
    case 33: return EDOM;
    case 34: return ERANGE;
    case 35: return EDEADLK;
    case 36: return ENAMETOOLONG;
    case 37: return ENOLCK;
    case 38: return ENOSYS;
    case 39: return ENOTEMPTY;
    case 40: return ELOOP;
    case 42: return ENOMSG;
    case 43: return EIDRM;

    case 60: return ENOSTR;
    case 61: return ENODATA;
    case 62: return ETIME;
    case 63: return ENOSR;

    case 66: return EREMOTE;
    case 67: return ENOLINK;

    case 71: return EPROTO;
    case 72: return EMULTIHOP;

    case 74: return EBADMSG;
    case 75: return EOVERFLOW;

    case 84: return EILSEQ;
    case 85: return ERESTART;

    case 87: return EUSERS;
    case 88: return ENOTSOCK;
    case 89: return EDESTADDRREQ;
    case 90: return EMSGSIZE;
    case 91: return EPROTOTYPE;
    case 92: return ENOPROTOOPT;
    case 93: return EPROTONOSUPPORT;
    case 94: return ESOCKTNOSUPPORT;
    case 95: return EOPNOTSUPP;
    case 96: return EPFNOSUPPORT;
    case 97: return EAFNOSUPPORT;
    case 98: return EADDRINUSE;
    case 99: return EADDRNOTAVAIL;
    case 100: return ENETDOWN;
    case 101: return ENETUNREACH;
    case 102: return ENETRESET;
    case 103: return ECONNABORTED;
    case 104: return ECONNRESET;
    case 105: return ENOBUFS;
    case 106: return EISCONN;
    case 107: return ENOTCONN;
    case 108: return ESHUTDOWN;
    case 109: return ETOOMANYREFS;
    case 110: return ETIMEDOUT;
    case 111: return ECONNREFUSED;
    case 112: return EHOSTDOWN;
    case 113: return EHOSTUNREACH;
    case 114: return EALREADY;
    case 115: return EINPROGRESS;
    case 116: return ESTALE;
    case 117: return EUCLEAN;

    case 121: return EREMOTEIO;
    case 122: return EDQUOT;

    case 125: return ECANCELED;

    case 129: return EKEYREJECTED;
    case 130: return EOWNERDEAD;
    case 131: return ENOTRECOVERABLE;


    case 44: return EPERM;        // linux: ECHRNG
    case 45: return EPERM;        // linux: EL2NSYNC
    case 46: return EPERM;        // linux: EL3HLT
    case 47: return EPERM;        // linux: EL3RST
    case 48: return EPERM;        // linux: ELNRNG
    case 49: return EPERM;        // linux: EUNATCH
    case 50: return EPERM;        // linux: ENOCSI
    case 51: return EPERM;        // linux: EL2HLT
    case 52: return EPERM;        // linux: EBADE
    case 53: return EPERM;        // linux: EBADR
    case 54: return EPERM;        // linux: EXFULL
    case 55: return EPERM;        // linux: ENOANO
    case 56: return EPERM;        // linux: EBADRQC
    case 57: return EPERM;        // linux: EBADSLT
    case 59: return EPERM;        // linux: EBFONT

    case 64: return ENETDOWN;     // linux: ENONET
    case 65: return ENOSYS;       // linux: ENOPKG

    case 68: return EPERM;        // linux: EADV
    case 69: return EPERM;        // linux: ESRMNT
    case 70: return EPERM;        // linux: ECOMM

    case 73: return EPERM;        // linux: EDOTDOT

    case 76: return EPERM;        // linux: ENOTUNIQ;
    case 77: return EPERM;        // linux: EBADFD;
    case 78: return EPERM;        // linux: EREMCHG;
    case 79: return EPERM;        // linux: ELIBACC;
    case 80: return EPERM;        // linux: ELIBBAD;
    case 81: return EPERM;        // linux: ELIBSCN;
    case 82: return EPERM;        // linux: ELIBMAX;
    case 83: return EPERM;        // linux: ELIBEXEC;

    case 86: return EPIPE;        // linux: ESTRPIPE;

    case 118: return EPERM;       // linux: ENOTNAM;
    case 119: return EPERM;       // linux: ENAVAIL;
    case 120: return EPERM;       // linux: EISNAM;

    case 123: return EPERM;       // linux: ENOMEDIUM;
    case 124: return EPERM;       // linux: EMEDIUMTYPE;

    case 126: return EPERM;       // linux: ENOKEY;
    case 127: return EPERM;       // linux: EKEYEXPIRED;
    case 128: return EPERM;       // linux: EKEYREVOKED;

    case 132: return EPERM;       // linux: ERFKILL;
    case 133: return EPERM;       // linux: EHWPOISON;
    default:
      return r;
  }
}

static __u32 hostos_to_ceph_errno_unsigned(__u32 r) {
  // Windows errno -> Linux errno
  switch(r) {
    case EPERM: return 1;
    case ENOENT: return 2;
    case ESRCH: return 3;
    case EINTR: return 4;
    case EIO: return 5;
    case ENXIO: return 6;
    case E2BIG: return 7;
    case ENOEXEC: return 8;
    case EBADF: return 9;
    case ECHILD: return 10;
    case EAGAIN: return 11;
    case ENOMEM: return 12;
    case EACCES: return 13;
    case EFAULT: return 14;
    case ENOTBLK: return 15;
    case EBUSY: return 16;
    case EEXIST: return 17;
    case EXDEV: return 18;
    case ENODEV: return 19;
    case ENOTDIR: return 20;
    case EISDIR: return 21;
    case EINVAL: return 22;
    case ENFILE: return 23;
    case EMFILE: return 24;
    case ENOTTY: return 25;
    case ETXTBSY: return 26;
    case EFBIG: return 27;
    case ENOSPC: return 28;
    case ESPIPE: return 29;
    case EROFS: return 30;
    case EMLINK: return 31;
    case EPIPE: return 32;
    case EDOM: return 33;
    case ERANGE: return 34;
    case EDEADLK: return 35;
    case ENAMETOOLONG: return 36;
    case ENOLCK: return 37;
    case ENOSYS: return 38;
    case ENOTEMPTY: return 39;
    case ELOOP: return 40;
    case ENOMSG: return 42;
    case EIDRM: return 43;

    case ENOSTR: return 60;
    case ENODATA: return 61;
    case ETIME: return 62;
    case ENOSR: return 63;

    case EREMOTE: return 66;
    case ENOLINK: return 67;

    case EPROTO: return 71;
    case EMULTIHOP: return 72;

    case EBADMSG: return 74;
    case EOVERFLOW: return 75;

    case EILSEQ: return 84;

    // compat.h defines ERESTART as EINTR
    // case ERESTART: return 85;

    case EUSERS: return 87;
    case ENOTSOCK: return 88;
    case EDESTADDRREQ: return 89;
    case EMSGSIZE: return 90;
    case EPROTOTYPE: return 91;
    case ENOPROTOOPT: return 92;
    case EPROTONOSUPPORT: return 93;
    case ESOCKTNOSUPPORT: return 94;
    case EOPNOTSUPP: return 95;
    case ENOTSUP: return 95;
    case EPFNOSUPPORT: return 96;
    case EAFNOSUPPORT: return 97;
    case EADDRINUSE: return 98;
    case EADDRNOTAVAIL: return 99;
    case ENETDOWN: return 100;
    case ENETUNREACH: return 101;
    case ENETRESET: return 102;
    case ECONNABORTED: return 103;
    case ECONNRESET: return 104;
    case ENOBUFS: return 105;
    case EISCONN: return 106;
    case ENOTCONN: return 107;
    case ESHUTDOWN: return 108;
    case ETOOMANYREFS: return 109;
    case ETIMEDOUT: return 110;
    case ECONNREFUSED: return 111;
    case EHOSTDOWN: return 112;
    case EHOSTUNREACH: return 113;
    case EALREADY: return 114;
    case EINPROGRESS: return 115;
    case ESTALE: return 116;
    case EUCLEAN: return 117;

    case EREMOTEIO: return 121;
    case EDQUOT: return 122;

    case ECANCELED: return 125;

    case EKEYREJECTED: return 129;
    case EOWNERDEAD: return 130;
    case ENOTRECOVERABLE: return 131;

    default:
      return r;
 }
}

// converts from linux errno values to host values
__s32 ceph_to_hostos_errno(__s32 r)
{
  int sign = (r < 0 ? -1 : 1);
  return ceph_to_hostos_errno_unsigned(abs(r)) * sign;
}

__s32 hostos_to_ceph_errno(__s32 r)
{
  int sign = (r < 0 ? -1 : 1);
  return hostos_to_ceph_errno_unsigned(abs(r)) * sign;
}
