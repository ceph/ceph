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


#include <errno.h>
#include <stdlib.h>

#include "include/int_types.h"
#include <ntdef.h>
#include <ntstatus.h>

#include "include/compat.h"
#include "include/int_types.h"
#include "include/types.h"
#include "include/fs_types.h"

#include <unordered_map>

// We're only converting errors defined in errno.h, not standard Windows
// system error codes that are usually retrievied using GetLastErrorCode().
// TODO: consider converting WinSock2 (WSA*) error codes, which are quite
// similar to the errno.h ones.

__u32 ceph_to_hostos_errno_unsigned(__u32 r)
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
    // same as EWOULDBLOCK
    // need to be careful of treating EWOULDBLOCK same way as EAGAIN
    // in errno.h (in Windows) the EWOULDBLOCK is set to 140, while EAGAIN is 11
    // In the cephfs client tests we changed assertions that expected EWOULDBLOCK to EAGAIN,
    // since MDS returns EAGAIN for all cases. EWOULBLOCK originally was used with socket context,
    // so semantically it makes sense to use EAGAIN for operations that indicate some possible waiting/retry
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
    // same as EDEADLK
    case 35: return EDEADLOCK;
    case 36: return ENAMETOOLONG;
    case 37: return ENOLCK;
    case 38: return ENOSYS;
    case 39: return ENOTEMPTY;
    case 40: return ELOOP;
    case 42: return ENOMSG;
    case 43: return EIDRM;
    case 44: return ECHRNG;
    case 45: return EL2NSYNC;
    case 46: return EL3HLT;
    case 47: return EL3RST;
    case 48: return ELNRNG;
    case 49: return EUNATCH;
    case 50: return ENOCSI;
    case 51: return EL2HLT;
    case 52: return EBADE;
    case 53: return EBADR;
    case 54: return EXFULL;
    case 55: return ENOANO;
    case 56: return EBADRQC;
    case 57: return EBADSLT;
    case 59: return EBFONT;
    case 60: return ENOSTR;
    case 61: return ENODATA;
    case 62: return ETIME;
    case 63: return ENOSR;
    case 64: return ENONET;
    case 65: return ENOPKG;
    case 66: return EREMOTE;
    case 67: return ENOLINK;
    case 68: return EADV;
    case 69: return ESRMNT;
    case 70: return ECOMM;
    case 71: return EPROTO;
    case 72: return EMULTIHOP;
    case 73: return EDOTDOT;
    case 74: return EBADMSG;
    case 75: return EOVERFLOW;
    case 76: return ENOTUNIQ;
    case 77: return EBADFD;
    case 78: return EREMCHG;
    case 79: return ELIBACC;
    case 80: return ELIBBAD;
    case 81: return ELIBSCN;
    case 82: return ELIBMAX;
    case 83: return ELIBEXEC;
    case 84: return EILSEQ;
    case 85: return ERESTART;
    case 86: return ESTRPIPE;
    case 87: return EUSERS;
    case 88: return ENOTSOCK;
    case 89: return EDESTADDRREQ;
    case 90: return EMSGSIZE;
    case 91: return EPROTOTYPE;
    case 92: return ENOPROTOOPT;
    case 93: return EPROTONOSUPPORT;
    case 94: return ESOCKTNOSUPPORT;
    // same as ENOTSUP
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
    case 118: return ENOTNAM;
    case 119: return ENAVAIL;
    case 120: return EISNAM;
    case 121: return EREMOTEIO;
    case 122: return EDQUOT;
    case 123: return ENOMEDIUM;
    case 124: return EMEDIUMTYPE;
    case 125: return ECANCELED;
    case 126: return ENOKEY;
    case 127: return EKEYEXPIRED;
    case 128: return EKEYREVOKED;
    case 129: return EKEYREJECTED;
    case 130: return EOWNERDEAD;
    case 131: return ENOTRECOVERABLE;
    case 132: return ERFKILL;
    case 133: return EHWPOISON;
    default:
      return r;
  }
}

__u32 hostos_to_ceph_errno_unsigned(__u32 r) {
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
    case EWOULDBLOCK: return 11;
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
    // same as EDEADLOCK
    // case EDEADLK: return 35;
    case EDEADLOCK: return 35;
    case ENAMETOOLONG: return 36;
    case ENOLCK: return 37;
    case ENOSYS: return 38;
    case ENOTEMPTY: return 39;
    case ELOOP: return 40;
    case ENOMSG: return 42;
    case EIDRM: return 43;
    case ECHRNG: return 44;
    case EL2NSYNC: return 45;
    case EL3HLT: return 46;
    case EL3RST: return 47;
    case ELNRNG: return 48;
    case EUNATCH: return 49;
    case ENOCSI: return 50;
    case EL2HLT: return 51;
    case EBADE: return 52;
    case EBADR: return 53;
    case EXFULL: return 54;
    case ENOANO: return 55;
    case EBADRQC: return 56;
    case EBADSLT: return 57;
    case EBFONT: return 59;
    case ENOSTR: return 60;
    case ENODATA: return 61;
    case ETIME: return 62;
    case ENOSR: return 63;
    case ENONET: return 64;
    case ENOPKG: return 65;
    case EREMOTE: return 66;
    case ENOLINK: return 67;
    case EADV: return 68;
    case ESRMNT: return 69;
    case ECOMM: return 70;
    case EPROTO: return 71;
    case EMULTIHOP: return 72;
    case EDOTDOT: return 73;
    case EBADMSG: return 74;
    case EOVERFLOW: return 75;
    case ENOTUNIQ: return 76;
    case EBADFD: return 77;
    case EREMCHG: return 78;
    case ELIBACC: return 79;
    case ELIBBAD: return 80;
    case ELIBSCN: return 81;
    case ELIBMAX: return 82;
    case ELIBEXEC: return 83;
    case EILSEQ: return 84;
    // compat.h defines ERESTART as EINTR
    // case ERESTART: return 85;
    case ESTRPIPE: return 86;
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
    case ENOTNAM: return 118;
    case ENAVAIL: return 119;
    case EISNAM: return 120;
    case EREMOTEIO: return 121;
    case EDQUOT: return 122;
    case ENOMEDIUM: return 123;
    case EMEDIUMTYPE: return 124;
    case ECANCELED: return 125;
    case ENOKEY: return 126;
    case EKEYEXPIRED: return 127;
    case EKEYREVOKED: return 128;
    case EKEYREJECTED: return 129;
    case EOWNERDEAD: return 130;
    case ENOTRECOVERABLE: return 131;
    case ERFKILL: return 132;
    case EHWPOISON: return 133;
    default:
      return r;
 }
}

__s32 wsae_to_errno_unsigned(__s32 r)
{
  switch(r) {
    case WSAEINTR: return EINTR;
    case WSAEBADF: return EBADF;
    case WSAEACCES: return EACCES;
    case WSAEFAULT: return EFAULT;
    case WSAEINVAL: return EINVAL;
    case WSAEMFILE: return EMFILE;
    // Linux defines WSAEWOULDBLOCK as EAGAIN, but not Windows headers.
    // Since all ceph code uses EAGAIN instead of EWOULDBLOCK, we'll do
    // the same here.
    case WSAEWOULDBLOCK: return EAGAIN;
    // Some functions (e.g. connect) can return WSAEWOULDBLOCK instead of
    // EINPROGRESS.
    case WSAEINPROGRESS: return EINPROGRESS;
    case WSAEALREADY: return EALREADY;
    case WSAENOTSOCK: return ENOTSOCK;
    case WSAEDESTADDRREQ: return EDESTADDRREQ;
    case WSAEMSGSIZE: return EMSGSIZE;
    case WSAEPROTOTYPE: return EPROTOTYPE;
    case WSAENOPROTOOPT: return ENOPROTOOPT;
    case WSAEPROTONOSUPPORT: return EPROTONOSUPPORT;
    case WSAESOCKTNOSUPPORT: return ESOCKTNOSUPPORT;
    case WSAEOPNOTSUPP: return EOPNOTSUPP;
    case WSAEPFNOSUPPORT: return EPFNOSUPPORT;
    case WSAEAFNOSUPPORT: return EAFNOSUPPORT;
    case WSAEADDRINUSE: return EADDRINUSE;
    case WSAEADDRNOTAVAIL: return EADDRNOTAVAIL;
    case WSAENETDOWN: return ENETDOWN;
    case WSAENETUNREACH: return ENETUNREACH;
    case WSAENETRESET: return ENETRESET;
    case WSAECONNABORTED: return ECONNABORTED;
    case WSAECONNRESET: return ECONNRESET;
    case WSAENOBUFS: return ENOBUFS;
    case WSAEISCONN: return EISCONN;
    case WSAENOTCONN: return ENOTCONN;
    case WSAESHUTDOWN: return ESHUTDOWN;
    case WSAETOOMANYREFS: return ETOOMANYREFS;
    case WSAETIMEDOUT: return ETIMEDOUT;
    case WSAECONNREFUSED: return ECONNREFUSED;
    case WSAELOOP: return ELOOP;
    case WSAENAMETOOLONG: return ENAMETOOLONG;
    case WSAEHOSTDOWN: return EHOSTDOWN;
    case WSAEHOSTUNREACH: return EHOSTUNREACH;
    case WSAENOTEMPTY: return ENOTEMPTY;
    // case WSAEPROCLIM
    case WSAEUSERS: return EUSERS;
    case WSAEDQUOT: return EDQUOT;
    case WSAESTALE: return ESTALE;
    case WSAEREMOTE: return EREMOTE;
    // case WSASYSNOTREADY
    // case WSAVERNOTSUPPORTED
    // case WSANOTINITIALISED
    case WSAEDISCON: return ESHUTDOWN;
    // case WSAENOMORE
    case WSAECANCELLED: return ECANCELED;
    // We might return EINVAL, but it's probably better if we propagate the
    // original error code here.
    // case WSAEINVALIDPROCTABLE
    // case WSAEINVALIDPROVIDER
    // case WSAEPROVIDERFAILEDINIT
    // case WSASYSCALLFAILURE
    // case WSASERVICE_NOT_FOUND:
    // case WSATYPE_NOT_FOUND:
    // case WSA_E_NO_MORE:
    case WSA_E_CANCELLED: return ECANCELED;
    case WSAEREFUSED: return ECONNREFUSED;
    case WSAHOST_NOT_FOUND: return EHOSTUNREACH;
    case WSATRY_AGAIN: return EAGAIN;
    // case WSANO_RECOVERY
    // case WSANO_DATA:
    default: return r;
  }
}

// converts from linux errno values to host values
__s32 ceph_to_hostos_errno(__s32 r)
{
  int sign = (r < 0 ? -1 : 1);
  return ceph_to_hostos_errno_unsigned(abs(r)) * sign;
}

// converts Host OS errno values to linux/Ceph values
__s32 hostos_to_ceph_errno(__s32 r)
{
  int sign = (r < 0 ? -1 : 1);
  return hostos_to_ceph_errno_unsigned(abs(r)) * sign;
}

__s32 wsae_to_errno(__s32 r)
{
  int sign = (r < 0 ? -1 : 1);
  return wsae_to_errno_unsigned(abs(r)) * sign;
}

__u32 errno_to_ntstatus(__s32 r) {
  // errno -> NTSTATUS
  // In some cases, there might be more than one applicable NTSTATUS
  // value or there might be none. Certain values can be overridden
  // when the caller (or whoever is supposed to handle the error) is
  // expecting a different NTSTATUS value.
  r = abs(r);

  switch(r) {
    case 0: return 0;
    case EPERM: return STATUS_ACCESS_DENIED;
    case ENOENT: return STATUS_OBJECT_NAME_NOT_FOUND;
    case ESRCH: return STATUS_NOT_FOUND;
    case EINTR: return STATUS_RETRY;
    case EIO: return STATUS_DATA_ERROR;
    case ENXIO: return STATUS_NOT_FOUND;
    case E2BIG: return STATUS_FILE_TOO_LARGE;
    case ENOEXEC: return STATUS_ACCESS_DENIED;
    case EBADF: return STATUS_INVALID_HANDLE;
    case ECHILD: return STATUS_INTERNAL_ERROR;
    case EAGAIN: return STATUS_RETRY;
    case EWOULDBLOCK: return STATUS_RETRY;
    case ENOMEM: return STATUS_NO_MEMORY;
    case EACCES: return STATUS_ACCESS_DENIED;
    case EFAULT: return STATUS_INVALID_ADDRESS;
    case ENOTBLK: return STATUS_BAD_DEVICE_TYPE;
    case EBUSY: return STATUS_DEVICE_BUSY;
    case EEXIST: return STATUS_OBJECT_NAME_COLLISION;
    case EXDEV: return STATUS_NOT_SAME_DEVICE;
    case ENODEV: return STATUS_SYSTEM_DEVICE_NOT_FOUND;
    case ENOTDIR: return STATUS_NOT_A_DIRECTORY;
    case EISDIR: return STATUS_FILE_IS_A_DIRECTORY;
    case EINVAL: return STATUS_INVALID_PARAMETER;
    case ENFILE: return STATUS_TOO_MANY_OPENED_FILES;
    case EMFILE: return STATUS_TOO_MANY_OPENED_FILES;
    case ENOTTY: return STATUS_INVALID_PARAMETER;
    case ETXTBSY: return STATUS_DEVICE_BUSY;
    case EFBIG: return STATUS_FILE_TOO_LARGE;
    case ENOSPC: return STATUS_DISK_FULL;
    case ESPIPE: return STATUS_INVALID_PARAMETER;
    case EROFS: return STATUS_MEDIA_WRITE_PROTECTED;
    case EMLINK: return STATUS_TOO_MANY_LINKS;
    case EPIPE: return STATUS_PIPE_BROKEN;
    case EDOM: return STATUS_INVALID_PARAMETER;
    case ERANGE: return STATUS_INVALID_PARAMETER;
    // same as EDEADLOCK
    // case EDEADLK: return 35;
    case EDEADLOCK: return STATUS_POSSIBLE_DEADLOCK;
    case ENAMETOOLONG: return STATUS_NAME_TOO_LONG;
    case ENOLCK: return STATUS_NOT_LOCKED;
    case ENOSYS: return STATUS_NOT_IMPLEMENTED;
    case ENOTEMPTY: return STATUS_DIRECTORY_NOT_EMPTY;
    case ELOOP: return STATUS_TOO_MANY_LINKS;
    case ENOMSG: return STATUS_MESSAGE_NOT_FOUND;
    case EIDRM: return STATUS_INVALID_PARAMETER;
    case ECHRNG: return STATUS_INVALID_PARAMETER;
    case EL2NSYNC: return STATUS_INTERNAL_ERROR;
    case EL3HLT: return STATUS_INTERNAL_ERROR;
    case EL3RST: return STATUS_INTERNAL_ERROR;
    case ELNRNG: return STATUS_INTERNAL_ERROR;
    case EUNATCH: return STATUS_INTERNAL_ERROR;
    case ENOCSI: return STATUS_INTERNAL_ERROR;
    case EL2HLT: return STATUS_INTERNAL_ERROR;
    case EBADE: return STATUS_INTERNAL_ERROR;
    case EBADR: return STATUS_INVALID_HANDLE;
    case EXFULL: return STATUS_DISK_FULL;
    case ENOANO: return STATUS_INTERNAL_ERROR;
    case EBADRQC: return STATUS_INVALID_PARAMETER;
    case EBADSLT: return STATUS_INVALID_PARAMETER;
    case EBFONT: return STATUS_INVALID_PARAMETER;
    case ENOSTR: return STATUS_INVALID_PARAMETER;
    case ENODATA: return STATUS_NOT_FOUND;
    case ETIME: return STATUS_TIMEOUT;
    case ENOSR: return STATUS_INSUFFICIENT_RESOURCES;
    case ENONET: return STATUS_NETWORK_UNREACHABLE;
    case ENOPKG: return STATUS_NO_SUCH_PACKAGE;
    case EREMOTE: return STATUS_INVALID_PARAMETER;
    case ENOLINK: return STATUS_INTERNAL_ERROR;
    case EADV: return STATUS_INTERNAL_ERROR;
    case ESRMNT: return STATUS_INTERNAL_ERROR;
    case ECOMM: return STATUS_INTERNAL_ERROR;
    case EPROTO: return STATUS_PROTOCOL_NOT_SUPPORTED;
    case EMULTIHOP: return STATUS_INTERNAL_ERROR;
    case EDOTDOT: return STATUS_INTERNAL_ERROR;
    case EBADMSG: return STATUS_INVALID_PARAMETER;
    case EOVERFLOW: return STATUS_BUFFER_OVERFLOW;
    case ENOTUNIQ: return STATUS_DUPLICATE_NAME;
    case EBADFD: return STATUS_INVALID_HANDLE;
    case EREMCHG: return STATUS_FILE_RENAMED;
    case ELIBACC: return STATUS_DLL_NOT_FOUND;
    case ELIBBAD: return STATUS_BAD_DLL_ENTRYPOINT;
    case ELIBSCN: return STATUS_BAD_DLL_ENTRYPOINT;
    case ELIBMAX: return STATUS_TOO_MANY_OPENED_FILES;
    case ELIBEXEC: return STATUS_INVALID_PARAMETER;
    case EILSEQ: return STATUS_INVALID_PARAMETER;
    // compat.h defines ERESTART as EINTR
    // case ERESTART: return 85;
    case ESTRPIPE: return STATUS_RETRY;
    case EUSERS: return STATUS_TOO_MANY_SIDS;
    case ENOTSOCK: return STATUS_INVALID_HANDLE;
    case EDESTADDRREQ: return STATUS_INVALID_PARAMETER;
    case EMSGSIZE: return STATUS_BUFFER_OVERFLOW;
    case EPROTOTYPE: return STATUS_INVALID_PARAMETER;
    case ENOPROTOOPT: return STATUS_PROTOCOL_NOT_SUPPORTED;
    case EPROTONOSUPPORT: return STATUS_PROTOCOL_NOT_SUPPORTED;
    case ESOCKTNOSUPPORT: return STATUS_NOT_SUPPORTED;
    case EOPNOTSUPP: return STATUS_NOT_SUPPORTED;
    case ENOTSUP: return STATUS_NOT_SUPPORTED;
    case EPFNOSUPPORT: return STATUS_PROTOCOL_NOT_SUPPORTED;
    case EAFNOSUPPORT: return STATUS_NOT_SUPPORTED;
    case EADDRINUSE: return STATUS_ADDRESS_ALREADY_EXISTS;
    case EADDRNOTAVAIL: return STATUS_INVALID_ADDRESS;
    case ENETDOWN: return STATUS_NETWORK_UNREACHABLE;
    case ENETUNREACH: return STATUS_NETWORK_UNREACHABLE;
    case ENETRESET: return STATUS_CONNECTION_RESET;
    case ECONNABORTED: return STATUS_CONNECTION_ABORTED;
    case ECONNRESET: return STATUS_CONNECTION_DISCONNECTED;
    case ENOBUFS: return STATUS_BUFFER_TOO_SMALL;
    case EISCONN: return STATUS_CONNECTION_ACTIVE;
    case ENOTCONN: return STATUS_CONNECTION_DISCONNECTED;
    case ESHUTDOWN: return STATUS_SYSTEM_SHUTDOWN;
    case ETOOMANYREFS: return STATUS_TOO_MANY_LINKS;
    case ETIMEDOUT: return STATUS_TIMEOUT;
    case ECONNREFUSED: return STATUS_CONNECTION_REFUSED;
    case EHOSTDOWN: return STATUS_FILE_CLOSED;
    case EHOSTUNREACH: return STATUS_HOST_UNREACHABLE;
    case EALREADY: return STATUS_PENDING;
    case EINPROGRESS: return STATUS_PENDING;
    case ESTALE: return STATUS_INVALID_HANDLE;
    case EUCLEAN: return STATUS_INVALID_PARAMETER;
    case ENOTNAM: return STATUS_INVALID_PARAMETER;
    case ENAVAIL: return STATUS_INVALID_PARAMETER;
    case EISNAM: return STATUS_INVALID_PARAMETER;
    case EREMOTEIO: return STATUS_DATA_ERROR;
    case EDQUOT: return STATUS_QUOTA_EXCEEDED;
    case ENOMEDIUM: return STATUS_NO_MEDIA;
    case EMEDIUMTYPE: return STATUS_INVALID_PARAMETER;
    case ECANCELED: return STATUS_REQUEST_CANCELED;
    case ENOKEY: return STATUS_NO_USER_KEYS;
    case EKEYEXPIRED: return STATUS_SMARTCARD_CERT_EXPIRED;
    case EKEYREVOKED: return STATUS_IMAGE_CERT_REVOKED;
    case EKEYREJECTED: return STATUS_ACCESS_DENIED;
    case EOWNERDEAD: return STATUS_INTERNAL_ERROR;
    case ENOTRECOVERABLE: return STATUS_INTERNAL_ERROR;
    case ERFKILL: return STATUS_INTERNAL_ERROR;
    case EHWPOISON: return STATUS_INTERNAL_ERROR;
    default:
      return STATUS_INTERNAL_ERROR;
 }
}

std::string win32_strerror(int err)
{
  // As opposed to dlerror messages, this has to be freed.
  LPSTR msg = NULL;
  DWORD msg_len = ::FormatMessageA(
    FORMAT_MESSAGE_ALLOCATE_BUFFER |
    FORMAT_MESSAGE_FROM_SYSTEM |
    FORMAT_MESSAGE_IGNORE_INSERTS,
    NULL,
    err,
    0,
    (LPSTR) &msg,
    0,
    NULL);

  std::ostringstream msg_stream;
  msg_stream << "(" << err << ") ";
  if (!msg_len) {
    msg_stream << "Unknown error";
  }
  else {
    msg_stream << msg;
    ::LocalFree(msg);
  }
  return msg_stream.str();
}

std::string win32_lasterror_str()
{
  DWORD err = ::GetLastError();
  return win32_strerror(err);
}
