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

#define H2C_ERRNO(a,b) [a] = b
#define C2H_ERRNO(a,b) [a] = b

// Build a table with the FreeBSD error as index
// and the Linux error as value
// Use the fact that the arry is initialised per default on all 0's
// And we do not translate for 0's, but return the original value.
static const __s32 ceph_to_hostos_conv[256] = {
//       Linux errno  FreeBSD errno
       C2H_ERRNO(11,  EAGAIN),	
       C2H_ERRNO(35,  EDEADLK),	
       C2H_ERRNO(36,  ENAMETOOLONG),	
       C2H_ERRNO(37,  ENOLCK),	
       C2H_ERRNO(38,  ENOSYS),	
       C2H_ERRNO(39,  ENOTEMPTY),	
       C2H_ERRNO(40,  ELOOP),	
       C2H_ERRNO(42,  ENOMSG),	
       C2H_ERRNO(43,  EIDRM),	
       C2H_ERRNO(44,  EPERM),	 //TODO ECHRNG   /* Channel number out of range */
       C2H_ERRNO(45,  EPERM),	 //TODO EL2NSYNC /* Level 2 not synchronized */
       C2H_ERRNO(46,  EPERM),	 //TODO EL3HLT   /* Level 3 halted */
       C2H_ERRNO(47,  EPERM),	 //TODO EL3RST   /* Level 3 reset */
       C2H_ERRNO(48,  EPERM),	 //TODO ELNRNG   /* Link number out of range */
       C2H_ERRNO(49,  EPERM),	 //TODO EUNATCH  /* Protocol driver not attached */
       C2H_ERRNO(50,  EPERM),	 //TODO ENOCSI   /* No CSI structure available */
       C2H_ERRNO(51,  EPERM),	 //TODO EL2HLT   /* Level 2 halted */
       C2H_ERRNO(52,  EPERM),	 //TODO EBADE    /* Invalid exchange */
       C2H_ERRNO(53,  EPERM),	 //TODO EBADR    /* Invalid request descriptor */
       C2H_ERRNO(54,  EPERM),	 //TODO EXFULL   /* Exchange full */
       C2H_ERRNO(55,  EPERM),	 //TODO ENOANO   /* No anode */
       C2H_ERRNO(56,  EPERM),	 //TODO EBADRQC  /* Invalid request code */
       C2H_ERRNO(57,  EPERM),	 //TODO EBADSLT  /* Invalid slot */
       C2H_ERRNO(59,  EPERM),	 //TODO EBFONT   /* Bad font file format */
       C2H_ERRNO(60,  ENOSTR),	
       C2H_ERRNO(61,  ENODATA),	
       C2H_ERRNO(62,  ETIME),	
       C2H_ERRNO(63,  ENOSR),	
       C2H_ERRNO(64,  EPERM),	 //TODO ENONET
       C2H_ERRNO(65,  EPERM),	 //TODO ENOPKG
       C2H_ERRNO(66,  EREMOTE),	
       C2H_ERRNO(67,  ENOLINK),	
       C2H_ERRNO(68,  EPERM),	 //TODO EADV
       C2H_ERRNO(69,  EPERM),	 //TODO ESRMNT
       C2H_ERRNO(70,  EPERM),	 //TODO ECOMM
       C2H_ERRNO(71,  EPROTO),	
       C2H_ERRNO(72,  EMULTIHOP),	
       C2H_ERRNO(73,  EPERM),	 //TODO EDOTDOT
       C2H_ERRNO(74,  EBADMSG),	
       C2H_ERRNO(75,  EOVERFLOW),	
       C2H_ERRNO(76,  EPERM),	 //TODO ENOTUNIQ
       C2H_ERRNO(77,  EPERM),	 //TODO EBADFD
       C2H_ERRNO(78,  EPERM),	 //TODO EREMCHG
       C2H_ERRNO(79,  EPERM),	 //TODO ELIBACC
       C2H_ERRNO(80,  EPERM),	 //TODO ELIBBAD
       C2H_ERRNO(81,  EPERM),	 //TODO ELIBSCN
       C2H_ERRNO(82,  EPERM),	 //TODO ELIBMAX
       C2H_ERRNO(83,  EPERM),	 //TODO ELIBEXEC
       C2H_ERRNO(84,  EILSEQ),	
       C2H_ERRNO(85,  EINTR),	 /* not quite, since this is a syscll restart */
       C2H_ERRNO(86,  EPERM),	 //ESTRPIPE;
       C2H_ERRNO(87,  EUSERS),	
       C2H_ERRNO(88,  ENOTSOCK),	
       C2H_ERRNO(89,  EDESTADDRREQ),	
       C2H_ERRNO(90,  EMSGSIZE),	
       C2H_ERRNO(91,  EPROTOTYPE),	
       C2H_ERRNO(92,  ENOPROTOOPT),	
       C2H_ERRNO(93,  EPROTONOSUPPORT),	
       C2H_ERRNO(94,  ESOCKTNOSUPPORT),	
       C2H_ERRNO(95,  EOPNOTSUPP),	
       C2H_ERRNO(96,  EPFNOSUPPORT),	
       C2H_ERRNO(97,  EAFNOSUPPORT),	
       C2H_ERRNO(98,  EADDRINUSE),	
       C2H_ERRNO(99,  EADDRNOTAVAIL),	
       C2H_ERRNO(100, ENETDOWN),	
       C2H_ERRNO(101, ENETUNREACH),	
       C2H_ERRNO(102, ENETRESET),	
       C2H_ERRNO(103, ECONNABORTED),	
       C2H_ERRNO(104, ECONNRESET),	
       C2H_ERRNO(105, ENOBUFS),	
       C2H_ERRNO(106, EISCONN),	
       C2H_ERRNO(107, ENOTCONN),	
       C2H_ERRNO(108, ESHUTDOWN),	
       C2H_ERRNO(109, ETOOMANYREFS),	
       C2H_ERRNO(110, ETIMEDOUT),	
       C2H_ERRNO(111, ECONNREFUSED),	
       C2H_ERRNO(112, EHOSTDOWN),	
       C2H_ERRNO(113, EHOSTUNREACH),	
       C2H_ERRNO(114, EALREADY),	
       C2H_ERRNO(115, EINPROGRESS),	
       C2H_ERRNO(116, ESTALE),	
       C2H_ERRNO(117, EPERM),	 //TODO EUCLEAN
       C2H_ERRNO(118, EPERM),	 //TODO ENOTNAM
       C2H_ERRNO(119, EPERM),	 //TODO ENAVAIL
       C2H_ERRNO(120, EPERM),	 //TODO EISNAM
       C2H_ERRNO(121, EREMOTEIO),	
       C2H_ERRNO(122, EDQUOT),	
       C2H_ERRNO(123, EPERM),	 //TODO ENOMEDIUM
       C2H_ERRNO(124, EPERM),	 //TODO EMEDIUMTYPE - not used
       C2H_ERRNO(125, ECANCELED),	
       C2H_ERRNO(126, EPERM),	 //TODO ENOKEY
       C2H_ERRNO(127, EPERM),	 //TODO EKEYEXPIRED
       C2H_ERRNO(128, EPERM),	 //TODO EKEYREVOKED
       C2H_ERRNO(129, EPERM),	 //TODO EKEYREJECTED
       C2H_ERRNO(130, EOWNERDEAD),	
       C2H_ERRNO(131, ENOTRECOVERABLE),	
       C2H_ERRNO(132, EPERM),	 //TODO ERFKILL
       C2H_ERRNO(133, EPERM),	 //TODO EHWPOISON
    };

// Build a table with the FreeBSD error as index
// and the Linux error as value
// Use the fact that the arry is initialised per default on all 0's
// And we do not translate for 0's, but return the original value.
static const __s32 hostos_to_ceph_conv[256] = {
	//        FreeBSD errno Linux errno
	H2C_ERRNO(EDEADLK,	35),   	/* Resource deadlock avoided */
        H2C_ERRNO(EAGAIN,	11),   	/* Resource temporarily unavailable */
        H2C_ERRNO(EINPROGRESS,	115),	/* Operation now in progress */
        H2C_ERRNO(EALREADY,	114),	/* Operation already in progress */
        H2C_ERRNO(ENOTSOCK,	88),	/* Socket operation on non-socket */
        H2C_ERRNO(EDESTADDRREQ,	89),	/* Destination address required */
        H2C_ERRNO(EMSGSIZE,	90),	/* Message too long */
        H2C_ERRNO(EPROTOTYPE,	91),	/* Protocol wrong type for socket */
        H2C_ERRNO(ENOPROTOOPT,	92),	/* Protocol not available */
        H2C_ERRNO(EPROTONOSUPPORT, 93),	/* Protocol not supported */
        H2C_ERRNO(ESOCKTNOSUPPORT, 94),	/* Socket type not supported */
        H2C_ERRNO(EOPNOTSUPP,	95),	/* Operation not supported */
        H2C_ERRNO(EPFNOSUPPORT,	96),	/* Protocol family not supported */
        H2C_ERRNO(EAFNOSUPPORT,	97),	/* Address family not supported by protocol family */
        H2C_ERRNO(EADDRINUSE,	98),	/* Address already in use */
        H2C_ERRNO(EADDRNOTAVAIL, 99),	/* Can't assign requested address */
        H2C_ERRNO(ENETDOWN,	100),	/* Network is down */
        H2C_ERRNO(ENETUNREACH,	101),	/* Network is unreachable */
        H2C_ERRNO(ENETRESET,	102),	/* Network dropped connection on reset */
        H2C_ERRNO(ECONNABORTED,	103),	/* Software caused connection abort */
        H2C_ERRNO(ECONNRESET,	104),	/* Connection reset by peer */
        H2C_ERRNO(ENOBUFS,	105),	/* No buffer space available */
        H2C_ERRNO(EISCONN,	106),	/* Socket is already connected */
        H2C_ERRNO(ENOTCONN,	107),	/* Socket is not connected */
        H2C_ERRNO(ESHUTDOWN,	108),	/* Can't send after socket shutdown */
        H2C_ERRNO(ETOOMANYREFS,	109),	/* Too many references: can't splice */
        H2C_ERRNO(ETIMEDOUT,	110),	/* Operation timed out */
        H2C_ERRNO(ECONNREFUSED,	111),	/* Connection refused */
        H2C_ERRNO(ELOOP,	40),	/* Too many levels of symbolic links */
        H2C_ERRNO(ENAMETOOLONG,	36),	/* File name too long */
        H2C_ERRNO(EHOSTDOWN,	112),	/* Host is down */
        H2C_ERRNO(EHOSTUNREACH,	113),	/* No route to host */
        H2C_ERRNO(ENOTEMPTY,	39),	/* Directory not empty */
        H2C_ERRNO(EPROCLIM,	EPERM),	/* Too many processes */
        H2C_ERRNO(EUSERS,	87),	/* Too many users */
        H2C_ERRNO(EDQUOT,	122),	/* Disc quota exceeded */
        H2C_ERRNO(ESTALE,	116),	/* Stale NFS file handle */
        H2C_ERRNO(EREMOTE,	66),	/* Too many levels of remote in path */
        H2C_ERRNO(EBADRPC,	EPERM),	/* RPC struct is bad */
        H2C_ERRNO(ERPCMISMATCH,	EPERM),	/* RPC version wrong */
        H2C_ERRNO(EPROGUNAVAIL,	EPERM),	/* RPC prog. not avail */
        H2C_ERRNO(EPROGMISMATCH, EPERM),/* Program version wrong */
        H2C_ERRNO(EPROCUNAVAIL,	EPERM),	/* Bad procedure for program */
        H2C_ERRNO(ENOLCK,	EPERM),	/* No locks available */
        H2C_ERRNO(ENOSYS,	EPERM),	/* Function not implemented */
        H2C_ERRNO(EFTYPE,	EPERM),	/* Inappropriate file type or format */
        H2C_ERRNO(EAUTH,	EPERM),	/* Authentication error */
        H2C_ERRNO(ENEEDAUTH,	EPERM),	/* Need authenticator */
        H2C_ERRNO(EIDRM,	43),	/* Identifier removed */
        H2C_ERRNO(ENOMSG,	42),	/* No message of desired type */
        H2C_ERRNO(EOVERFLOW,	75),	/* Value too large to be stored in data type */
        H2C_ERRNO(ECANCELED,	125),	/* Operation canceled */
        H2C_ERRNO(EILSEQ,	84),	/* Illegal byte sequence */
        H2C_ERRNO(ENOATTR,	61),	/* Attribute not found */
        H2C_ERRNO(EDOOFUS,	EPERM),	/* Programming error */
        H2C_ERRNO(EBADMSG,	74),	/* Bad message */
        H2C_ERRNO(EMULTIHOP,	72),	/* Multihop attempted */
        H2C_ERRNO(ENOLINK,	67),	/* Link has been severed */
        H2C_ERRNO(EPROTO,	71),	/* Protocol error */
        H2C_ERRNO(ENOTCAPABLE,	EPERM),	/* Capabilities insufficient */
        H2C_ERRNO(ECAPMODE,	EPERM),	/* Not permitted in capability mode */
        H2C_ERRNO(ENOTRECOVERABLE, 131),/* State not recoverable */
        H2C_ERRNO(EOWNERDEAD,	130),	/* Previous owner died */
	};

// converts from linux errno values to host values
__s32 ceph_to_hostos_errno(__s32 r)
{
  int sign = (r < 0 ? -1 : 1);
  int err = std::abs(r);
  if (err < 256 && ceph_to_hostos_conv[err] !=0 ) {
    err = ceph_to_hostos_conv[err];
  }
  return err * sign;
}

// converts Host OS errno values to linux/Ceph values
__s32 hostos_to_ceph_errno(__s32 r)
{
  int sign = (r < 0 ? -1 : 1);
  int err = std::abs(r);
  if (err < 256 && hostos_to_ceph_conv[err] !=0 ) {
    err = hostos_to_ceph_conv[err];
  }
  return err * sign;
}
