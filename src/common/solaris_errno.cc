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


// converts from linux errno values to host values
__s32 ceph_to_host_errno(__s32 r) 
{
  if (r < -34) {
    switch (r) {
      case -35:
        return -EDEADLK;
      case -36:
        return -ENAMETOOLONG;
      case -37:
        return -ENOLCK;
      case -38:
        return -ENOSYS;
      case -39:
        return -ENOTEMPTY;
      case -40:
        return -ELOOP;
      case -42:
        return -ENOMSG;
      case -43:
        return -EIDRM;
      case -44:
        return -ECHRNG;
      case -45:
        return -EL2NSYNC;
      case -46:
        return -EL3HLT;
      case -47:
        return -EL3RST;
      case -48:
        return -ELNRNG;
      case -49:
        return -EUNATCH;
      case -50:
        return -ENOCSI;
      case -51:
        return -EL2HLT;
      case -52:
        return -EBADE;
      case -53:
        return -EBADR;
      case -54:
        return -EXFULL;
      case -55:
        return -ENOANO;
      case -56:
        return -EBADRQC;
      case -57:
        return -EBADSLT;
      case -59:
        return -EBFONT;
      case -60:
        return -ENOSTR;
      case -61:
        return -ENODATA;
      case -62:
        return -ETIME;
      case -63:
        return -ENOSR;
      //case -64:
      //  return -EPERM; //TODO ENONET
      //case -65:
      //  return -EPERM; //TODO ENOPKG
      //case -66:
      //  return -EREMOTE;
      //case -67:
      //  return -ENOLINK;
      //case -68:
      //  return -EPERM; //TODO EADV 
      //case -69:
      //  return -EPERM; //TODO ESRMNT 
      //case -70:
      //  return -EPERM; //TODO ECOMM
      case -71:
        return -EPROTO;
      case -72:
        return -EMULTIHOP;
      case -73:
        return -EPERM; //TODO EDOTDOT 
      case -74:
        return -EBADMSG;
      case -75:
        return -EOVERFLOW;
      case -76:
        return -ENOTUNIQ;
      case -77:
        return -EBADFD;
      case -78:
        return -EREMCHG;
      case -79:
        return -ELIBACC;
      case -80:
        return -ELIBBAD;
      case -81:
        return -ELIBSCN;
      case -82:
        return -ELIBMAX;
      case -83:
	return -ELIBEXEC;
      case -84:
        return -EILSEQ;
      case -85:
        return -ERESTART;
      case -86:
        return -ESTRPIPE; 
      case -87:
        return -EUSERS;
      case -88:
        return -ENOTSOCK;
      case -89:
        return -EDESTADDRREQ;
      case -90:
        return -EMSGSIZE;
      case -91:
        return -EPROTOTYPE;
      case -92:
        return -ENOPROTOOPT;
      case -93:
        return -EPROTONOSUPPORT;
      case -94:
        return -ESOCKTNOSUPPORT;
      case -95:
        return -EOPNOTSUPP;
      case -96:
        return -EPFNOSUPPORT;
      case -97:
        return -EAFNOSUPPORT;
      case -98:
        return -EADDRINUSE;
      case -99:
        return -EADDRNOTAVAIL;
      case -100:
        return -ENETDOWN;
      case -101:
        return -ENETUNREACH;
      case -102:
        return -ENETRESET;
      case -103:
        return -ECONNABORTED;
      case -104:
        return -ECONNRESET;
      case -105:
        return -ENOBUFS;
      case -106:
        return -EISCONN;
      case -107:
        return -ENOTCONN;
      case -108:
        return -ESHUTDOWN;
      case -109:
        return -ETOOMANYREFS;
      case -110:
        return -ETIMEDOUT;
      case -111:
        return -ECONNREFUSED;
      case -112:
        return -EHOSTDOWN;
      case -113:
        return -EHOSTUNREACH;
      case -114:
        return -EALREADY;
      case -115:
        return -EINPROGRESS;
      case -116:
        return -ESTALE;
      case -117:
        return -EPERM; //TODO EUCLEAN 
      case -118:
        return -EPERM; //TODO ENOTNAM
      case -119:
        return -EPERM; //TODO ENAVAIL
      case -120:
        return -EPERM; //TODO EISNAM
      case -121:
        return -EPERM; //TODO EREMOTEIO
      case -122:
        return -EDQUOT;
      case -123:
        return -EPERM; //TODO ENOMEDIUM
      case -124:
        return -EPERM; //TODO EMEDIUMTYPE - not used
      case -125:
        return -ECANCELED;
      case -126:
        return -EPERM; //TODO ENOKEY
      case -127:
        return -EPERM; //TODO EKEYEXPIRED
      case -128:
        return -EPERM; //TODO EKEYREVOKED
      case -129:
        return -EPERM; //TODO EKEYREJECTED
      case -130:
        return -EOWNERDEAD;
      case -131:
        return -ENOTRECOVERABLE;
      case -132:
        return -EPERM; //TODO ERFKILL
      case -133:
        return -EPERM; //TODO EHWPOISON

      default: { 
        break;
      }
    }
  } 
  return r; // otherwise return original value
}


