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

#include "msg/Message.h"
#include "messages/PaxosServiceMessage.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"
#include "messages/MBackfillReserve.h"
#include "messages/MCacheExpire.h"
#include "messages/MClientCapRelease.h"
#include "messages/MClientCaps.h"
#include "messages/MClientLease.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientReply.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientSession.h"
#include "messages/MClientSnap.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MDentryLink.h"
#include "messages/MDentryUnlink.h"
#include "messages/MDirUpdate.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"
#include "messages/MExportCaps.h"
#include "messages/MExportCapsAck.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirFinish.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MForward.h"
#include "messages/MGenericMessage.h"
#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MHeartbeat.h"
#include "messages/MInodeFileCaps.h"
#include "messages/MLock.h"
#include "messages/MLog.h"
#include "messages/MLogAck.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSCacheRejoin.h"
#include "messages/MMDSFindIno.h"
#include "messages/MMDSFindInoReply.h"
#include "messages/MMDSFragmentNotify.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMDSMap.h"
#include "messages/MMDSOpenIno.h"
#include "messages/MMDSOpenInoReply.h"
#include "messages/MMDSResolve.h"
#include "messages/MMDSResolveAck.h"
#include "messages/MMDSSlaveRequest.h"
#include "messages/MMDSTableRequest.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonElection.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MMonGlobalID.h"
#include "messages/MMonHealth.h"
#include "messages/MMonJoin.h"
#include "messages/MMonMap.h"
#include "messages/MMonPaxos.h"
#include "messages/MMonProbe.h"
#include "messages/MMonScrub.h"
#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"
#include "messages/MMonSync.h"
#include "messages/MOSDAlive.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGMissing.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDScrub.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"
#include "messages/MPing.h"
#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"
#include "messages/MRecoveryReserve.h"
#include "messages/MRemoveSnaps.h"
#include "messages/MRoute.h"
#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"
#include "messages/MTimeCheck.h"
#include "messages/MWatchNotify.h"

#define dout_subsys ceph_subsys_ms

#include "solaris_errno.h"
#include "solaris_message_errno.h"

// converts from linux errno values to host values
__s32 translate_errno(CephContext *cct,__s32 r) 
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
        if (cct)
          lderr(cct) << "Couldn't convert return code:" << r << " to host equivalent"<< dendl;
        break;
      }
    }
  } 
  return r; // otherwise return original value
}


// tanslate errno's from linux values to Solaris platform equivalents 
void translate_message_errno(CephContext *cct, Message *m)
{
  switch (m->get_header().type) {
    case CEPH_MSG_OSD_OPREPLY: {
      MOSDOpReply *t = (MOSDOpReply*)m;
      t->set_result(translate_errno(cct,t->get_result()));
      break;
    }
    case MSG_COMMAND_REPLY: {
      MCommandReply *t = (MCommandReply*)m;
      t->r = translate_errno(cct,t->r);
      break;
    }
    case CEPH_MSG_AUTH_REPLY: {
      MAuthReply *t = (MAuthReply*)m;
      t->result = translate_errno(cct,t->result);
      break;
    }
    case MSG_MON_COMMAND_ACK: {
      MMonCommandAck *t = (MMonCommandAck*)m;
      t->r = translate_errno(cct,t->r);   
      break;
    }
    case CEPH_MSG_WATCH_NOTIFY: {
      MWatchNotify *t = (MWatchNotify*)m;
      t->return_code = translate_errno(cct,t->return_code);
      break;
    }
    case CEPH_MSG_AUTH:
    case MSG_OSD_BACKFILL_RESERVE:
    case MSG_MDS_CACHEEXPIRE:
    case CEPH_MSG_CLIENT_CAPRELEASE:
    case CEPH_MSG_CLIENT_CAPS:
    case CEPH_MSG_CLIENT_LEASE:
    case CEPH_MSG_CLIENT_RECONNECT:
    case CEPH_MSG_CLIENT_REPLY:
    case CEPH_MSG_CLIENT_REQUEST:
    case CEPH_MSG_CLIENT_REQUEST_FORWARD:
    case CEPH_MSG_CLIENT_SESSION:
    case CEPH_MSG_CLIENT_SNAP:
    case MSG_MON_COMMAND:
    case MSG_MDS_DENTRYLINK:
    case MSG_MDS_DENTRYUNLINK:
    case MSG_MDS_DIRUPDATE:
    case MSG_MDS_DISCOVER:
    case MSG_MDS_DISCOVERREPLY:
    case MSG_MDS_EXPORTCAPS:
    case MSG_MDS_EXPORTCAPSACK:
    case MSG_MDS_EXPORTDIR:
    case MSG_MDS_EXPORTDIRACK:
    case MSG_MDS_EXPORTDIRCANCEL:
    case MSG_MDS_EXPORTDIRDISCOVER:
    case MSG_MDS_EXPORTDIRDISCOVERACK:
    case MSG_MDS_EXPORTDIRFINISH:
    case MSG_MDS_EXPORTDIRNOTIFY:
    case MSG_MDS_EXPORTDIRNOTIFYACK:
    case MSG_MDS_EXPORTDIRPREP:
    case MSG_MDS_EXPORTDIRPREPACK:
    case MSG_FORWARD:
    case MSG_GETPOOLSTATS:
    case MSG_GETPOOLSTATSREPLY:
    case MSG_MDS_HEARTBEAT:
    case MSG_MDS_INODEFILECAPS:
    case MSG_MDS_LOCK:
    case MSG_LOG:
    case MSG_LOGACK:
    case MSG_MDS_BEACON:
    case MSG_MDS_CACHEREJOIN:
    case MSG_MDS_FINDINO:
    case MSG_MDS_FINDINOREPLY:
    case MSG_MDS_FRAGMENTNOTIFY:
    case MSG_MDS_OFFLOAD_TARGETS:
    case CEPH_MSG_MDS_MAP:
    case MSG_MDS_OPENINO:
    case MSG_MDS_OPENINOREPLY:
    case MSG_MDS_RESOLVE:
    case MSG_MDS_RESOLVEACK:
    case MSG_MDS_SLAVE_REQUEST:
    case MSG_MDS_TABLE_REQUEST:
    case MSG_MON_ELECTION:
    case CEPH_MSG_MON_GET_MAP:
    case CEPH_MSG_MON_GET_VERSION:
    case CEPH_MSG_MON_GET_VERSION_REPLY:
    case MSG_MON_GLOBAL_ID:
    case MSG_MON_HEALTH:
    case MSG_MON_JOIN:
    case CEPH_MSG_MON_MAP:
    case MSG_MON_PAXOS:
    case MSG_MON_PROBE:
    case MSG_MON_SCRUB:
    case CEPH_MSG_MON_SUBSCRIBE:
    case CEPH_MSG_MON_SUBSCRIBE_ACK:
    case MSG_MON_SYNC:
    case MSG_OSD_ALIVE:
    case MSG_OSD_BOOT:
    case MSG_OSD_EC_READ:
    case MSG_OSD_EC_READ_REPLY:
    case MSG_OSD_EC_WRITE:
    case MSG_OSD_EC_WRITE_REPLY:
    case MSG_OSD_FAILURE:
    case CEPH_MSG_OSD_MAP:
    case MSG_OSD_MARK_ME_DOWN:
    case CEPH_MSG_OSD_OP:
    case MSG_OSD_PG_BACKFILL:
    case MSG_OSD_PG_CREATE:
    case MSG_OSD_PG_INFO:
    case MSG_OSD_PG_LOG:
    case MSG_OSD_PG_MISSING:
    case MSG_OSD_PG_NOTIFY:
    case MSG_OSD_PG_PULL:
    case MSG_OSD_PG_PUSH:
    case MSG_OSD_PG_PUSH_REPLY:
    case MSG_OSD_PG_QUERY:
    case MSG_OSD_PG_REMOVE:
    case MSG_OSD_PG_SCAN:
    case MSG_OSD_PGTEMP:
    case MSG_OSD_PG_TRIM:
    case MSG_OSD_PING:
    case MSG_OSD_REP_SCRUB:
    case MSG_OSD_SCRUB:
    case MSG_OSD_SUBOPREPLY:
    case MSG_PGSTATS:
    case MSG_PGSTATSACK:
    case CEPH_MSG_PING:
    case CEPH_MSG_POOLOP:
    case CEPH_MSG_POOLOP_REPLY:
    case MSG_OSD_RECOVERY_RESERVE:
    case MSG_REMOVE_SNAPS:
    case MSG_ROUTE:
    case CEPH_MSG_STATFS:
    case CEPH_MSG_STATFS_REPLY:
    case MSG_TIMECHECK:
      break;

    default: {
      if (cct) {
        lderr(cct) << "failed to decode message of type " << m->get_header().type 
                   << " v" << m->get_header().version << dendl;
        ldout(cct, 30) << "dump: \n";
        m->get_payload().hexdump(*_dout);
        *_dout << dendl;
        if (cct->_conf->ms_die_on_bad_msg)
          assert(0);
      }
      m->put();    
      break;
    }
  }

}
