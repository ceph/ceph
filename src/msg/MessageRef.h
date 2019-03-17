// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc. <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MESSAGEREF_H
#define CEPH_MESSAGEREF_H
 
#include <boost/intrusive_ptr.hpp>

template<typename T>
using MRef = boost::intrusive_ptr<T>;
template<typename T>
using MConstRef = boost::intrusive_ptr<T const>;

using MessageRef = MRef<class Message>;
using MessageConstRef = MConstRef<class Message>;

/* cd src/messages/ && for f in *; do printf 'class '; basename "$f" .h | tr -d '\n'; printf ';\n'; done >> ../msg/MessageRef.h */

class MAuth;
class MAuthReply;
class MBackfillReserve;
class MCacheExpire;
class MClientCapRelease;
class MClientCaps;
class MClientLease;
class MClientQuota;
class MClientReclaim;
class MClientReclaimReply;
class MClientReconnect;
class MClientReply;
class MClientRequestForward;
class MClientRequest;
class MClientSession;
class MClientSnap;
class MCommand;
class MCommandReply;
class MConfig;
class MDentryLink;
class MDentryUnlink;
class MDirUpdate;
class MDiscover;
class MDiscoverReply;
class MExportCapsAck;
class MExportCaps;
class MExportDirAck;
class MExportDirCancel;
class MExportDirDiscoverAck;
class MExportDirDiscover;
class MExportDirFinish;
class MExportDir;
class MExportDirNotifyAck;
class MExportDirNotify;
class MExportDirPrepAck;
class MExportDirPrep;
class MForward;
class MFSMap;
class MFSMapUser;
class MGatherCaps;
class MGenericMessage;
class MGetConfig;
class MGetPoolStats;
class MGetPoolStatsReply;
class MHeartbeat;
class MInodeFileCaps;
class MLock;
class MLogAck;
class MLog;
class MMDSBeacon;
class MMDSCacheRejoin;
class MMDSFindIno;
class MMDSFindInoReply;
class MMDSFragmentNotifyAck;
class MMDSFragmentNotify;
class MMDSLoadTargets;
class MMDSMap;
class MMDSOpenIno;
class MMDSOpenInoReply;
class MMDSResolveAck;
class MMDSResolve;
class MMDSSlaveRequest;
class MMDSSnapUpdate;
class MMDSTableRequest;
class MMgrBeacon;
class MMgrClose;
class MMgrConfigure;
class MMgrDigest;
class MMgrMap;
class MMgrOpen;
class MMgrReport;
class MMonCommandAck;
class MMonCommand;
class MMonElection;
class MMonGetMap;
class MMonGetOSDMap;
class MMonGetVersion;
class MMonGetVersionReply;
class MMonGlobalID;
class MMonHealthChecks;
class MMonHealth;
class MMonJoin;
class MMonMap;
class MMonMetadata;
class MMonMgrReport;
class MMonPaxos;
class MMonProbe;
class MMonQuorumService;
class MMonScrub;
class MMonSubscribeAck;
class MMonSubscribe;
class MMonSync;
class MNop;
class MOSDAlive;
class MOSDBackoff;
class MOSDBeacon;
class MOSDBoot;
class MOSDECSubOpRead;
class MOSDECSubOpReadReply;
class MOSDECSubOpWrite;
class MOSDECSubOpWriteReply;
class MOSDFailure;
class MOSDFastDispatchOp;
class MOSDForceRecovery;
class MOSDFull;
class MOSDMap;
class MOSDMarkMeDown;
class MOSDOp;
class MOSDOpReply;
class MOSDPeeringOp;
class MOSDPGBackfill;
class MOSDPGBackfillRemove;
class MOSDPGCreate2;
class MOSDPGCreated;
class MOSDPGCreate;
class MOSDPGInfo;
class MOSDPGLog;
class MOSDPGNotify;
class MOSDPGPull;
class MOSDPGPush;
class MOSDPGPushReply;
class MOSDPGQuery;
class MOSDPGReadyToMerge;
class MOSDPGRecoveryDelete;
class MOSDPGRecoveryDeleteReply;
class MOSDPGRemove;
class MOSDPGScan;
class MOSDPGTemp;
class MOSDPGTrim;
class MOSDPGUpdateLogMissing;
class MOSDPGUpdateLogMissingReply;
class MOSDPing;
class MOSDRepOp;
class MOSDRepOpReply;
class MOSDRepScrub;
class MOSDRepScrubMap;
class MOSDScrub2;
class MOSDScrub;
class MOSDScrubReserve;
class MPGStatsAck;
class MPGStats;
class MPing;
class MPoolOp;
class MPoolOpReply;
class MRecoveryReserve;
class MRemoveSnaps;
class MRoute;
class MServiceMap;
class MStatfs;
class MStatfsReply;
class MTimeCheck2;
class MTimeCheck;
class MWatchNotify;
class PaxosServiceMessage;

#endif
