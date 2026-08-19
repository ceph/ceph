// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MON_TYPES_H
#define CEPH_MON_TYPES_H

// use as paxos_service index
enum {
  PAXOS_MDSMAP,
  PAXOS_OSDMAP,
  PAXOS_LOG,
  PAXOS_MONMAP,
  PAXOS_AUTH,
  PAXOS_MGR,
  PAXOS_MGRSTAT,
  PAXOS_HEALTH,
  PAXOS_CONFIG,
  PAXOS_KV,
  PAXOS_NVMEGW,
  PAXOS_NUM
};

#define CEPH_MON_ONDISK_MAGIC "ceph mon volume v012"

#endif
