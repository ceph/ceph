// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef __MON_TYPES_H
#define __MON_TYPES_H

#define PAXOS_TEST       0
#define PAXOS_MDSMAP     1
#define PAXOS_OSDMAP     2
#define PAXOS_CLIENTMAP  3

inline const char *get_paxos_name(int p) {
  switch (p) {
  case PAXOS_TEST: return "test";
  case PAXOS_MDSMAP: return "mdsmap";
  case PAXOS_OSDMAP: return "osdmap";
  case PAXOS_CLIENTMAP: return "clientmap";
  default: assert(0); return 0;
  }
}

#endif
