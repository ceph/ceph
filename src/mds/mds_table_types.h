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

#ifndef CEPH_MDSTABLETYPES_H
#define CEPH_MDSTABLETYPES_H

// MDS TABLES

enum {
  TABLE_ANCHOR,
  TABLE_SNAP,
};

inline const char *get_mdstable_name(int t) {
  switch (t) {
  case TABLE_ANCHOR: return "anchortable";
  case TABLE_SNAP: return "snaptable";
  default: assert(0);
  }
}

enum {
  TABLESERVER_OP_QUERY        =  1,
  TABLESERVER_OP_QUERY_REPLY  = -2,
  TABLESERVER_OP_PREPARE      =  3,
  TABLESERVER_OP_AGREE        = -4,
  TABLESERVER_OP_COMMIT       =  5,
  TABLESERVER_OP_ACK          = -6,
  TABLESERVER_OP_ROLLBACK     =  7,
  TABLESERVER_OP_SERVER_UPDATE = 8,
  TABLESERVER_OP_SERVER_READY = -9,
};

inline const char *get_mdstableserver_opname(int op) {
  switch (op) {
  case TABLESERVER_OP_QUERY: return "query";
  case TABLESERVER_OP_QUERY_REPLY: return "query_reply";
  case TABLESERVER_OP_PREPARE: return "prepare";
  case TABLESERVER_OP_AGREE: return "agree";
  case TABLESERVER_OP_COMMIT: return "commit";
  case TABLESERVER_OP_ACK: return "ack";
  case TABLESERVER_OP_ROLLBACK: return "rollback";
  case TABLESERVER_OP_SERVER_UPDATE: return "server_update";
  case TABLESERVER_OP_SERVER_READY: return "server_ready";
  default: assert(0); return 0;
  }
}

enum {
  TABLE_OP_CREATE,
  TABLE_OP_UPDATE,
  TABLE_OP_DESTROY,
};

inline const char *get_mdstable_opname(int op) {
  switch (op) {
  case TABLE_OP_CREATE: return "create";
  case TABLE_OP_UPDATE: return "update";
  case TABLE_OP_DESTROY: return "destroy";
  default: assert(0); return 0;
  }
}

#endif
