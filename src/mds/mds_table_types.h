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

#ifndef __MDSTABLETYPES_H
#define __MDSTABLETYPES_H

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
  TABLE_OP_QUERY        =  1,
  TABLE_OP_QUERY_REPLY  = -2,
  TABLE_OP_PREPARE      =  3,
  TABLE_OP_AGREE        = -4,
  TABLE_OP_COMMIT       =  5,
  TABLE_OP_ACK          = -6,
  TABLE_OP_ROLLBACK     =  7,
};

enum {
  TABLE_OP_CREATE,
  TABLE_OP_UPDATE,
  TABLE_OP_DESTROY,
};

inline const char *get_mdstable_opname(int op) {
  switch (op) {
  case TABLE_OP_QUERY: return "query";
  case TABLE_OP_QUERY_REPLY: return "query_reply";
  case TABLE_OP_PREPARE: return "prepare";
  case TABLE_OP_AGREE: return "agree";
  case TABLE_OP_COMMIT: return "commit";
  case TABLE_OP_ACK: return "ack";
  case TABLE_OP_ROLLBACK: return "rollback";
  default: assert(0); return 0;

  }
};

#endif
