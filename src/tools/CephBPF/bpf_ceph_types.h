/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Dongdong Tao <dongdong.tao@canonical.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef BPF_CEPH_TYPES_H
#define BPF_CEPH_TYPES_H

/* ========================================================================
 * Constants
 * ======================================================================== */

#define MAX_ACTING_SIZE 11      /* Maximum number of OSDs in acting set */
#define MAX_OPS 3               /* Maximum number of OSD ops to capture */
#define MAX_OBJECT_NAME_LEN 128 /* Maximum object name length */
#define MAX_CLS_NAME_LEN 8      /* Maximum class name length */
#define MAX_METHOD_NAME_LEN 32  /* Maximum method name length */

/* Variable ID base for finish_op probe (send_op uses 0-based) */
#define FINISH_OP_VARID_BASE 20

/* ========================================================================
 * BPF-Specific Structures for Tracing
 * ======================================================================== */

typedef struct cls_op {
  char cls_name[MAX_CLS_NAME_LEN];
  char method_name[MAX_METHOD_NAME_LEN];
} cls_op_t;

struct client_op_k {
  __u64 cid;
  __u64 tid;
};

struct client_op_v {
  __u64 cid;
  __u64 tid;
  __u16 rw;
  __u64 sent_stamp;
  __u64 finish_stamp;
  __u32 target_osd;
  __u32 pid;
  char object_name[MAX_OBJECT_NAME_LEN];
  __u64 m_pool;
  __u32 m_seed;
  int acting[MAX_ACTING_SIZE];
  __u64 offset;
  __u64 length;
  __u16 ops[MAX_OPS];
  __u32 ops_size;
  cls_op_t cls_ops[MAX_OPS];
};

typedef struct VarLocation {
  int reg;
  int offset;
  bool stack;
#ifndef BPF_KERNEL_SPACE
  VarLocation() {
    reg = 0;
    offset = 0;
    stack = false;
  }
#endif
} VarLocation;

struct Field {
  int offset;
  bool pointer;
};

#ifdef BPF_KERNEL_SPACE
struct VarField {
  VarLocation varloc;
  struct Field fields[10];
  int size;
};
#else
struct VarField {
  VarLocation varloc;
  std::vector<Field> fields;
};

struct VarField_Kernel {
  VarLocation varloc;
  struct Field fields[10];
  int size;
};
#endif

/*
 * Include exported Ceph types only in BPF kernel space.
 * In userspace, the real Ceph headers provide these definitions.
 */
#ifdef BPF_KERNEL_SPACE
#include "bpf_ceph_exported.h"
#endif

#endif /* BPF_CEPH_TYPE_H */
