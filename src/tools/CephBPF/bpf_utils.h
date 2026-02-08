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

#ifndef BPF_UTILS_H
#define BPF_UTILS_H

struct utime_t {
  __u32 sec;
  __u32 nsec;
};

struct timespec64 {
  __u64 tv_sec; /* seconds */
  long tv_nsec; /* nanoseconds */
};

static __always_inline __u64 to_nsec(struct utime_t *ut) {
  return (__u64)ut->sec * 1000000000ull + (__u64)ut->nsec;
}

__u32 get_pid() {
  __u64 pid_tgid = bpf_get_current_pid_tgid();
  __u32 pid = pid_tgid >> 32;
  return pid;
}

__u32 get_tid() {
  __u64 pid_tgid = bpf_get_current_pid_tgid();
  __u32 tid = (__u32)pid_tgid;
  return tid;
}

// Fetch register value from pt_regs based on DWARF register number
// Note: switch case is not supported well by eBPF, we'll run into
// "unable to dereference modified ctx" error, so we use if-else chain
__u64 fetch_register(const struct pt_regs *const ctx, int reg) {
  __u64 v = 0;

#if defined(__TARGET_ARCH_x86)
  // x86_64 DWARF register mapping
  if (reg == 0)
    v = ctx->rax;
  else if (reg == 1)
    v = ctx->rdx;
  else if (reg == 2)
    v = ctx->rcx;
  else if (reg == 3)
    v = ctx->rbx;
  else if (reg == 4)
    v = ctx->rsi;
  else if (reg == 5)
    v = ctx->rdi;
  else if (reg == 6)
    v = ctx->rbp;
  else if (reg == 7)
    v = ctx->rsp;
  else if (reg == 8)
    v = ctx->r8;
  else if (reg == 9)
    v = ctx->r9;
  else if (reg == 10)
    v = ctx->r10;
  else if (reg == 11)
    v = ctx->r11;
  else if (reg == 12)
    v = ctx->r12;
  else if (reg == 13)
    v = ctx->r13;
  else if (reg == 14)
    v = ctx->r14;
  else if (reg == 15)
    v = ctx->r15;
  else
    bpf_printk("unexpected x86_64 register: %d\n", reg);

#elif defined(__TARGET_ARCH_arm64)
  // ARM64 DWARF register mapping
  // x0-x30 are registers 0-30, sp is 31
  // Use user_pt_regs which is properly defined in BPF uprobe context
  const struct user_pt_regs *regs = (const struct user_pt_regs *)ctx;
  if (reg >= 0 && reg <= 30)
    v = regs->regs[reg];
  else if (reg == 31)
    v = regs->sp;
  else
    bpf_printk("unexpected arm64 register: %d\n", reg);

#else
  #error "Unsupported architecture for fetch_register"
#endif

  return v;
}


// deal with member dereference vf->size > 1
__u64 fetch_var_member_addr(__u64 cur_addr, struct VarField *vf) {
  if (vf == NULL) return 0;
  //__u64 cur_addr = fetch_register(ctx, vf->varloc.reg);
  if (cur_addr == 0) return 0;
  // special handling for the first member
  __u64 tmpaddr;
  if (vf->varloc.stack) {
    cur_addr += vf->varloc.offset;
    if (vf->fields[1].pointer) {
      bpf_probe_read_user(&tmpaddr, sizeof(tmpaddr), (void *)cur_addr);
      cur_addr = tmpaddr;
    }
  }
  cur_addr += vf->fields[1].offset;

  if (2 >= vf->size) return cur_addr;
  if (vf->fields[2].pointer) {
    bpf_probe_read_user(&tmpaddr, sizeof(tmpaddr), (void *)cur_addr);
    cur_addr = tmpaddr + vf->fields[2].offset;
  } else {
    cur_addr += vf->fields[2].offset;
  }

  if (3 >= vf->size) return cur_addr;
  if (vf->fields[3].pointer) {
    bpf_probe_read_user(&tmpaddr, sizeof(tmpaddr), (void *)cur_addr);
    cur_addr = tmpaddr + vf->fields[3].offset;
  } else {
    cur_addr += vf->fields[3].offset;
  }

  if (4 >= vf->size) return cur_addr;
  if (vf->fields[4].pointer) {
    bpf_probe_read_user(&tmpaddr, sizeof(tmpaddr), (void *)cur_addr);
    cur_addr = tmpaddr + vf->fields[4].offset;
  } else {
    cur_addr += vf->fields[4].offset;
  }

  if (5 >= vf->size) return cur_addr;
  if (vf->fields[5].pointer) {
    bpf_probe_read_user(&tmpaddr, sizeof(tmpaddr), (void *)cur_addr);
    cur_addr = tmpaddr + vf->fields[5].offset;
  } else {
    cur_addr += vf->fields[5].offset;
  }

  if (6 >= vf->size) return cur_addr;
  if (vf->fields[6].pointer) {
    bpf_probe_read_user(&tmpaddr, sizeof(tmpaddr), (void *)cur_addr);
    cur_addr = tmpaddr + vf->fields[6].offset;
  } else {
    cur_addr += vf->fields[6].offset;
  }

  if (7 >= vf->size) return cur_addr;
  if (vf->fields[7].pointer) {
    bpf_probe_read_user(&tmpaddr, sizeof(tmpaddr), (void *)cur_addr);
    cur_addr = tmpaddr + vf->fields[7].offset;
  } else {
    cur_addr += vf->fields[7].offset;
  }

  if (8 >= vf->size) return cur_addr;
  if (vf->fields[8].pointer) {
    bpf_probe_read_user(&tmpaddr, sizeof(tmpaddr), (void *)cur_addr);
    cur_addr = tmpaddr + vf->fields[8].offset;
  } else {
    cur_addr += vf->fields[8].offset;
  }

  return cur_addr;
}

int print_vf(struct VarField *vf) {
  if (vf == NULL) return 0;
  bpf_printk("reg %d offset %d onstack %d\n", vf->varloc.reg, vf->varloc.offset,
             vf->varloc.stack);
  if (1 >= vf->size) return 0;
  bpf_printk("field 1 offset %d, pointer %d", vf->fields[1].offset,
             vf->fields[1].pointer);
  if (2 >= vf->size) return 0;
  bpf_printk("field 2 offset %d, pointer %d", vf->fields[2].offset,
             vf->fields[2].pointer);
  if (3 >= vf->size) return 0;
  bpf_printk("field 3 offset %d, pointer %d", vf->fields[3].offset,
             vf->fields[3].pointer);
  if (4 >= vf->size) return 0;
  bpf_printk("field 4 offset %d, pointer %d", vf->fields[4].offset,
             vf->fields[4].pointer);
  if (5 >= vf->size) return 0;
  bpf_printk("field 5 offset %d, pointer %d", vf->fields[5].offset,
             vf->fields[5].pointer);
  return 0;
}

#endif
