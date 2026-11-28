// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Clyso Technologies Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "common/livepatch.h"

#include <cstring>
#include <memory>

#include "common/Formatter.h"

#ifdef WITH_LIVEPATCH
// libpulp's in-process state, as its own `ulp patches` tool reads it from
// outside with ptrace (include/ulp_common.h). libpulp has no in-process API
// for this, so the layout below is only trusted for the releases it was
// checked against; see known_layout(). The symbols are weak: they are only
// there when the process was started with libpulp.so preloaded.
extern "C" {
struct ulp_applied_patch {
  unsigned char patch_id[32];
  const char *lib_name;
  const char *container_name;
  void *units;
  struct ulp_applied_patch *next;
  void *deps;
  time_t timestamp;
  void *so_handler;
};
struct ulp_patching_state {
  char load_state;
  struct ulp_applied_patch *patches;
};
extern struct ulp_patching_state __ulp_state __attribute__((weak));
extern const char __ulp_version[] __attribute__((weak));
}

namespace {

// struct ulp_applied_patch as of libpulp 0.3.17
bool known_layout()
{
  return &__ulp_version[0] != nullptr &&
    strncmp(__ulp_version, "0.3.", 4) == 0;
}

struct raw_patch_t {
  unsigned char id[20];   // `ulp dump` prints 20 of the 32 bytes
  char target[256];
  char file[256];
  time_t applied;
};

void copy_name(char *dst, size_t len, const char *src)
{
  dst[0] = 0;
  if (src) {
    strncpy(dst, src, len - 1);
    dst[len - 1] = 0;
  }
}

} // anonymous namespace
#endif

namespace ceph::livepatch {

bool enabled()
{
#ifdef WITH_LIVEPATCH
  return &__ulp_state != nullptr && __ulp_state.load_state;
#else
  return false;
#endif
}

std::string runtime_version()
{
#ifdef WITH_LIVEPATCH
  if (enabled() && &__ulp_version[0] != nullptr) {
    return std::string("libpulp ") + __ulp_version;
  }
#endif
  return {};
}

std::vector<patch_t> list()
{
  std::vector<patch_t> v;
#ifdef WITH_LIVEPATCH
  if (!enabled() || !known_layout()) {
    return v;
  }
  // libpulp changes this list only while it has every thread of the process
  // stopped, and a revert frees the entry it unlinks. Nothing libpulp exports
  // lets a thread exclude that, so copy the list out without allocating or
  // blocking, to keep the time spent holding pointers into it as short as
  // possible. A revert that lands inside this loop can still leave it reading
  // a freed entry.
  static constexpr size_t max_patches = 32;
  auto raw = std::make_unique<raw_patch_t[]>(max_patches);
  size_t n = 0;
  for (auto p = __ulp_state.patches; p && n < max_patches; p = p->next, n++) {
    memcpy(raw[n].id, p->patch_id, sizeof(raw[n].id));
    copy_name(raw[n].target, sizeof(raw[n].target), p->lib_name);
    copy_name(raw[n].file, sizeof(raw[n].file), p->container_name);
    raw[n].applied = p->timestamp;
  }
  // libpulp pushes new patches in front
  for (size_t i = n; i-- > 0; ) {
    static const char hex[] = "0123456789abcdef";
    patch_t e;
    for (auto c : raw[i].id) {
      e.id += hex[c >> 4];
      e.id += hex[c & 0xf];
    }
    e.target = raw[i].target;
    e.file = raw[i].file;
    e.applied = raw[i].applied;
    v.push_back(std::move(e));
  }
#endif
  return v;
}

void dump(Formatter *f)
{
  if (!enabled()) {
    return;
  }
  f->dump_bool("livepatch_enabled", true);
  f->dump_string("livepatch_runtime", runtime_version());
  f->open_array_section("livepatches");
  for (auto& p : list()) {
    f->open_object_section("livepatch");
    f->dump_string("id", p.id);
    f->dump_string("target", p.target);
    f->dump_string("file", p.file);
    f->dump_int("applied", p.applied);
    f->close_section();
  }
  f->close_section();
}

} // namespace ceph::livepatch
