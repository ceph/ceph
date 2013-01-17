// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "common/Formatter.h"

void dump(const ceph_dir_layout& dl, Formatter *f)
{
  f->dump_unsigned("dir_hash", dl.dl_dir_hash);
}
