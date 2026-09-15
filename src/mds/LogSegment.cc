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

#include "LogSegment.h"

#include <ostream>

#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"
#include "mdstypes.h" // for dirfrag_t, metareqid_t

#include "include/fs_types.h" // for inodeno_t

LogSegment::LogSegment(uint64_t _seq, loff_t off) :
  seq(_seq), offset(off), end(off),
  dirty_dirfrags(member_offset(CDir, item_dirty)),
  new_dirfrags(member_offset(CDir, item_new)),
  dirty_inodes(member_offset(CInode, item_dirty)),
  dirty_dentries(member_offset(CDentry, item_dirty)),
  open_files(member_offset(CInode, item_open_file)),
  dirty_parent_inodes(member_offset(CInode, item_dirty_parent)),
  dirty_dirfrag_dir(member_offset(CInode, item_dirty_dirfrag_dir)),
  dirty_dirfrag_nest(member_offset(CInode, item_dirty_dirfrag_nest)),
  dirty_dirfrag_dirfragtree(member_offset(CInode, item_dirty_dirfrag_dirfragtree))
{}

LogSegment::~LogSegment() noexcept = default;

std::ostream& operator<<(std::ostream& out, const LogSegment& ls) {
  return out << "LogSegment(" << ls.seq << "/0x" << std::hex << ls.offset
             << "~" << ls.end << std::dec << " events=" << ls.num_events << ")";
}
