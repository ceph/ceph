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

#include "common/debug.h"

#include "mds/CDir.h"
#include "mds/CInode.h"

#include "DamageTable.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << rank << ".damage " << __func__ << " "

namespace {
/**
 * Record damage to a particular dirfrag, implicitly affecting
 * any dentries within it.
 */
class DirFragDamage : public DamageEntry
{
  public:
  inodeno_t ino;
  frag_t frag;

  DirFragDamage(inodeno_t ino_, frag_t frag_)
    : ino(ino_), frag(frag_)
  {}

  damage_entry_type_t get_type() const override
  {
    return DAMAGE_ENTRY_DIRFRAG;
  }

  void dump(Formatter *f) const override
  {
    f->open_object_section("dir_frag_damage");
    f->dump_string("damage_type", "dir_frag");
    f->dump_int("id", id);
    f->dump_int("ino", ino);
    f->dump_stream("frag") << frag;
    f->dump_string("path", path);
    f->close_section();
  }
};


/**
 * Record damage to a particular dname within a particular dirfrag
 */
class DentryDamage : public DamageEntry
{
  public:
  inodeno_t ino;
  frag_t frag;
  std::string dname;
  snapid_t snap_id;

  DentryDamage(
      inodeno_t ino_,
      frag_t frag_,
      std::string_view dname_,
      snapid_t snap_id_)
    : ino(ino_), frag(frag_), dname(dname_), snap_id(snap_id_)
  {}

  damage_entry_type_t get_type() const override
  {
    return DAMAGE_ENTRY_DENTRY;
  }

  void dump(Formatter *f) const override
  {
    f->open_object_section("dentry_damage");
    f->dump_string("damage_type", "dentry");
    f->dump_int("id", id);
    f->dump_int("ino", ino);
    f->dump_stream("frag") << frag;
    f->dump_string("dname", dname);
    f->dump_stream("snap_id") << snap_id;
    f->dump_string("path", path);
    f->close_section();
  }
};


/**
 * Record damage to our ability to look up an ino by number
 */
class BacktraceDamage : public DamageEntry
{
  public:
  inodeno_t ino;

  BacktraceDamage(inodeno_t ino_)
    : ino(ino_)
  {}

  damage_entry_type_t get_type() const override
  {
    return DAMAGE_ENTRY_BACKTRACE;
  }

  void dump(Formatter *f) const override
  {
    f->open_object_section("backtrace_damage");
    f->dump_string("damage_type", "backtrace");
    f->dump_int("id", id);
    f->dump_int("ino", ino);
    f->dump_string("path", path);
    f->close_section();
  }
};

/**
 * Record about Uninline failures during scrub
 */
class UninlineDamage : public DamageEntry
{
  public:
  inodeno_t ino;
  mds_rank_t rank;
  int32_t failure_errno;
  std::string scrub_tag;

  UninlineDamage(
    inodeno_t ino_, mds_rank_t rank_, int32_t errno_, std::string_view scrub_tag_)
    : ino(ino_), rank(rank_), failure_errno(errno_), scrub_tag(scrub_tag_)
  {}

  damage_entry_type_t get_type() const override
  {
    return DAMAGE_ENTRY_UNINLINE_FILE;
  }

  void dump(Formatter *f) const override
  {
    f->open_object_section("uninline_damage");
    f->dump_string("damage_type", "uninline");
    f->dump_int("id", id);
    f->dump_int("ino", ino);
    f->dump_int("rank", rank);
    f->dump_string("errno", cpp_strerror(failure_errno));
    f->dump_string("scrub_tag", scrub_tag);
    f->dump_string("path", path);
    f->close_section();
  }
};
}

DamageEntry::~DamageEntry()
{}

bool DamageTable::notify_dentry(
    inodeno_t ino, frag_t frag,
    snapid_t snap_id, std::string_view dname, std::string_view path)
{
  if (oversized()) {
    return true;
  }

  // Special cases: damage to these dirfrags is considered fatal to
  // the MDS rank that owns them.
  if (
      (MDS_INO_IS_MDSDIR(ino) && MDS_INO_MDSDIR_OWNER(ino) == rank)
      ||
      (MDS_INO_IS_STRAY(ino) && MDS_INO_STRAY_OWNER(ino) == rank)
     ) {
    derr << "Damage to dentries in fragment " << frag << " of ino " << ino
         << "is fatal because it is a system directory for this rank" << dendl;
    return true;
  }

  auto& df_dentries = dentries[DirFragIdent(ino, frag)];
  if (auto [it, inserted] = df_dentries.try_emplace(DentryIdent(dname, snap_id)); inserted) {
    auto entry = std::make_shared<DentryDamage>(ino, frag, dname, snap_id);
    entry->path = path;
    it->second = entry;
    by_id[entry->id] = std::move(entry);
  }

  return false;
}

bool DamageTable::notify_dirfrag(inodeno_t ino, frag_t frag,
                                 std::string_view path)
{
  // Special cases: damage to these dirfrags is considered fatal to
  // the MDS rank that owns them.
  if ((MDS_INO_IS_STRAY(ino) && MDS_INO_STRAY_OWNER(ino) == rank)
      || (ino == CEPH_INO_ROOT)) {
    derr << "Damage to fragment " << frag << " of ino " << ino
         << " is fatal because it is a system directory for this rank" << dendl;
    return true;
  }

  if (oversized()) {
    return true;
  }

  if (auto [it, inserted] = dirfrags.try_emplace(DirFragIdent(ino, frag)); inserted) {
    DamageEntryRef entry = std::make_shared<DirFragDamage>(ino, frag);
    entry->path = path;
    it->second = entry;
    by_id[entry->id] = std::move(entry);
  }

  return false;
}

bool DamageTable::notify_remote_damaged(inodeno_t ino, std::string_view path)
{
  if (oversized()) {
    return true;
  }

  if (auto [it, inserted] = remotes.try_emplace(ino); inserted) {
    auto entry = std::make_shared<BacktraceDamage>(ino);
    entry->path = path;
    it->second = entry;
    by_id[entry->id] = std::move(entry);
  }

  return false;
}

void DamageTable::remove_dentry_damage_entry(CDir *dir)
{
  if (dentries.count(
        DirFragIdent(dir->inode->ino(), dir->frag)
        ) > 0){
          const auto frag_dentries =
            dentries.at(DirFragIdent(dir->inode->ino(), dir->frag));
          for(const auto &i : frag_dentries) {
            erase(i.second->id);
          }
        }
}

void DamageTable::remove_dirfrag_damage_entry(CDir *dir)
{
  if (is_dirfrag_damaged(dir)){
    erase(dirfrags.find(DirFragIdent(dir->inode->ino(), dir->frag))->second->id);
  }
}

void DamageTable::remove_backtrace_damage_entry(inodeno_t ino)
{  
  if (is_remote_damaged(ino)){
    erase(remotes.find(ino)->second->id);
  }  
}

bool DamageTable::notify_uninline_failed(
  inodeno_t ino,
  mds_rank_t rank,
  int32_t failure_errno,
  std::string_view scrub_tag,
  std::string_view path)
{
  if (oversized()) {
    return true;
  }

  if (auto [it, inserted] = uninline_failures.try_emplace(ino); inserted) {
    auto entry = std::make_shared<UninlineDamage>(ino, rank, errno, scrub_tag);
    entry->path = path;
    it->second = entry;
    by_id[entry->id] = std::move(entry);
  }

  return false;
}

bool DamageTable::oversized() const
{
  return by_id.size() > (size_t)(g_conf()->mds_damage_table_max_entries);
}

bool DamageTable::is_dentry_damaged(
        const CDir *dir_frag,
        std::string_view dname,
        const snapid_t snap_id) const
{
  if (dentries.count(
        DirFragIdent(dir_frag->inode->ino(), dir_frag->frag)
        ) == 0) {
    return false;
  }

  const std::map<DentryIdent, DamageEntryRef> &frag_dentries =
    dentries.at(DirFragIdent(dir_frag->inode->ino(), dir_frag->frag));

  return frag_dentries.count(DentryIdent(dname, snap_id)) > 0;
}

bool DamageTable::is_dirfrag_damaged(
    const CDir *dir_frag) const
{
  return dirfrags.count(
      DirFragIdent(dir_frag->inode->ino(), dir_frag->frag)) > 0;
}

bool DamageTable::is_remote_damaged(
    const inodeno_t ino) const
{
  return remotes.count(ino) > 0;
}

void DamageTable::dump(Formatter *f) const
{
  f->open_array_section("damage_table");
  for (const auto &i : by_id)
  {
    i.second->dump(f);
  }
  f->close_section();
}

void DamageTable::erase(damage_entry_id_t damage_id)
{
  auto by_id_entry = by_id.find(damage_id);
  if (by_id_entry == by_id.end()) {
    return;
  }

  DamageEntryRef entry = by_id_entry->second;
  ceph_assert(entry->id == damage_id);  // Sanity

  const auto type = entry->get_type();
  if (type == DAMAGE_ENTRY_DIRFRAG) {
    auto dirfrag_entry = std::static_pointer_cast<DirFragDamage>(entry);
    dirfrags.erase(DirFragIdent(dirfrag_entry->ino, dirfrag_entry->frag));
  } else if (type == DAMAGE_ENTRY_DENTRY) {
    auto dentry_entry = std::static_pointer_cast<DentryDamage>(entry);
    dentries.erase(DirFragIdent(dentry_entry->ino, dentry_entry->frag));
  } else if (type == DAMAGE_ENTRY_BACKTRACE) {
    auto backtrace_entry = std::static_pointer_cast<BacktraceDamage>(entry);
    remotes.erase(backtrace_entry->ino);
  } else if (type == DAMAGE_ENTRY_UNINLINE_FILE) {
    auto uninline_entry = std::static_pointer_cast<UninlineDamage>(entry);
    uninline_failures.erase(uninline_entry->ino);
  } else {
    derr << "Invalid type " << type << dendl;
    ceph_abort();
  }

  by_id.erase(by_id_entry);
}

