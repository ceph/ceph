// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */


#include "CDentry.h"
#include "CInode.h"
#include "CDir.h"

#include <cassert>

// CDentry

ostream& operator<<(ostream& out, CDentry& dn)
{
  out << "[dentry " << dn.get_name();
  if (dn.is_pinned()) out << " " << dn.num_pins() << " pins";
  
  if (dn.is_null()) out << " NULL";
  if (dn.is_remote()) out << " REMOTE";

  if (dn.get_lockstate() == DN_LOCK_UNPINNING) out << " unpinning";
  if (dn.is_dirty()) out << " dirty";
  if (dn.get_lockstate() == DN_LOCK_PREXLOCK) out << " prexlock=" << dn.get_xlockedby() << " g=" << dn.get_gather_set();
  if (dn.get_lockstate() == DN_LOCK_XLOCK) out << " xlock=" << dn.get_xlockedby();

  out << " dirv=" << dn.get_parent_dir_version();

  out << " inode=" << dn.get_inode();
  out << " " << &dn;
  out << " in " << *dn.get_dir();
  out << "]";
  return out;
}

CDentry::CDentry(const CDentry& m) {
  assert(0); //std::cerr << "copy cons called, implement me" << endl;
}


void CDentry::mark_dirty() 
{
  dout(10) << " mark_dirty " << *this << endl;

  // dir is now dirty (if it wasn't already)
  dir->mark_dirty();

  // pin inode?
  if (is_primary() && !dirty && inode) inode->get(CINODE_PIN_DNDIRTY);
	
  // i now live in that (potentially newly dirty) version
  parent_dir_version = dir->get_version();

  dirty = true;
}
void CDentry::mark_clean() {
  dout(10) << " mark_clean " << *this << endl;
  assert(parent_dir_version <= dir->get_version());
  assert(parent_dir_version >= dir->get_last_committed_version());

  if (is_primary() && dirty && inode) inode->put(CINODE_PIN_DNDIRTY);
  dirty = false;
}	


void CDentry::make_path(string& s)
{
  if (dir->inode->get_parent_dn()) 
	dir->inode->get_parent_dn()->make_path(s);

  s += "/";
  s += name;
}


void CDentry::link_remote(CInode *in)
{
  assert(is_remote());
  assert(in->ino() == remote_ino);

  inode = in;
  in->add_remote_parent(this);
}

void CDentry::unlink_remote()
{
  assert(is_remote());
  assert(inode);
  
  inode->remove_remote_parent(this);
  inode = 0;
}





// =
const CDentry& CDentry::operator= (const CDentry& right) {
  assert(0); //std::cerr << "copy op called, implement me" << endl;
  return *this;
}

  // comparisons
  bool CDentry::operator== (const CDentry& right) const {
	return name == right.name;
  }
  bool CDentry::operator!= (const CDentry& right) const {
	return name == right.name;
  }
  bool CDentry::operator< (const CDentry& right) const {
	return name < right.name;
  }
  bool CDentry::operator> (const CDentry& right) const {
	return name > right.name;
  }
  bool CDentry::operator>= (const CDentry& right) const {
	return name >= right.name;
  }
  bool CDentry::operator<= (const CDentry& right) const {
	return name <= right.name;
  }
