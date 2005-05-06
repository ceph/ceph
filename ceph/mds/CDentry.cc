
#include "CDentry.h"
#include "CInode.h"
#include "CDir.h"

#include <cassert>

// CDentry

ostream& operator<<(ostream& out, CDentry& dn)
{
  out << "[dentry " << dn.get_name();
  if (dn.is_pinned()) out << " " << dn.num_pins() << " pins";
  if (dn.get_lockstate() == DN_LOCK_UNPINNING) out << " unpinning";
  if (dn.is_dirty()) out << " dirty";
  if (dn.get_lockstate() == DN_LOCK_PREXLOCK) out << " prexlock=" << dn.get_xlockedby() << " g=" << dn.get_gather_set();
  if (dn.get_lockstate() == DN_LOCK_XLOCK) out << " xlock=" << dn.get_xlockedby();
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
  if (inode && !dirty) inode->get(CINODE_PIN_DNDIRTY);
	
  // i now live in that (potentially newly dirty) version
  parent_dir_version = dir->get_version();

  dirty = true;
}
void CDentry::mark_clean() {
  dout(10) << " mark_clean " << *this << endl;

  if (dirty && inode) inode->put(CINODE_PIN_DNDIRTY);
  dirty = false;
}	


void CDentry::make_path(string& s)
{
  if (dir->inode->get_parent_dn()) 
	dir->inode->get_parent_dn()->make_path(s);

  s += "/";
  s += name;
}

// =
const CDentry& CDentry::operator= (const CDentry& right) {
  assert(0); //std::cerr << "copy op called, implement me" << endl;
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
