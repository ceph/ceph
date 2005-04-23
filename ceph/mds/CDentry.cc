
#include "CDentry.h"
#include "CDir.h"

#include <cassert>

// CDentry

ostream& operator<<(ostream& out, CDentry& dn)
{
  out << "[dentry " << dn.get_name();
  if (dn.is_pinned()) out << " " << dn.num_pins() << " pins";
  if (dn.get_lockstate() == DN_LOCK_UNPINNING) out << " unpinning";
  if (dn.get_lockstate() == DN_LOCK_PREXLOCK) out << " prexlock g=" << dn.get_gather_set();
  if (dn.get_lockstate() == DN_LOCK_XLOCK) out << " xlock";
  out << " in " << *dn.get_dir() << "]";
  return out;
}

CDentry::CDentry(const CDentry& m) {
  assert(0); //std::cerr << "copy cons called, implement me" << endl;
}


void CDentry::mark_dirty() 
{
  dout(10) << " mark_dirty " << *this << endl;
  dirty = true;

  // dir is now dirty (if it wasn't already)
  dir->mark_dirty();
	
  // i now live in that (potentially newly dirty) version
  parent_dir_version = dir->get_version();
}
void CDentry::mark_clean() {
  dout(10) << " mark_clean " << *this << endl;
  dirty = false;
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
