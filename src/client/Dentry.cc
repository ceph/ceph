// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "include/utime.h"

#include "Dentry.h"
#include "Dir.h"
#include "Inode.h"

#include "common/Formatter.h"
#include "common/strescape.h"

void Dentry::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_stream("dir") << dir->parent_inode->ino;
  if (inode)
    f->dump_stream("ino") << inode->ino;
  f->dump_int("ref", ref);
  f->dump_int("offset", offset);
  if (lease_mds >= 0) {
    f->dump_int("lease_mds", lease_mds);
    f->dump_stream("lease_ttl") << lease_ttl;
    f->dump_unsigned("lease_gen", lease_gen);
    f->dump_unsigned("lease_seq", lease_seq);
  }
  f->dump_int("cap_shared_gen", cap_shared_gen);
}

void Dentry::print(std::ostream& os) const
{
  os << dir->parent_inode->vino();
  os << "[";
  os << "\"" << binstrprint(name) << "\"";
  if (!alternate_name.empty()) {
    os << " altn=\"" << binstrprint(alternate_name, 16) << "\"";
  }
  os << " ref=" << ref;
  if (inode) {
    os << " ino=" << inode->vino();
  } else {
    os << " ino=nil";
  }
  os << " csg=" << cap_shared_gen;
  if (is_renaming) {
    os << " is_renaming=true";
  }
  os << "]";
}

void intrusive_ptr_add_ref(Dentry* dn)
{
  dn->get();
}

void intrusive_ptr_release(Dentry* dn)
{
  dn->put();
}
