// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ClientSnapRealm.h"
#include "common/Formatter.h"

void SnapRealm::dump(Formatter *f) const
{
  f->dump_stream("ino") << ino;
  f->dump_int("nref", nref);
  f->dump_stream("created") << created;
  f->dump_stream("seq") << seq;
  f->dump_stream("parent_ino") << parent;
  f->dump_stream("parent_since") << parent_since;

  f->open_array_section("prior_parent_snaps");
  for (vector<snapid_t>::const_iterator p = prior_parent_snaps.begin(); p != prior_parent_snaps.end(); ++p)
    f->dump_stream("snapid") << *p;
  f->close_section();
  f->open_array_section("my_snaps");
  for (vector<snapid_t>::const_iterator p = my_snaps.begin(); p != my_snaps.end(); ++p)
    f->dump_stream("snapid") << *p;
  f->close_section();

  f->open_array_section("children");
  for (set<SnapRealm*>::const_iterator p = pchildren.begin(); p != pchildren.end(); ++p)
    f->dump_stream("child") << (*p)->ino;
  f->close_section();
}
