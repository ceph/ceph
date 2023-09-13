// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ClientSnapRealm.h"
#include "common/Formatter.h"

using std::set;
using std::vector;

void SnapRealm::build_snap_context()
{
  set<snapid_t> snaps;
  snapid_t max_seq = seq;

  // start with prior_parents?
  for (unsigned i=0; i<prior_parent_snaps.size(); i++)
    snaps.insert(prior_parent_snaps[i]);

  // current parent's snaps
  if (pparent) {
    const SnapContext& psnapc = pparent->get_snap_context();
    for (unsigned i=0; i<psnapc.snaps.size(); i++)
      if (psnapc.snaps[i] >= parent_since)
	snaps.insert(psnapc.snaps[i]);
    if (psnapc.seq > max_seq)
      max_seq = psnapc.seq;
  }

  // my snaps
  for (unsigned i=0; i<my_snaps.size(); i++)
    snaps.insert(my_snaps[i]);

  // ok!
  cached_snap_context.seq = max_seq;
  cached_snap_context.snaps.resize(0);
  cached_snap_context.snaps.reserve(snaps.size());
  for (set<snapid_t>::reverse_iterator p = snaps.rbegin(); p != snaps.rend(); ++p)
    cached_snap_context.snaps.push_back(*p);
}

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
