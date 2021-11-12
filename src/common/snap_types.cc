// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "snap_types.h"
#include "common/Formatter.h"

void SnapRealmInfo::encode(ceph::buffer::list& bl) const
{
  h.num_snaps = my_snaps.size();
  h.num_prior_parent_snaps = prior_parent_snaps.size();
  using ceph::encode;
  encode(h, bl);
  ceph::encode_nohead(my_snaps, bl);
  ceph::encode_nohead(prior_parent_snaps, bl);
}

void SnapRealmInfo::decode(ceph::buffer::list::const_iterator& bl)
{
  using ceph::decode;
  decode(h, bl);
  ceph::decode_nohead(h.num_snaps, my_snaps, bl);
  ceph::decode_nohead(h.num_prior_parent_snaps, prior_parent_snaps, bl);
}

void SnapRealmInfo::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("ino", ino());
  f->dump_unsigned("parent", parent());
  f->dump_unsigned("seq", seq());
  f->dump_unsigned("parent_since", parent_since());
  f->dump_unsigned("created", created());

  f->open_array_section("snaps");
  for (auto p = my_snaps.begin(); p != my_snaps.end(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();

  f->open_array_section("prior_parent_snaps");
  for (auto p = prior_parent_snaps.begin(); p != prior_parent_snaps.end(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();
}

void SnapRealmInfo::generate_test_instances(std::list<SnapRealmInfo*>& o)
{
  o.push_back(new SnapRealmInfo);
  o.push_back(new SnapRealmInfo(1, 10, 10, 0));
  o.push_back(new SnapRealmInfo(1, 10, 10, 0));
  o.back()->my_snaps.push_back(10);
  o.push_back(new SnapRealmInfo(1, 10, 10, 5));
  o.back()->my_snaps.push_back(10);
  o.back()->prior_parent_snaps.push_back(3);
  o.back()->prior_parent_snaps.push_back(5);
}


// -----

bool SnapContext::is_valid() const
{
  // seq is a valid snapid
  if (seq > CEPH_MAXSNAP)
    return false;
  if (!snaps.empty()) {
    // seq >= snaps[0]
    if (snaps[0] > seq)
      return false;
    // snaps[] is descending
    snapid_t t = snaps[0];
    for (unsigned i=1; i<snaps.size(); i++) {
      if (snaps[i] >= t || t == 0)
	return false;
      t = snaps[i];
    }
  }
  return true;
}

void SnapContext::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->open_array_section("snaps");
  for (auto p = snaps.cbegin(); p != snaps.cend(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();
}

void SnapContext::generate_test_instances(std::list<SnapContext*>& o)
{
  o.push_back(new SnapContext);
  std::vector<snapid_t> v;
  o.push_back(new SnapContext(10, v));
  v.push_back(18);
  v.push_back(3);
  v.push_back(1);
  o.push_back(new SnapContext(20, v));
}
