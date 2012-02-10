
#include "snap_types.h"
#include "common/Formatter.h"

void SnapRealmInfo::encode(bufferlist& bl) const
{
  h.num_snaps = my_snaps.size();
  h.num_prior_parent_snaps = prior_parent_snaps.size();
  ::encode(h, bl);
  ::encode_nohead(my_snaps, bl);
  ::encode_nohead(prior_parent_snaps, bl);
}

void SnapRealmInfo::decode(bufferlist::iterator& bl)
{
  ::decode(h, bl);
  ::decode_nohead(h.num_snaps, my_snaps, bl);
  ::decode_nohead(h.num_prior_parent_snaps, prior_parent_snaps, bl);
}

void SnapRealmInfo::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino());
  f->dump_unsigned("parent", parent());
  f->dump_unsigned("seq", seq());
  f->dump_unsigned("parent_since", parent_since());
  f->dump_unsigned("created", created());

  f->open_array_section("snaps");
  for (vector<snapid_t>::const_iterator p = my_snaps.begin(); p != my_snaps.end(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();

  f->open_array_section("prior_parent_snaps");
  for (vector<snapid_t>::const_iterator p = prior_parent_snaps.begin(); p != prior_parent_snaps.end(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();  
}

void SnapRealmInfo::generate_test_instances(list<SnapRealmInfo*>& o)
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

void SnapContext::dump(Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->open_array_section("snaps");
  for (vector<snapid_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();
}

void SnapContext::generate_test_instances(list<SnapContext*>& o)
{
  o.push_back(new SnapContext);
  vector<snapid_t> v;
  o.push_back(new SnapContext(10, v));
  v.push_back(18);
  v.push_back(3);
  v.push_back(1);
  o.push_back(new SnapContext(20, v));
}
