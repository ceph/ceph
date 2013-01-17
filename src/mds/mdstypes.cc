// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "mdstypes.h"
#include "common/Formatter.h"

/*
 * file_layout_policy_t
 */

void file_layout_policy_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(layout, bl);
  ENCODE_FINISH(bl);
}

void file_layout_policy_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(layout, bl);
  DECODE_FINISH(bl);
}

void dump(const ceph_file_layout& l, Formatter *f)
{
  f->dump_unsigned("stripe_unit", l.fl_stripe_unit);
  f->dump_unsigned("stripe_count", l.fl_stripe_count);
  f->dump_unsigned("object_size", l.fl_object_size);
  if (l.fl_cas_hash)
    f->dump_unsigned("cas_hash", l.fl_cas_hash);
  if (l.fl_object_stripe_unit)
    f->dump_unsigned("object_stripe_unit", l.fl_object_stripe_unit);
  if (l.fl_pg_pool)
    f->dump_unsigned("pg_pool", l.fl_pg_pool);
}

void dump(const ceph_dir_layout& l, Formatter *f)
{
  f->dump_unsigned("dir_hash", l.dl_dir_hash);
}

void file_layout_policy_t::dump(Formatter *f) const
{
  ::dump(layout, f);
}

void file_layout_policy_t::generate_test_instances(list<file_layout_policy_t*>& ls)
{
  ls.push_back(new file_layout_policy_t);
  ls.push_back(new file_layout_policy_t);
  ls.back()->layout.fl_stripe_unit = 1024;
  ls.back()->layout.fl_stripe_count = 2;
  ls.back()->layout.fl_object_size = 2048;
  ls.back()->layout.fl_cas_hash = 3;
  ls.back()->layout.fl_object_stripe_unit = 8;
  ls.back()->layout.fl_pg_pool = 9;
}


/*
 * frag_info_t
 */

void frag_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(version, bl);
  ::encode(mtime, bl);
  ::encode(nfiles, bl);
  ::encode(nsubdirs, bl);
  ENCODE_FINISH(bl);
}

void frag_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(version, bl);
  ::decode(mtime, bl);
  ::decode(nfiles, bl);
  ::decode(nsubdirs, bl);
  DECODE_FINISH(bl);
}

void frag_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("num_files", nfiles);
  f->dump_unsigned("num_subdirs", nsubdirs);
}

void frag_info_t::generate_test_instances(list<frag_info_t*>& ls)
{
  ls.push_back(new frag_info_t);
  ls.push_back(new frag_info_t);
  ls.back()->version = 1;
  ls.back()->mtime = utime_t(2, 3);
  ls.back()->nfiles = 4;
  ls.back()->nsubdirs = 5;
}

ostream& operator<<(ostream &out, const frag_info_t &f)
{
  if (f == frag_info_t())
    return out << "f()";
  out << "f(v" << f.version;
  if (f.mtime != utime_t())
    out << " m" << f.mtime;
  if (f.nfiles || f.nsubdirs)
    out << " " << f.size() << "=" << f.nfiles << "+" << f.nsubdirs;
  out << ")";
  return out;
}


/*
 * nest_info_t
 */

void nest_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(version, bl);
  ::encode(rbytes, bl);
  ::encode(rfiles, bl);
  ::encode(rsubdirs, bl);
  ::encode(ranchors, bl);
  ::encode(rsnaprealms, bl);
  ::encode(rctime, bl);
  ENCODE_FINISH(bl);
}

void nest_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(version, bl);
  ::decode(rbytes, bl);
  ::decode(rfiles, bl);
  ::decode(rsubdirs, bl);
  ::decode(ranchors, bl);
  ::decode(rsnaprealms, bl);
  ::decode(rctime, bl);
  DECODE_FINISH(bl);
}

void nest_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_unsigned("rbytes", rbytes);
  f->dump_unsigned("rfiles", rfiles);
  f->dump_unsigned("rsubdirs", rsubdirs);
  f->dump_unsigned("ranchors", ranchors);
  f->dump_unsigned("rsnaprealms", rsnaprealms);
  f->dump_stream("rctime") << rctime;
}

void nest_info_t::generate_test_instances(list<nest_info_t*>& ls)
{
  ls.push_back(new nest_info_t);
  ls.push_back(new nest_info_t);
  ls.back()->version = 1;
  ls.back()->rbytes = 2;
  ls.back()->rfiles = 3;
  ls.back()->rsubdirs = 4;
  ls.back()->ranchors = 5;
  ls.back()->rsnaprealms = 6;
  ls.back()->rctime = utime_t(7, 8);
}

ostream& operator<<(ostream &out, const nest_info_t &n)
{
  if (n == nest_info_t())
    return out << "n()";
  out << "n(v" << n.version;
  if (n.rctime != utime_t())
    out << " rc" << n.rctime;
  if (n.rbytes)
    out << " b" << n.rbytes;
  if (n.ranchors)
    out << " a" << n.ranchors;
  if (n.rsnaprealms)
    out << " sr" << n.rsnaprealms;
  if (n.rfiles || n.rsubdirs)
    out << " " << n.rsize() << "=" << n.rfiles << "+" << n.rsubdirs;
  out << ")";    
  return out;
}


/*
 * client_writeable_range_t
 */

void client_writeable_range_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(range.first, bl);
  ::encode(range.last, bl);
  ::encode(follows, bl);
  ENCODE_FINISH(bl);
}

void client_writeable_range_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(range.first, bl);
  ::decode(range.last, bl);
  ::decode(follows, bl);
  DECODE_FINISH(bl);
}

void client_writeable_range_t::dump(Formatter *f) const
{
  f->open_object_section("byte range");
  f->dump_unsigned("first", range.first);
  f->dump_unsigned("last", range.last);
  f->close_section();
  f->dump_unsigned("follows", follows);
}

void client_writeable_range_t::generate_test_instances(list<client_writeable_range_t*>& ls)
{
  ls.push_back(new client_writeable_range_t);
  ls.push_back(new client_writeable_range_t);
  ls.back()->range.first = 123;
  ls.back()->range.last = 456;
  ls.back()->follows = 12;
}

ostream& operator<<(ostream& out, const client_writeable_range_t& r)
{
  return out << r.range.first << '-' << r.range.last << "@" << r.follows;
}


/*
 * inode_t
 */
void inode_t::encode(bufferlist &bl) const
{
  ENCODE_START(6, 6, bl);

  ::encode(ino, bl);
  ::encode(rdev, bl);
  ::encode(ctime, bl);

  ::encode(mode, bl);
  ::encode(uid, bl);
  ::encode(gid, bl);

  ::encode(nlink, bl);
  ::encode(anchored, bl);

  ::encode(dir_layout, bl);
  ::encode(layout, bl);
  ::encode(size, bl);
  ::encode(truncate_seq, bl);
  ::encode(truncate_size, bl);
  ::encode(truncate_from, bl);
  ::encode(truncate_pending, bl);
  ::encode(mtime, bl);
  ::encode(atime, bl);
  ::encode(time_warp_seq, bl);
  ::encode(client_ranges, bl);

  ::encode(dirstat, bl);
  ::encode(rstat, bl);
  ::encode(accounted_rstat, bl);

  ::encode(version, bl);
  ::encode(file_data_version, bl);
  ::encode(xattr_version, bl);
  ::encode(last_renamed_version, bl);

  ENCODE_FINISH(bl);
}

void inode_t::decode(bufferlist::iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 6, 6, p);

  ::decode(ino, p);
  ::decode(rdev, p);
  ::decode(ctime, p);

  ::decode(mode, p);
  ::decode(uid, p);
  ::decode(gid, p);

  ::decode(nlink, p);
  ::decode(anchored, p);

  if (struct_v >= 4)
    ::decode(dir_layout, p);
  else
    memset(&dir_layout, 0, sizeof(dir_layout));
  ::decode(layout, p);
  ::decode(size, p);
  ::decode(truncate_seq, p);
  ::decode(truncate_size, p);
  ::decode(truncate_from, p);
  if (struct_v >= 5)
    ::decode(truncate_pending, p);
  else
    truncate_pending = 0;
  ::decode(mtime, p);
  ::decode(atime, p);
  ::decode(time_warp_seq, p);
  if (struct_v >= 3) {
    ::decode(client_ranges, p);
  } else {
    map<client_t, client_writeable_range_t::byte_range_t> m;
    ::decode(m, p);
    for (map<client_t, client_writeable_range_t::byte_range_t>::iterator
	q = m.begin(); q != m.end(); q++)
      client_ranges[q->first].range = q->second;
  }
    
  ::decode(dirstat, p);
  ::decode(rstat, p);
  ::decode(accounted_rstat, p);

  ::decode(version, p);
  ::decode(file_data_version, p);
  ::decode(xattr_version, p);
  if (struct_v >= 2)
    ::decode(last_renamed_version, p);

  DECODE_FINISH(p);
}

void inode_t::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_unsigned("rdev", rdev);
  f->dump_stream("ctime") << ctime;
  f->dump_unsigned("mode", mode);
  f->dump_unsigned("uid", uid);
  f->dump_unsigned("gid", gid);
  f->dump_unsigned("nlink", nlink);
  f->dump_unsigned("anchored", (int)anchored);

  f->open_object_section("dir_layout");
  ::dump(dir_layout, f);
  f->close_section();

  f->open_object_section("layout");
  ::dump(layout, f);
  f->close_section();

  f->dump_unsigned("size", size);
  f->dump_unsigned("truncate_seq", truncate_seq);
  f->dump_unsigned("truncate_size", truncate_size);
  f->dump_unsigned("truncate_from", truncate_from);
  f->dump_unsigned("truncate_pending", truncate_pending);
  f->dump_stream("mtime") << mtime;
  f->dump_stream("atime") << atime;
  f->dump_unsigned("time_warp_seq", time_warp_seq);

  f->open_array_section("client_ranges");
  for (map<client_t,client_writeable_range_t>::const_iterator p = client_ranges.begin(); p != client_ranges.end(); ++p) {
    f->open_object_section("client");
    f->dump_unsigned("client", p->first.v);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_object_section("dirstat");
  dirstat.dump(f);
  f->close_section();

  f->open_object_section("rstat");
  rstat.dump(f);
  f->close_section();

  f->open_object_section("accounted_rstat");
  accounted_rstat.dump(f);
  f->close_section();

  f->dump_unsigned("version", version);
  f->dump_unsigned("file_data_version", file_data_version);
  f->dump_unsigned("xattr_version", xattr_version);
  f->dump_unsigned("last_renamed_version", last_renamed_version);
}

void inode_t::generate_test_instances(list<inode_t*>& ls)
{
  ls.push_back(new inode_t);
  ls.push_back(new inode_t);
  ls.back()->ino = 1;
  // i am lazy.
}


/*
 * old_inode_t
 */
void old_inode_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(first, bl);
  ::encode(inode, bl);
  ::encode(xattrs, bl);
  ENCODE_FINISH(bl);
}

void old_inode_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(first, bl);
  ::decode(inode, bl);
  ::decode(xattrs, bl);
  DECODE_FINISH(bl);
}

void old_inode_t::dump(Formatter *f) const
{
  f->dump_unsigned("first", first);
  inode.dump(f);
  f->open_object_section("xattrs");
  for (map<string,bufferptr>::const_iterator p = xattrs.begin(); p != xattrs.end(); ++p) {
    string v(p->second.c_str(), p->second.length());
    f->dump_string(p->first.c_str(), v);
  }
  f->close_section();
}

void old_inode_t::generate_test_instances(list<old_inode_t*>& ls)
{
  ls.push_back(new old_inode_t);
  ls.push_back(new old_inode_t);
  ls.back()->first = 2;
  list<inode_t*> ils;
  inode_t::generate_test_instances(ils);
  ls.back()->inode = *ils.back();
  ls.back()->xattrs["user.foo"] = buffer::copy("asdf", 4);
  ls.back()->xattrs["user.unprintable"] = buffer::copy("\000\001\002", 3);
}


/*
 * fnode_t
 */
void fnode_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(version, bl);
  ::encode(snap_purged_thru, bl);
  ::encode(fragstat, bl);
  ::encode(accounted_fragstat, bl);
  ::encode(rstat, bl);
  ::encode(accounted_rstat, bl);
  ENCODE_FINISH(bl);
}

void fnode_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(version, bl);
  ::decode(snap_purged_thru, bl);
  ::decode(fragstat, bl);
  ::decode(accounted_fragstat, bl);
  ::decode(rstat, bl);
  ::decode(accounted_rstat, bl);
  DECODE_FINISH(bl);
}

void fnode_t::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_unsigned("snap_purged_thru", snap_purged_thru);

  f->open_object_section("fragstat");
  fragstat.dump(f);
  f->close_section();

  f->open_object_section("accounted_fragstat");
  accounted_fragstat.dump(f);
  f->close_section();

  f->open_object_section("rstat");
  rstat.dump(f);
  f->close_section();

  f->open_object_section("accounted_rstat");
  accounted_rstat.dump(f);
  f->close_section();
}

void fnode_t::generate_test_instances(list<fnode_t*>& ls)
{
  ls.push_back(new fnode_t);
  ls.push_back(new fnode_t);
  ls.back()->version = 1;
  ls.back()->snap_purged_thru = 2;
  list<frag_info_t*> fls;
  frag_info_t::generate_test_instances(fls);
  ls.back()->fragstat = *fls.back();
  ls.back()->accounted_fragstat = *fls.front();
  list<nest_info_t*> nls;
  nest_info_t::generate_test_instances(nls);
  ls.back()->rstat = *nls.front();
  ls.back()->accounted_rstat = *nls.back();
}


/*
 * session_info_t
 */
void session_info_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(inst, bl);
  ::encode(completed_requests, bl);
  ::encode(prealloc_inos, bl);   // hacky, see below.
  ::encode(used_inos, bl);
  ENCODE_FINISH(bl);
}

void session_info_t::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  ::decode(inst, p);
  ::decode(completed_requests, p);
  ::decode(prealloc_inos, p);
  ::decode(used_inos, p);
  prealloc_inos.insert(used_inos);
  used_inos.clear();
  DECODE_FINISH(p);
}

void session_info_t::dump(Formatter *f) const
{
  f->dump_stream("inst") << inst;

  f->open_array_section("completed_requests");
  for (set<tid_t>::const_iterator p = completed_requests.begin();
       p != completed_requests.end();
       ++p)
    f->dump_unsigned("tid", *p);
  f->close_section();

  f->open_array_section("prealloc_inos");
  for (interval_set<inodeno_t>::const_iterator p = prealloc_inos.begin();
       p != prealloc_inos.end();
       ++p) {
    f->open_object_section("ino_range");
    f->dump_unsigned("start", p.get_start());
    f->dump_unsigned("length", p.get_len());
    f->close_section();
  }
  f->close_section();

  f->open_array_section("used_inos");
  for (interval_set<inodeno_t>::const_iterator p = prealloc_inos.begin();
       p != prealloc_inos.end();
       ++p) {
    f->open_object_section("ino_range");
    f->dump_unsigned("start", p.get_start());
    f->dump_unsigned("length", p.get_len());
    f->close_section();
  }
  f->close_section();
}

void session_info_t::generate_test_instances(list<session_info_t*>& ls)
{
  ls.push_back(new session_info_t);
  ls.push_back(new session_info_t);
  ls.back()->inst = entity_inst_t(entity_name_t::MDS(12), entity_addr_t());
  ls.back()->completed_requests.insert(234);
  ls.back()->completed_requests.insert(237);
  ls.back()->prealloc_inos.insert(333, 12);
  ls.back()->prealloc_inos.insert(377, 112);
  // we can't add used inos; they're cleared on decode
}
