// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "mdstypes.h"
#include "common/Formatter.h"

const mds_gid_t MDS_GID_NONE = mds_gid_t(0);
const mds_rank_t MDS_RANK_NONE = mds_rank_t(-1);


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
  ENCODE_START(3, 2, bl);
  ::encode(version, bl);
  ::encode(rbytes, bl);
  ::encode(rfiles, bl);
  ::encode(rsubdirs, bl);
  {
    // removed field
    int64_t ranchors = 0;
    ::encode(ranchors, bl);
  }
  ::encode(rsnaprealms, bl);
  ::encode(rctime, bl);
  ENCODE_FINISH(bl);
}

void nest_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  ::decode(version, bl);
  ::decode(rbytes, bl);
  ::decode(rfiles, bl);
  ::decode(rsubdirs, bl);
  {
    int64_t ranchors;
    ::decode(ranchors, bl);
  }
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
  if (n.rsnaprealms)
    out << " sr" << n.rsnaprealms;
  if (n.rfiles || n.rsubdirs)
    out << " " << n.rsize() << "=" << n.rfiles << "+" << n.rsubdirs;
  out << ")";    
  return out;
}

/*
 * quota_info_t
 */
void quota_info_t::dump(Formatter *f) const
{
  f->dump_int("max_bytes", max_bytes);
  f->dump_int("max_files", max_files);
}

void quota_info_t::generate_test_instances(list<quota_info_t *>& ls)
{
  ls.push_back(new quota_info_t);
  ls.push_back(new quota_info_t);
  ls.back()->max_bytes = 16;
  ls.back()->max_files = 16;
}

ostream& operator<<(ostream &out, const quota_info_t &n)
{
  out << "quota("
      << "max_bytes = " << n.max_bytes
      << " max_files = " << n.max_files
      << ")";
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
 * inline_data_t
 */
void inline_data_t::encode(bufferlist &bl) const
{
  ::encode(version, bl);
  if (blp)
    ::encode(*blp, bl);
  else
    ::encode(bufferlist(), bl);
}
void inline_data_t::decode(bufferlist::iterator &p)
{
  ::decode(version, p);
  uint32_t inline_len;
  ::decode(inline_len, p);
  if (inline_len > 0)
    ::decode_nohead(inline_len, get_data(), p);
  else
    free_data();
}

/*
 * inode_t
 */
void inode_t::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(13, 6, bl);

  ::encode(ino, bl);
  ::encode(rdev, bl);
  ::encode(ctime, bl);

  ::encode(mode, bl);
  ::encode(uid, bl);
  ::encode(gid, bl);

  ::encode(nlink, bl);
  {
    // removed field
    bool anchored = 0;
    ::encode(anchored, bl);
  }

  ::encode(dir_layout, bl);
  ::encode(layout, bl, features);
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
  ::encode(backtrace_version, bl);
  ::encode(old_pools, bl);
  ::encode(max_size_ever, bl);
  ::encode(inline_data, bl);
  ::encode(quota, bl);

  ::encode(stray_prior_path, bl);

  ::encode(last_scrub_version, bl);
  ::encode(last_scrub_stamp, bl);

  ENCODE_FINISH(bl);
}

void inode_t::decode(bufferlist::iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(13, 6, 6, p);

  ::decode(ino, p);
  ::decode(rdev, p);
  ::decode(ctime, p);

  ::decode(mode, p);
  ::decode(uid, p);
  ::decode(gid, p);

  ::decode(nlink, p);
  {
    bool anchored;
    ::decode(anchored, p);
  }

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
	q = m.begin(); q != m.end(); ++q)
      client_ranges[q->first].range = q->second;
  }
    
  ::decode(dirstat, p);
  ::decode(rstat, p);
  ::decode(accounted_rstat, p);

  ::decode(version, p);
  ::decode(file_data_version, p);
  ::decode(xattr_version, p);
  if (struct_v >= 2)
    ::decode(backtrace_version, p);
  if (struct_v >= 7)
    ::decode(old_pools, p);
  if (struct_v >= 8)
    ::decode(max_size_ever, p);
  if (struct_v >= 9) {
    ::decode(inline_data, p);
  } else {
    inline_data.version = CEPH_INLINE_NONE;
  }
  if (struct_v < 10)
    backtrace_version = 0; // force update backtrace
  if (struct_v >= 11)
    ::decode(quota, p);

  if (struct_v >= 12) {
    ::decode(stray_prior_path, p);
  }

  if (struct_v >= 13) {
    ::decode(last_scrub_version, p);
    ::decode(last_scrub_stamp, p);
  }

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

  f->open_object_section("dir_layout");
  ::dump(dir_layout, f);
  f->close_section();

  f->dump_object("layout", layout);

  f->open_array_section("old_pools");
  for (compact_set<int64_t>::const_iterator i = old_pools.begin();
       i != old_pools.end();
       ++i)
    f->dump_int("pool", *i);
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
  f->dump_unsigned("backtrace_version", backtrace_version);

  f->dump_string("stray_prior_path", stray_prior_path);
}

void inode_t::generate_test_instances(list<inode_t*>& ls)
{
  ls.push_back(new inode_t);
  ls.push_back(new inode_t);
  ls.back()->ino = 1;
  // i am lazy.
}

int inode_t::compare(const inode_t &other, bool *divergent) const
{
  assert(ino == other.ino);
  *divergent = false;
  if (version == other.version) {
    if (rdev != other.rdev ||
        ctime != other.ctime ||
        mode != other.mode ||
        uid != other.uid ||
        gid != other.gid ||
        nlink != other.nlink ||
        memcmp(&dir_layout, &other.dir_layout, sizeof(dir_layout)) ||
        layout != other.layout ||
        old_pools != other.old_pools ||
        size != other.size ||
        max_size_ever != other.max_size_ever ||
        truncate_seq != other.truncate_seq ||
        truncate_size != other.truncate_size ||
        truncate_from != other.truncate_from ||
        truncate_pending != other.truncate_pending ||
        mtime != other.mtime ||
        atime != other.atime ||
        time_warp_seq != other.time_warp_seq ||
        inline_data != other.inline_data ||
        client_ranges != other.client_ranges ||
        !(dirstat == other.dirstat) ||
        !(rstat == other.rstat) ||
        !(accounted_rstat == other.accounted_rstat) ||
        file_data_version != other.file_data_version ||
        xattr_version != other.xattr_version ||
        backtrace_version != other.backtrace_version) {
      *divergent = true;
    }
    return 0;
  } else if (version > other.version) {
    *divergent = !older_is_consistent(other);
    return 1;
  } else {
    assert(version < other.version);
    *divergent = !other.older_is_consistent(*this);
    return -1;
  }
}

bool inode_t::older_is_consistent(const inode_t &other) const
{
  if (max_size_ever < other.max_size_ever ||
      truncate_seq < other.truncate_seq ||
      time_warp_seq < other.time_warp_seq ||
      inline_data.version < other.inline_data.version ||
      dirstat.version < other.dirstat.version ||
      rstat.version < other.rstat.version ||
      accounted_rstat.version < other.accounted_rstat.version ||
      file_data_version < other.file_data_version ||
      xattr_version < other.xattr_version ||
      backtrace_version < other.backtrace_version) {
    return false;
  }
  return true;
}

/*
 * old_inode_t
 */
void old_inode_t::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  ::encode(first, bl);
  ::encode(inode, bl, features);
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
  ENCODE_START(4, 3, bl);
  ::encode(version, bl);
  ::encode(snap_purged_thru, bl);
  ::encode(fragstat, bl);
  ::encode(accounted_fragstat, bl);
  ::encode(rstat, bl);
  ::encode(accounted_rstat, bl);
  ::encode(damage_flags, bl);
  ::encode(recursive_scrub_version, bl);
  ::encode(recursive_scrub_stamp, bl);
  ::encode(localized_scrub_version, bl);
  ::encode(localized_scrub_stamp, bl);
  ENCODE_FINISH(bl);
}

void fnode_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  ::decode(version, bl);
  ::decode(snap_purged_thru, bl);
  ::decode(fragstat, bl);
  ::decode(accounted_fragstat, bl);
  ::decode(rstat, bl);
  ::decode(accounted_rstat, bl);
  if (struct_v >= 3) {
    ::decode(damage_flags, bl);
  }
  if (struct_v >= 4) {
    ::decode(recursive_scrub_version, bl);
    ::decode(recursive_scrub_stamp, bl);
    ::decode(localized_scrub_version, bl);
    ::decode(localized_scrub_stamp, bl);
  }
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
 * old_rstat_t
 */
void old_rstat_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(first, bl);
  ::encode(rstat, bl);
  ::encode(accounted_rstat, bl);
  ENCODE_FINISH(bl);
}

void old_rstat_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(first, bl);
  ::decode(rstat, bl);
  ::decode(accounted_rstat, bl);
  DECODE_FINISH(bl);
}

void old_rstat_t::dump(Formatter *f) const
{
  f->dump_unsigned("snapid", first);
  f->open_object_section("rstat");
  rstat.dump(f);
  f->close_section();
  f->open_object_section("accounted_rstat");
  accounted_rstat.dump(f);
  f->close_section();
}

void old_rstat_t::generate_test_instances(list<old_rstat_t*>& ls)
{
  ls.push_back(new old_rstat_t());
  ls.push_back(new old_rstat_t());
  ls.back()->first = 12;
  list<nest_info_t*> nls;
  nest_info_t::generate_test_instances(nls);
  ls.back()->rstat = *nls.back();
  ls.back()->accounted_rstat = *nls.front();
}

/*
 * session_info_t
 */
void session_info_t::encode(bufferlist& bl) const
{
  ENCODE_START(6, 3, bl);
  ::encode(inst, bl);
  ::encode(completed_requests, bl);
  ::encode(prealloc_inos, bl);   // hacky, see below.
  ::encode(used_inos, bl);
  ::encode(client_metadata, bl);
  ::encode(completed_flushes, bl);
  ::encode(auth_name, bl);
  ENCODE_FINISH(bl);
}

void session_info_t::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 2, 2, p);
  ::decode(inst, p);
  if (struct_v <= 2) {
    set<ceph_tid_t> s;
    ::decode(s, p);
    while (!s.empty()) {
      completed_requests[*s.begin()] = inodeno_t();
      s.erase(s.begin());
    }
  } else {
    ::decode(completed_requests, p);
  }
  ::decode(prealloc_inos, p);
  ::decode(used_inos, p);
  prealloc_inos.insert(used_inos);
  used_inos.clear();
  if (struct_v >= 4) {
    ::decode(client_metadata, p);
  }
  if (struct_v >= 5) {
    ::decode(completed_flushes, p);
  }
  if (struct_v >= 6) {
    ::decode(auth_name, p);
  }
  DECODE_FINISH(p);
}

void session_info_t::dump(Formatter *f) const
{
  f->dump_stream("inst") << inst;

  f->open_array_section("completed_requests");
  for (map<ceph_tid_t,inodeno_t>::const_iterator p = completed_requests.begin();
       p != completed_requests.end();
       ++p) {
    f->open_object_section("request");
    f->dump_unsigned("tid", p->first);
    f->dump_stream("created_ino") << p->second;
    f->close_section();
  }
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

  for (map<string, string>::const_iterator i = client_metadata.begin();
      i != client_metadata.end(); ++i) {
    f->dump_string(i->first.c_str(), i->second);
  }
}

void session_info_t::generate_test_instances(list<session_info_t*>& ls)
{
  ls.push_back(new session_info_t);
  ls.push_back(new session_info_t);
  ls.back()->inst = entity_inst_t(entity_name_t::MDS(12), entity_addr_t());
  ls.back()->completed_requests.insert(make_pair(234, inodeno_t(111222)));
  ls.back()->completed_requests.insert(make_pair(237, inodeno_t(222333)));
  ls.back()->prealloc_inos.insert(333, 12);
  ls.back()->prealloc_inos.insert(377, 112);
  // we can't add used inos; they're cleared on decode
}


/*
 * string_snap_t
 */
void string_snap_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(name, bl);
  ::encode(snapid, bl);
  ENCODE_FINISH(bl);
}

void string_snap_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(name, bl);
  ::decode(snapid, bl);
  DECODE_FINISH(bl);
}

void string_snap_t::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_unsigned("snapid", snapid);
}

void string_snap_t::generate_test_instances(list<string_snap_t*>& ls)
{
  ls.push_back(new string_snap_t);
  ls.push_back(new string_snap_t);
  ls.back()->name = "foo";
  ls.back()->snapid = 123;
  ls.push_back(new string_snap_t);
  ls.back()->name = "bar";
  ls.back()->snapid = 456;
}


/*
 * MDSCacheObjectInfo
 */
void MDSCacheObjectInfo::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(ino, bl);
  ::encode(dirfrag, bl);
  ::encode(dname, bl);
  ::encode(snapid, bl);
  ENCODE_FINISH(bl);
}

void MDSCacheObjectInfo::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  ::decode(ino, p);
  ::decode(dirfrag, p);
  ::decode(dname, p);
  ::decode(snapid, p);
  DECODE_FINISH(p);
}

void MDSCacheObjectInfo::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_stream("dirfrag") << dirfrag;
  f->dump_string("name", dname);
  f->dump_unsigned("snapid", snapid);
}

void MDSCacheObjectInfo::generate_test_instances(list<MDSCacheObjectInfo*>& ls)
{
  ls.push_back(new MDSCacheObjectInfo);
  ls.push_back(new MDSCacheObjectInfo);
  ls.back()->ino = 1;
  ls.back()->dirfrag = dirfrag_t(2, 3);
  ls.back()->dname = "fooname";
  ls.back()->snapid = CEPH_NOSNAP;
  ls.push_back(new MDSCacheObjectInfo);
  ls.back()->ino = 121;
  ls.back()->dirfrag = dirfrag_t(222, 0);
  ls.back()->dname = "bar foo";
  ls.back()->snapid = 21322;
}


/*
 * mds_table_pending_t
 */
void mds_table_pending_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(reqid, bl);
  ::encode(mds, bl);
  ::encode(tid, bl);
  ENCODE_FINISH(bl);
}

void mds_table_pending_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(reqid, bl);
  ::decode(mds, bl);
  ::decode(tid, bl);
  DECODE_FINISH(bl);
}

void mds_table_pending_t::dump(Formatter *f) const
{
  f->dump_unsigned("reqid", reqid);
  f->dump_unsigned("mds", mds);
  f->dump_unsigned("tid", tid);
}

void mds_table_pending_t::generate_test_instances(list<mds_table_pending_t*>& ls)
{
  ls.push_back(new mds_table_pending_t);
  ls.push_back(new mds_table_pending_t);
  ls.back()->reqid = 234;
  ls.back()->mds = 2;
  ls.back()->tid = 35434;
}


/*
 * inode_load_vec_t
 */
void inode_load_vec_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  for (int i=0; i<NUM; i++)
    ::encode(vec[i], bl);
  ENCODE_FINISH(bl);
}

void inode_load_vec_t::decode(const utime_t &t, bufferlist::iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  for (int i=0; i<NUM; i++)
    ::decode(vec[i], t, p);
  DECODE_FINISH(p);
}

void inode_load_vec_t::dump(Formatter *f)
{
  f->open_array_section("Decay Counters");
  for (vector<DecayCounter>::const_iterator i = vec.begin(); i != vec.end(); ++i) {
    f->open_object_section("Decay Counter");
    i->dump(f);
    f->close_section();
  }
  f->close_section();
}

void inode_load_vec_t::generate_test_instances(list<inode_load_vec_t*>& ls)
{
  utime_t sample;
  ls.push_back(new inode_load_vec_t(sample));
}


/*
 * dirfrag_load_vec_t
 */
void dirfrag_load_vec_t::dump(Formatter *f) const
{
  f->open_array_section("Decay Counters");
  for (vector<DecayCounter>::const_iterator i = vec.begin(); i != vec.end(); ++i) {
    f->open_object_section("Decay Counter");
    i->dump(f);
    f->close_section();
  }
  f->close_section();
}

void dirfrag_load_vec_t::generate_test_instances(list<dirfrag_load_vec_t*>& ls)
{
  utime_t sample;
  ls.push_back(new dirfrag_load_vec_t(sample));
}

/*
 * mds_load_t
 */
void mds_load_t::encode(bufferlist &bl) const {
  ENCODE_START(2, 2, bl);
  ::encode(auth, bl);
  ::encode(all, bl);
  ::encode(req_rate, bl);
  ::encode(cache_hit_rate, bl);
  ::encode(queue_len, bl);
  ::encode(cpu_load_avg, bl);
  ENCODE_FINISH(bl);
}

void mds_load_t::decode(const utime_t &t, bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(auth, t, bl);
  ::decode(all, t, bl);
  ::decode(req_rate, bl);
  ::decode(cache_hit_rate, bl);
  ::decode(queue_len, bl);
  ::decode(cpu_load_avg, bl);
  DECODE_FINISH(bl);
}

void mds_load_t::dump(Formatter *f) const
{
  f->dump_float("request rate", req_rate);
  f->dump_float("cache hit rate", cache_hit_rate);
  f->dump_float("queue length", queue_len);
  f->dump_float("cpu load", cpu_load_avg);
  f->open_object_section("auth dirfrag");
  auth.dump(f);
  f->close_section();
  f->open_object_section("all dirfrags");
  all.dump(f);
  f->close_section();
}

void mds_load_t::generate_test_instances(list<mds_load_t*>& ls)
{
  utime_t sample;
  ls.push_back(new mds_load_t(sample));
}

/*
 * cap_reconnect_t
 */
void cap_reconnect_t::encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);
  encode_old(bl); // extract out when something changes
  ::encode(snap_follows, bl);
  ENCODE_FINISH(bl);
}

void cap_reconnect_t::encode_old(bufferlist& bl) const {
  ::encode(path, bl);
  capinfo.flock_len = flockbl.length();
  ::encode(capinfo, bl);
  ::encode_nohead(flockbl, bl);
}

void cap_reconnect_t::decode(bufferlist::iterator& bl) {
  DECODE_START(1, bl);
  decode_old(bl); // extract out when something changes
  if (struct_v >= 2)
    ::decode(snap_follows, bl);
  DECODE_FINISH(bl);
}

void cap_reconnect_t::decode_old(bufferlist::iterator& bl) {
  ::decode(path, bl);
  ::decode(capinfo, bl);
  ::decode_nohead(capinfo.flock_len, flockbl, bl);
}

void cap_reconnect_t::dump(Formatter *f) const
{
  f->dump_string("path", path);
  f->dump_int("cap_id", capinfo.cap_id);
  f->dump_string("cap wanted", ccap_string(capinfo.wanted));
  f->dump_string("cap issued", ccap_string(capinfo.issued));
  f->dump_int("snaprealm", capinfo.snaprealm);
  f->dump_int("path base ino", capinfo.pathbase);
  f->dump_string("has file locks", capinfo.flock_len ? "true" : "false");
}

void cap_reconnect_t::generate_test_instances(list<cap_reconnect_t*>& ls)
{
  ls.push_back(new cap_reconnect_t);
  ls.back()->path = "/test/path";
  ls.back()->capinfo.cap_id = 1;
}

uint64_t MDSCacheObject::last_wait_seq = 0;

void MDSCacheObject::dump(Formatter *f) const
{
  f->dump_bool("is_auth", is_auth());

  // Fields only meaningful for auth
  f->open_object_section("auth_state");
  {
    f->open_object_section("replicas");
    const compact_map<mds_rank_t,unsigned>& replicas = get_replicas();
    for (compact_map<mds_rank_t,unsigned>::const_iterator i = replicas.begin();
         i != replicas.end(); ++i) {
      std::ostringstream rank_str;
      rank_str << i->first;
      f->dump_int(rank_str.str().c_str(), i->second);
    }
    f->close_section();
  }
  f->close_section(); // auth_state

  // Fields only meaningful for replica
  f->open_object_section("replica_state");
  {
    f->open_array_section("authority");
    f->dump_int("first", authority().first);
    f->dump_int("second", authority().second);
    f->close_section();
    f->dump_int("replica_nonce", get_replica_nonce());
  }
  f->close_section();  // replica_state

  f->dump_int("auth_pins", auth_pins);
  f->dump_int("nested_auth_pins", nested_auth_pins);
  f->dump_bool("is_frozen", is_frozen());
  f->dump_bool("is_freezing", is_freezing());

#ifdef MDS_REF_SET
    f->open_object_section("pins");
    for(std::map<int, int>::const_iterator it = ref_map.begin();
        it != ref_map.end(); ++it) {
      f->dump_int(pin_name(it->first), it->second);
    }
    f->close_section();
#endif
    f->dump_int("nref", ref);
}

/*
 * Use this in subclasses when printing their specialized
 * states too.
 */
void MDSCacheObject::dump_states(Formatter *f) const
{
  if (state_test(STATE_AUTH)) f->dump_string("state", "auth");
  if (state_test(STATE_DIRTY)) f->dump_string("state", "dirty");
  if (state_test(STATE_NOTIFYREF)) f->dump_string("state", "notifyref");
  if (state_test(STATE_REJOINING)) f->dump_string("state", "rejoining");
  if (state_test(STATE_REJOINUNDEF))
    f->dump_string("state", "rejoinundef");
}


ostream& operator<<(ostream &out, const mds_role_t &role)
{
  out << role.fscid << ":" << role.rank;
  return out;
}

