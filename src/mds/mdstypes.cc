// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "mdstypes.h"
#include "MDSContext.h"
#include "common/Formatter.h"
#include "common/StackStringStream.h"

const mds_gid_t MDS_GID_NONE = mds_gid_t(0);

using std::list;
using std::make_pair;
using std::ostream;
using std::set;
using std::vector;

using ceph::bufferlist;
using ceph::Formatter;

/*
 * frag_info_t
 */

void frag_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(3, 2, bl);
  encode(version, bl);
  encode(mtime, bl);
  encode(nfiles, bl);
  encode(nsubdirs, bl);
  encode(change_attr, bl);
  ENCODE_FINISH(bl);
}

void frag_info_t::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(version, bl);
  decode(mtime, bl);
  decode(nfiles, bl);
  decode(nsubdirs, bl);
  if (struct_v >= 3)
    decode(change_attr, bl);
  else
    change_attr = 0;
  DECODE_FINISH(bl);
}

void frag_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("num_files", nfiles);
  f->dump_unsigned("num_subdirs", nsubdirs);
  f->dump_unsigned("change_attr", change_attr);
}

void frag_info_t::decode_json(JSONObj *obj){

  JSONDecoder::decode_json("version", version, obj, true);
  JSONDecoder::decode_json("mtime", mtime, obj, true);
  JSONDecoder::decode_json("num_files", nfiles, obj, true);
  JSONDecoder::decode_json("num_subdirs", nsubdirs, obj, true);
  JSONDecoder::decode_json("change_attr", change_attr, obj, true);
}

void frag_info_t::generate_test_instances(std::list<frag_info_t*>& ls)
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
  encode(version, bl);
  encode(rbytes, bl);
  encode(rfiles, bl);
  encode(rsubdirs, bl);
  {
    // removed field
    int64_t ranchors = 0;
    encode(ranchors, bl);
  }
  encode(rsnaps, bl);
  encode(rctime, bl);
  ENCODE_FINISH(bl);
}

void nest_info_t::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(version, bl);
  decode(rbytes, bl);
  decode(rfiles, bl);
  decode(rsubdirs, bl);
  {
    int64_t ranchors;
    decode(ranchors, bl);
  }
  decode(rsnaps, bl);
  decode(rctime, bl);
  DECODE_FINISH(bl);
}

void nest_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_unsigned("rbytes", rbytes);
  f->dump_unsigned("rfiles", rfiles);
  f->dump_unsigned("rsubdirs", rsubdirs);
  f->dump_unsigned("rsnaps", rsnaps);
  f->dump_stream("rctime") << rctime;
}

void nest_info_t::decode_json(JSONObj *obj){

  JSONDecoder::decode_json("version", version, obj, true);
  JSONDecoder::decode_json("rbytes", rbytes, obj, true);
  JSONDecoder::decode_json("rfiles", rfiles, obj, true);
  JSONDecoder::decode_json("rsubdirs", rsubdirs, obj, true);
  JSONDecoder::decode_json("rsnaps", rsnaps, obj, true);
  JSONDecoder::decode_json("rctime", rctime, obj, true);
}

void nest_info_t::generate_test_instances(std::list<nest_info_t*>& ls)
{
  ls.push_back(new nest_info_t);
  ls.push_back(new nest_info_t);
  ls.back()->version = 1;
  ls.back()->rbytes = 2;
  ls.back()->rfiles = 3;
  ls.back()->rsubdirs = 4;
  ls.back()->rsnaps = 6;
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
  if (n.rsnaps)
    out << " rs" << n.rsnaps;
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

void  quota_info_t::decode_json(JSONObj *obj){

  JSONDecoder::decode_json("max_bytes", max_bytes, obj, true);
  JSONDecoder::decode_json("max_files", max_files, obj, true);
}

void quota_info_t::generate_test_instances(std::list<quota_info_t *>& ls)
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
  encode(range.first, bl);
  encode(range.last, bl);
  encode(follows, bl);
  ENCODE_FINISH(bl);
}

void client_writeable_range_t::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(range.first, bl);
  decode(range.last, bl);
  decode(follows, bl);
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

void client_writeable_range_t::byte_range_t::decode_json(JSONObj *obj){

  JSONDecoder::decode_json("first", first, obj, true);
  JSONDecoder::decode_json("last", last, obj, true);
}

void client_writeable_range_t::generate_test_instances(std::list<client_writeable_range_t*>& ls)
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
  using ceph::encode;
  encode(version, bl);
  if (blp)
    encode(*blp, bl);
  else
    encode(bufferlist(), bl);
}
void inline_data_t::decode(bufferlist::const_iterator &p)
{
  using ceph::decode;
  decode(version, p);
  uint32_t inline_len;
  decode(inline_len, p);
  if (inline_len > 0) {
    ceph::buffer::list bl;
    decode_nohead(inline_len, bl, p);
    set_data(bl);
  } else
    free_data();
}


/*
 * fnode_t
 */
void fnode_t::encode(bufferlist &bl) const
{
  ENCODE_START(4, 3, bl);
  encode(version, bl);
  encode(snap_purged_thru, bl);
  encode(fragstat, bl);
  encode(accounted_fragstat, bl);
  encode(rstat, bl);
  encode(accounted_rstat, bl);
  encode(damage_flags, bl);
  encode(recursive_scrub_version, bl);
  encode(recursive_scrub_stamp, bl);
  encode(localized_scrub_version, bl);
  encode(localized_scrub_stamp, bl);
  ENCODE_FINISH(bl);
}

void fnode_t::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(version, bl);
  decode(snap_purged_thru, bl);
  decode(fragstat, bl);
  decode(accounted_fragstat, bl);
  decode(rstat, bl);
  decode(accounted_rstat, bl);
  if (struct_v >= 3) {
    decode(damage_flags, bl);
  }
  if (struct_v >= 4) {
    decode(recursive_scrub_version, bl);
    decode(recursive_scrub_stamp, bl);
    decode(localized_scrub_version, bl);
    decode(localized_scrub_stamp, bl);
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
void fnode_t::decode_json(JSONObj *obj){
  JSONDecoder::decode_json("version", version, obj, true);
  uint64_t tmp;
  JSONDecoder::decode_json("snap_purged_thru", tmp, obj, true);
  snap_purged_thru.val = tmp;
  JSONDecoder::decode_json("fragstat", fragstat, obj, true);
  JSONDecoder::decode_json("accounted_fragstat", accounted_fragstat, obj, true);
  JSONDecoder::decode_json("rstat", rstat, obj, true);
  JSONDecoder::decode_json("accounted_rstat", accounted_rstat, obj, true);
}
void fnode_t::generate_test_instances(std::list<fnode_t*>& ls)
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
  encode(first, bl);
  encode(rstat, bl);
  encode(accounted_rstat, bl);
  ENCODE_FINISH(bl);
}

void old_rstat_t::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(first, bl);
  decode(rstat, bl);
  decode(accounted_rstat, bl);
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

void old_rstat_t::generate_test_instances(std::list<old_rstat_t*>& ls)
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
 * feature_bitset_t
 */
feature_bitset_t::feature_bitset_t(unsigned long value)
{
  if (value) {
    for (size_t i = 0; i < sizeof(value) * 8; i += bits_per_block) {
      _vec.push_back((block_type)(value >> i));
    }
  }
}

feature_bitset_t::feature_bitset_t(const vector<size_t>& array)
{
  if (!array.empty()) {
    size_t n = array.back();
    n += bits_per_block;
    n /= bits_per_block;
    _vec.resize(n, 0);

    size_t last = 0;
    for (auto& bit : array) {
      if (bit > last)
	last = bit;
      else
	ceph_assert(bit == last);
      _vec[bit / bits_per_block] |= (block_type)1 << (bit % bits_per_block);
    }
  }
}

feature_bitset_t& feature_bitset_t::operator-=(const feature_bitset_t& other)
{
  for (size_t i = 0; i < _vec.size(); ++i) {
    if (i >= other._vec.size())
      break;
    _vec[i] &= ~other._vec[i];
  }
  return *this;
}

void feature_bitset_t::encode(bufferlist& bl) const {
  using ceph::encode;
  using ceph::encode_nohead;
  uint32_t len = _vec.size() * sizeof(block_type);
  encode(len, bl);
  encode_nohead(_vec, bl);
}

void feature_bitset_t::decode(bufferlist::const_iterator &p) {
  using ceph::decode;
  using ceph::decode_nohead;
  uint32_t len;
  decode(len, p);

  _vec.clear();
  if (len >= sizeof(block_type))
    decode_nohead(len / sizeof(block_type), _vec, p);

  if (len % sizeof(block_type)) {
    ceph_le64 buf{};
    p.copy(len % sizeof(block_type), (char*)&buf);
    _vec.push_back((block_type)buf);
  }
}

void feature_bitset_t::dump(Formatter *f) const {
  CachedStackStringStream css;
  print(*css);
  f->dump_string("feature_bits", css->strv());
}

void feature_bitset_t::print(ostream& out) const
{
  std::ios_base::fmtflags f(out.flags());
  out << "0x";
  for (int i = _vec.size() - 1; i >= 0; --i)
    out << std::setfill('0') << std::setw(sizeof(block_type) * 2)
        << std::hex << _vec[i];
  out.flags(f);
}

/*
 * metric_spec_t
 */
void metric_spec_t::encode(bufferlist& bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(metric_flags, bl);
  ENCODE_FINISH(bl);
}

void metric_spec_t::decode(bufferlist::const_iterator &p) {
  using ceph::decode;
  DECODE_START(1, p);
  decode(metric_flags, p);
  DECODE_FINISH(p);
}

void metric_spec_t::dump(Formatter *f) const {
  f->dump_object("metric_flags", metric_flags);
}

void metric_spec_t::print(ostream& out) const
{
  out << "{metric_flags: '" << metric_flags << "'}";
}

/*
 * client_metadata_t
 */
void client_metadata_t::encode(bufferlist& bl) const
{
  ENCODE_START(3, 1, bl);
  encode(kv_map, bl);
  encode(features, bl);
  encode(metric_spec, bl);
  ENCODE_FINISH(bl);
}

void client_metadata_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START(3, p);
  decode(kv_map, p);
  if (struct_v >= 2)
    decode(features, p);
  if (struct_v >= 3) {
    decode(metric_spec, p);
  }
  DECODE_FINISH(p);
}

void client_metadata_t::dump(Formatter *f) const
{
  f->dump_object("client_features", features);
  f->dump_object("metric_spec", metric_spec);
  for (const auto& [name, val] : kv_map)
    f->dump_string(name.c_str(), val);
}

/*
 * session_info_t
 */
void session_info_t::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(7, 7, bl);
  encode(inst, bl, features);
  encode(completed_requests, bl);
  encode(prealloc_inos, bl);   // hacky, see below.
  encode(used_inos, bl);
  encode(completed_flushes, bl);
  encode(auth_name, bl);
  encode(client_metadata, bl);
  ENCODE_FINISH(bl);
}

void session_info_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 2, 2, p);
  decode(inst, p);
  if (struct_v <= 2) {
    set<ceph_tid_t> s;
    decode(s, p);
    while (!s.empty()) {
      completed_requests[*s.begin()] = inodeno_t();
      s.erase(s.begin());
    }
  } else {
    decode(completed_requests, p);
  }
  decode(prealloc_inos, p);
  decode(used_inos, p);
  prealloc_inos.insert(used_inos);
  used_inos.clear();
  if (struct_v >= 4 && struct_v < 7) {
    decode(client_metadata.kv_map, p);
  }
  if (struct_v >= 5) {
    decode(completed_flushes, p);
  }
  if (struct_v >= 6) {
    decode(auth_name, p);
  }
  if (struct_v >= 7) {
    decode(client_metadata, p);
  }
  DECODE_FINISH(p);
}

void session_info_t::dump(Formatter *f) const
{
  f->dump_stream("inst") << inst;

  f->open_array_section("completed_requests");
  for (const auto& [tid, ino] : completed_requests) {
    f->open_object_section("request");
    f->dump_unsigned("tid", tid);
    f->dump_stream("created_ino") << ino;
    f->close_section();
  }
  f->close_section();

  f->open_array_section("prealloc_inos");
  for (const auto& [start, len] : prealloc_inos) {
    f->open_object_section("ino_range");
    f->dump_stream("start") << start;
    f->dump_unsigned("length", len);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("used_inos");
  for (const auto& [start, len] : used_inos) {
    f->open_object_section("ino_range");
    f->dump_stream("start") << start;
    f->dump_unsigned("length", len);
    f->close_section();
  }
  f->close_section();

  f->dump_object("client_metadata", client_metadata);
}

void session_info_t::generate_test_instances(std::list<session_info_t*>& ls)
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
  encode(name, bl);
  encode(snapid, bl);
  ENCODE_FINISH(bl);
}

void string_snap_t::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(name, bl);
  decode(snapid, bl);
  DECODE_FINISH(bl);
}

void string_snap_t::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_unsigned("snapid", snapid);
}

void string_snap_t::generate_test_instances(std::list<string_snap_t*>& ls)
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
  encode(ino, bl);
  encode(dirfrag, bl);
  encode(dname, bl);
  encode(snapid, bl);
  ENCODE_FINISH(bl);
}

void MDSCacheObjectInfo::decode(bufferlist::const_iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  decode(ino, p);
  decode(dirfrag, p);
  decode(dname, p);
  decode(snapid, p);
  DECODE_FINISH(p);
}

void MDSCacheObjectInfo::dump(Formatter *f) const
{
  f->dump_unsigned("ino", ino);
  f->dump_stream("dirfrag") << dirfrag;
  f->dump_string("name", dname);
  f->dump_unsigned("snapid", snapid);
}

void MDSCacheObjectInfo::generate_test_instances(std::list<MDSCacheObjectInfo*>& ls)
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
  encode(reqid, bl);
  encode(mds, bl);
  encode(tid, bl);
  ENCODE_FINISH(bl);
}

void mds_table_pending_t::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(reqid, bl);
  decode(mds, bl);
  decode(tid, bl);
  DECODE_FINISH(bl);
}

void mds_table_pending_t::dump(Formatter *f) const
{
  f->dump_unsigned("reqid", reqid);
  f->dump_unsigned("mds", mds);
  f->dump_unsigned("tid", tid);
}

void mds_table_pending_t::generate_test_instances(std::list<mds_table_pending_t*>& ls)
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
  for (const auto &i : vec) {
    encode(i, bl);
  }
  ENCODE_FINISH(bl);
}

void inode_load_vec_t::decode(bufferlist::const_iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  for (auto &i : vec) {
    decode(i, p);
  }
  DECODE_FINISH(p);
}

void inode_load_vec_t::dump(Formatter *f) const
{
  f->open_array_section("Decay Counters");
  for (const auto &i : vec) {
    f->open_object_section("Decay Counter");
    i.dump(f);
    f->close_section();
  }
  f->close_section();
}

void inode_load_vec_t::generate_test_instances(std::list<inode_load_vec_t*>& ls)
{
  ls.push_back(new inode_load_vec_t(DecayRate()));
}


/*
 * dirfrag_load_vec_t
 */
void dirfrag_load_vec_t::dump(Formatter *f) const
{
  f->open_array_section("Decay Counters");
  for (const auto &i : vec) {
    f->open_object_section("Decay Counter");
    i.dump(f);
    f->close_section();
  }
  f->close_section();
}

void dirfrag_load_vec_t::dump(Formatter *f, const DecayRate& rate) const
{
  f->dump_float("meta_load", meta_load());
  f->dump_float("IRD", get(META_POP_IRD).get());
  f->dump_float("IWR", get(META_POP_IWR).get());
  f->dump_float("READDIR", get(META_POP_READDIR).get());
  f->dump_float("FETCH", get(META_POP_FETCH).get());
  f->dump_float("STORE", get(META_POP_STORE).get());
}

void dirfrag_load_vec_t::generate_test_instances(std::list<dirfrag_load_vec_t*>& ls)
{
  ls.push_back(new dirfrag_load_vec_t(DecayRate()));
}

/*
 * mds_load_t
 */
void mds_load_t::encode(bufferlist &bl) const {
  ENCODE_START(2, 2, bl);
  encode(auth, bl);
  encode(all, bl);
  encode(req_rate, bl);
  encode(cache_hit_rate, bl);
  encode(queue_len, bl);
  encode(cpu_load_avg, bl);
  ENCODE_FINISH(bl);
}

void mds_load_t::decode(bufferlist::const_iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(auth, bl);
  decode(all, bl);
  decode(req_rate, bl);
  decode(cache_hit_rate, bl);
  decode(queue_len, bl);
  decode(cpu_load_avg, bl);
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

void mds_load_t::generate_test_instances(std::list<mds_load_t*>& ls)
{
  ls.push_back(new mds_load_t(DecayRate()));
}

/*
 * cap_reconnect_t
 */
void cap_reconnect_t::encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);
  encode_old(bl); // extract out when something changes
  encode(snap_follows, bl);
  ENCODE_FINISH(bl);
}

void cap_reconnect_t::encode_old(bufferlist& bl) const {
  using ceph::encode;
  encode(path, bl);
  capinfo.flock_len = flockbl.length();
  encode(capinfo, bl);
  ceph::encode_nohead(flockbl, bl);
}

void cap_reconnect_t::decode(bufferlist::const_iterator& bl) {
  DECODE_START(2, bl);
  decode_old(bl); // extract out when something changes
  if (struct_v >= 2)
    decode(snap_follows, bl);
  DECODE_FINISH(bl);
}

void cap_reconnect_t::decode_old(bufferlist::const_iterator& bl) {
  using ceph::decode;
  decode(path, bl);
  decode(capinfo, bl);
  ceph::decode_nohead(capinfo.flock_len, flockbl, bl);
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

void cap_reconnect_t::generate_test_instances(std::list<cap_reconnect_t*>& ls)
{
  ls.push_back(new cap_reconnect_t);
  ls.back()->path = "/test/path";
  ls.back()->capinfo.cap_id = 1;
}

/*
 * snaprealm_reconnect_t
 */
void snaprealm_reconnect_t::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode_old(bl); // extract out when something changes
  ENCODE_FINISH(bl);
}

void snaprealm_reconnect_t::encode_old(bufferlist& bl) const {
  using ceph::encode;
  encode(realm, bl);
}

void snaprealm_reconnect_t::decode(bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  decode_old(bl); // extract out when something changes
  DECODE_FINISH(bl);
}

void snaprealm_reconnect_t::decode_old(bufferlist::const_iterator& bl) {
  using ceph::decode;
  decode(realm, bl);
}

void snaprealm_reconnect_t::dump(Formatter *f) const
{
  f->dump_int("ino", realm.ino);
  f->dump_int("seq", realm.seq);
  f->dump_int("parent", realm.parent);
}

void snaprealm_reconnect_t::generate_test_instances(std::list<snaprealm_reconnect_t*>& ls)
{
  ls.push_back(new snaprealm_reconnect_t);
  ls.back()->realm.ino = 0x10000000001ULL;
  ls.back()->realm.seq = 2;
  ls.back()->realm.parent = 1;
}
