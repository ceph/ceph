#include "ESession.h"
#include "ESessions.h"
#include "ESubtreeMap.h"

#include "EMetaBlob.h"
#include "ENoOp.h"
#include "EResetJournal.h"

#include "ECommitted.h"
#include "EOpen.h"
#include "EPeerUpdate.h"
#include "EPurged.h"
#include "EUpdate.h"

#include "EExport.h"
#include "EFragment.h"
#include "EImportFinish.h"
#include "EImportStart.h"

#include "ELid.h"
#include "ESegment.h"
#include "ETableClient.h"
#include "ETableServer.h"

#include "include/stringify.h"

using std::list;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::vector;

void EMetaBlob::fullbit::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(9, 5, bl);
  encode(dn, bl);
  encode(dnfirst, bl);
  encode(dnlast, bl);
  encode(dnv, bl);
  encode(*inode, bl, features);
  if (xattrs)
    encode(*xattrs, bl);
  else
    encode((__u32)0, bl);

  if (inode->is_symlink())
    encode(symlink, bl);
  if (inode->is_dir()) {
    encode(dirfragtree, bl);
    encode(snapbl, bl);
  }
  encode(state, bl);
  if (!old_inodes || old_inodes->empty()) {
    encode(false, bl);
  } else {
    encode(true, bl);
    encode(*old_inodes, bl, features);
  }
  if (!inode->is_dir())
    encode(snapbl, bl);
  encode(oldest_snap, bl);
  encode(alternate_name, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::fullbit::decode(bufferlist::const_iterator& bl)
{
  DECODE_START(9, bl);
  decode(dn, bl);
  decode(dnfirst, bl);
  decode(dnlast, bl);
  decode(dnv, bl);
  {
    auto _inode = CInode::allocate_inode();
    decode(*_inode, bl);
    inode = std::move(_inode);
  }
  {
    CInode::mempool_xattr_map tmp;
    decode_noshare(tmp, bl);
    if (!tmp.empty())
      xattrs = CInode::allocate_xattr_map(std::move(tmp));
  }
  if (inode->is_symlink())
    decode(symlink, bl);
  if (inode->is_dir()) {
    decode(dirfragtree, bl);
    decode(snapbl, bl);
  }
  decode(state, bl);
  bool old_inodes_present;
  decode(old_inodes_present, bl);
  if (old_inodes_present) {
    auto _old_inodes = CInode::allocate_old_inode_map();
    decode(*_old_inodes, bl);
    old_inodes = std::move(_old_inodes);
  }
  if (!inode->is_dir()) {
    decode(snapbl, bl);
  }
  decode(oldest_snap, bl);
  if (struct_v >= 9) {
    decode(alternate_name, bl);
  }
  DECODE_FINISH(bl);
}

void EMetaBlob::fullbit::dump(Formatter* f) const
{
  f->dump_string("dentry", dn);
  f->dump_stream("snapid.first") << dnfirst;
  f->dump_stream("snapid.last") << dnlast;
  f->dump_int("dentry version", dnv);
  f->open_object_section("inode");
  inode->dump(f);
  f->close_section(); // inode
  f->open_object_section("xattrs");
  if (xattrs) {
    for (const auto& p : *xattrs) {
      std::string s(p.second.c_str(), p.second.length());
      f->dump_string(p.first.c_str(), s);
    }
  }
  f->close_section(); // xattrs
  if (inode->is_symlink()) {
    f->dump_string("symlink", symlink);
  }
  if (inode->is_dir()) {
    f->dump_stream("frag tree") << dirfragtree;
    f->dump_string("has_snapbl", snapbl.length() ? "true" : "false");
    if (inode->has_layout()) {
      f->open_object_section("file layout policy");
      // FIXME
      f->dump_string("layout", "the layout exists");
      f->close_section(); // file layout policy
    }
  }
  f->dump_string("state", state_string());
  if (old_inodes && !old_inodes->empty()) {
    f->open_array_section("old inodes");
    for (const auto& p : *old_inodes) {
      f->open_object_section("inode");
      f->dump_int("snapid", p.first);
      p.second.dump(f);
      f->close_section(); // inode
    }
    f->close_section(); // old inodes
  }
  f->dump_string("alternate_name", alternate_name);
}


void EMetaBlob::remotebit::encode(bufferlist& bl) const
{
  ENCODE_START(3, 2, bl);
  encode(dn, bl);
  encode(dnfirst, bl);
  encode(dnlast, bl);
  encode(dnv, bl);
  encode(ino, bl);
  encode(d_type, bl);
  encode(dirty, bl);
  encode(alternate_name, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::remotebit::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(dn, bl);
  decode(dnfirst, bl);
  decode(dnlast, bl);
  decode(dnv, bl);
  decode(ino, bl);
  decode(d_type, bl);
  decode(dirty, bl);
  if (struct_v >= 3)
    decode(alternate_name, bl);
  DECODE_FINISH(bl);
}

void EMetaBlob::remotebit::dump(Formatter *f) const
{
  f->dump_string("dentry", dn);
  f->dump_int("snapid.first", dnfirst);
  f->dump_int("snapid.last", dnlast);
  f->dump_int("dentry version", dnv);
  f->dump_int("inodeno", ino);
  uint32_t type = DTTOIF(d_type) & S_IFMT; // convert to type entries
  string type_string;
  switch(type) {
  case S_IFREG:
    type_string = "file"; break;
  case S_IFLNK:
    type_string = "symlink"; break;
  case S_IFDIR:
    type_string = "directory"; break;
  case S_IFIFO:
    type_string = "fifo"; break;
  case S_IFCHR:
    type_string = "chr"; break;
  case S_IFBLK:
    type_string = "blk"; break;
  case S_IFSOCK:
    type_string = "sock"; break;
  default:
    assert (0 == "unknown d_type!");
  }
  f->dump_string("d_type", type_string);
  f->dump_string("dirty", dirty ? "true" : "false");
  f->dump_string("alternate_name", alternate_name);
}

void EMetaBlob::nullbit::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  encode(dn, bl);
  encode(dnfirst, bl);
  encode(dnlast, bl);
  encode(dnv, bl);
  encode(dirty, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::nullbit::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(dn, bl);
  decode(dnfirst, bl);
  decode(dnlast, bl);
  decode(dnv, bl);
  decode(dirty, bl);
  DECODE_FINISH(bl);
}

void EMetaBlob::nullbit::dump(Formatter *f) const
{
  f->dump_string("dentry", dn);
  f->dump_int("snapid.first", dnfirst);
  f->dump_int("snapid.last", dnlast);
  f->dump_int("dentry version", dnv);
  f->dump_string("dirty", dirty ? "true" : "false");
}

void EMetaBlob::dirlump::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  encode(*fnode, bl);
  encode(state, bl);
  encode(nfull, bl);
  encode(nremote, bl);
  encode(nnull, bl);
  _encode_bits(features);
  encode(dnbl, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::dirlump::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl)
  {
    auto _fnode = CDir::allocate_fnode();
    decode(*_fnode, bl);
    fnode = std::move(_fnode);
  }
  decode(state, bl);
  decode(nfull, bl);
  decode(nremote, bl);
  decode(nnull, bl);
  decode(dnbl, bl);
  dn_decoded = false;      // don't decode bits unless we need them.
  DECODE_FINISH(bl);
}

void EMetaBlob::dirlump::dump(Formatter *f) const
{
  if (!dn_decoded) {
    dirlump *me = const_cast<dirlump*>(this);
    me->_decode_bits();
  }
  f->open_object_section("fnode");
  fnode->dump(f);
  f->close_section(); // fnode
  f->dump_string("state", state_string());
  f->dump_int("nfull", nfull);
  f->dump_int("nremote", nremote);
  f->dump_int("nnull", nnull);

  f->open_array_section("full bits");
  for (const auto& iter : dfull) {
    f->open_object_section("fullbit");
    iter.dump(f);
    f->close_section(); // fullbit
  }
  f->close_section(); // full bits
  f->open_array_section("remote bits");
  for (const auto& iter : dremote) {
    f->open_object_section("remotebit");
    iter.dump(f);
    f->close_section(); // remotebit
  }
  f->close_section(); // remote bits
  f->open_array_section("null bits");
  for (const auto& iter : dnull) {
    f->open_object_section("null bit");
    iter.dump(f);
    f->close_section(); // null bit
  }
  f->close_section(); // null bits
}

void EMetaBlob::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(8, 5, bl);
  encode(lump_order, bl);
  encode(lump_map, bl, features);
  encode(roots, bl, features);
  encode(table_tids, bl);
  encode(opened_ino, bl);
  encode(allocated_ino, bl);
  encode(used_preallocated_ino, bl);
  encode(preallocated_inos, bl);
  encode(client_name, bl);
  encode(inotablev, bl);
  encode(sessionmapv, bl);
  encode(truncate_start, bl);
  encode(truncate_finish, bl);
  encode(destroyed_inodes, bl);
  encode(client_reqs, bl);
  encode(renamed_dirino, bl);
  encode(renamed_dir_frags, bl);
  {
    // make MDSRank use v6 format happy
    int64_t i = -1;
    bool b = false;
    encode(i, bl);
    encode(b, bl);
  }
  encode(client_flushes, bl);
  ENCODE_FINISH(bl);
}
void EMetaBlob::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(8, 5, 5, bl);
  decode(lump_order, bl);
  decode(lump_map, bl);
  if (struct_v >= 4) {
    decode(roots, bl);
  } else {
    bufferlist rootbl;
    decode(rootbl, bl);
    if (rootbl.length()) {
      auto p = rootbl.cbegin();
      roots.emplace_back(p);
    }
  }
  decode(table_tids, bl);
  decode(opened_ino, bl);
  decode(allocated_ino, bl);
  decode(used_preallocated_ino, bl);
  decode(preallocated_inos, bl);
  decode(client_name, bl);
  decode(inotablev, bl);
  decode(sessionmapv, bl);
  decode(truncate_start, bl);
  decode(truncate_finish, bl);
  decode(destroyed_inodes, bl);
  if (struct_v >= 2) {
    decode(client_reqs, bl);
  } else {
    list<metareqid_t> r;
    decode(r, bl);
    while (!r.empty()) {
	client_reqs.push_back(pair<metareqid_t,uint64_t>(r.front(), 0));
	r.pop_front();
    }
  }
  if (struct_v >= 3) {
    decode(renamed_dirino, bl);
    decode(renamed_dir_frags, bl);
  }
  if (struct_v >= 6) {
    // ignore
    int64_t i;
    bool b;
    decode(i, bl);
    decode(b, bl);
  }
  if (struct_v >= 8) {
    decode(client_flushes, bl);
  }
  DECODE_FINISH(bl);
}

void EMetaBlob::dump(Formatter *f) const
{
  f->open_array_section("lumps");
  for (const auto& d : lump_order) {
    f->open_object_section("lump");
    f->open_object_section("dirfrag");
    f->dump_stream("dirfrag") << d;
    f->close_section(); // dirfrag
    f->open_object_section("dirlump");
    lump_map.at(d).dump(f);
    f->close_section(); // dirlump
    f->close_section(); // lump
  }
  f->close_section(); // lumps
  
  f->open_array_section("roots");
  for (const auto& iter : roots) {
    f->open_object_section("root");
    iter.dump(f);
    f->close_section(); // root
  }
  f->close_section(); // roots

  f->open_array_section("tableclient tranactions");
  for (const auto& p : table_tids) {
    f->open_object_section("transaction");
    f->dump_int("tid", p.first);
    f->dump_int("version", p.second);
    f->close_section(); // transaction
  }
  f->close_section(); // tableclient transactions
  
  f->dump_int("renamed directory inodeno", renamed_dirino);
  
  f->open_array_section("renamed directory fragments");
  for (const auto& p : renamed_dir_frags) {
    f->dump_int("frag", p);
  }
  f->close_section(); // renamed directory fragments

  f->dump_int("inotable version", inotablev);
  f->dump_int("SessionMap version", sessionmapv);
  f->dump_int("allocated ino", allocated_ino);
  
  f->dump_stream("preallocated inos") << preallocated_inos;
  f->dump_int("used preallocated ino", used_preallocated_ino);

  f->open_object_section("client name");
  client_name.dump(f);
  f->close_section(); // client name

  f->open_array_section("inodes starting a truncate");
  for(const auto& ino : truncate_start) {
    f->dump_int("inodeno", ino);
  }
  f->close_section(); // truncate inodes
  f->open_array_section("inodes finishing a truncated");
  for(const auto& p : truncate_finish) {
    f->open_object_section("inode+segment");
    f->dump_int("inodeno", p.first);
    f->dump_int("truncate starting segment", p.second);
    f->close_section(); // truncated inode
  }
  f->close_section(); // truncate finish inodes

  f->open_array_section("destroyed inodes");
  for(vector<inodeno_t>::const_iterator i = destroyed_inodes.begin();
      i != destroyed_inodes.end(); ++i) {
    f->dump_int("inodeno", *i);
  }
  f->close_section(); // destroyed inodes

  f->open_array_section("client requests");
  for(const auto& p : client_reqs) {
    f->open_object_section("Client request");
    f->dump_stream("request ID") << p.first;
    f->dump_int("oldest request on client", p.second);
    f->close_section(); // request
  }
  f->close_section(); // client requests
}


void EPurged::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(inos, bl);
  encode(inotablev, bl);
  encode(seq, bl);
  ENCODE_FINISH(bl);
}

void EPurged::decode(bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(inos, bl);
  decode(inotablev, bl);
  decode(seq, bl);
  DECODE_FINISH(bl);
}

void EPurged::dump(Formatter *f) const
{
  f->dump_stream("inos") << inos;
  f->dump_int("inotable version", inotablev);
  f->dump_int("segment seq", seq);
}

void ESession::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(6, 5, bl);
  encode(stamp, bl);
  encode(client_inst, bl, features);
  encode(open, bl);
  encode(cmapv, bl);
  encode(inos_to_free, bl);
  encode(inotablev, bl);
  encode(client_metadata, bl);
  encode(inos_to_purge, bl);
  ENCODE_FINISH(bl);
}

void ESession::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(client_inst, bl);
  decode(open, bl);
  decode(cmapv, bl);
  decode(inos_to_free, bl);
  decode(inotablev, bl);
  if (struct_v == 4) {
    decode(client_metadata.kv_map, bl);
  } else if (struct_v >= 5) {
    decode(client_metadata, bl);
  }
  if (struct_v >= 6){
    decode(inos_to_purge, bl);
  }
    
  DECODE_FINISH(bl);
}

void ESession::dump(Formatter *f) const
{
  f->dump_stream("client instance") << client_inst;
  f->dump_string("open", open ? "true" : "false");
  f->dump_int("client map version", cmapv);
  f->dump_stream("inos_to_free") << inos_to_free;
  f->dump_int("inotable version", inotablev);
  f->open_object_section("client_metadata");
  f->dump_stream("inos_to_purge") << inos_to_purge;
  client_metadata.dump(f);
  f->close_section();  // client_metadata
}

void ESessions::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(2, 1, bl);
  encode(client_map, bl, features);
  encode(cmapv, bl);
  encode(stamp, bl);
  encode(client_metadata_map, bl);
  ENCODE_FINISH(bl);
}

void ESessions::decode_old(bufferlist::const_iterator &bl)
{
  using ceph::decode;
  decode(client_map, bl);
  decode(cmapv, bl);
  if (!bl.end())
    decode(stamp, bl);
}

void ESessions::decode_new(bufferlist::const_iterator &bl)
{
  DECODE_START(2, bl);
  decode(client_map, bl);
  decode(cmapv, bl);
  decode(stamp, bl);
  if (struct_v >= 2)
    decode(client_metadata_map, bl);
  DECODE_FINISH(bl);
}

void ESessions::dump(Formatter *f) const
{
  f->dump_int("client map version", cmapv);

  f->open_array_section("client map");
  for (map<client_t,entity_inst_t>::const_iterator i = client_map.begin();
       i != client_map.end(); ++i) {
    f->open_object_section("client");
    f->dump_int("client id", i->first.v);
    f->dump_stream("client entity") << i->second;
    f->close_section(); // client
  }
  f->close_section(); // client map
}

void ETableServer::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(table, bl);
  encode(op, bl);
  encode(reqid, bl);
  encode(bymds, bl);
  encode(mutation, bl);
  encode(tid, bl);
  encode(version, bl);
  ENCODE_FINISH(bl);
}

void ETableServer::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(table, bl);
  decode(op, bl);
  decode(reqid, bl);
  decode(bymds, bl);
  decode(mutation, bl);
  decode(tid, bl);
  decode(version, bl);
  DECODE_FINISH(bl);
}

void ETableServer::dump(Formatter *f) const
{
  f->dump_int("table id", table);
  f->dump_int("op", op);
  f->dump_int("request id", reqid);
  f->dump_int("by mds", bymds);
  f->dump_int("tid", tid);
  f->dump_int("version", version);
}

void ETableClient::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(table, bl);
  encode(op, bl);
  encode(tid, bl);
  ENCODE_FINISH(bl);
}

void ETableClient::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(table, bl);
  decode(op, bl);
  decode(tid, bl);
  DECODE_FINISH(bl);
}

void ETableClient::dump(Formatter *f) const
{
  f->dump_int("table", table);
  f->dump_int("op", op);
  f->dump_int("tid", tid);
}

void EUpdate::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(4, 4, bl);
  encode(stamp, bl);
  encode(type, bl);
  encode(metablob, bl, features);
  encode(client_map, bl);
  encode(cmapv, bl);
  encode(reqid, bl);
  encode(had_peers, bl);
  ENCODE_FINISH(bl);
}
 
void EUpdate::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(type, bl);
  decode(metablob, bl);
  decode(client_map, bl);
  if (struct_v >= 3)
    decode(cmapv, bl);
  decode(reqid, bl);
  decode(had_peers, bl);
  DECODE_FINISH(bl);
}

void EUpdate::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob

  f->dump_string("type", type);
  f->dump_int("client map length", client_map.length());
  f->dump_int("client map version", cmapv);
  f->dump_stream("reqid") << reqid;
  f->dump_string("had peers", had_peers ? "true" : "false");
}

void EOpen::encode(bufferlist &bl, uint64_t features) const {
  ENCODE_START(4, 3, bl);
  encode(stamp, bl);
  encode(metablob, bl, features);
  encode(inos, bl);
  encode(snap_inos, bl);
  ENCODE_FINISH(bl);
} 

void EOpen::decode(bufferlist::const_iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(metablob, bl);
  decode(inos, bl);
  if (struct_v >= 4)
    decode(snap_inos, bl);
  DECODE_FINISH(bl);
}

void EOpen::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob
  f->open_array_section("inos involved");
  for (vector<inodeno_t>::const_iterator i = inos.begin();
       i != inos.end(); ++i) {
    f->dump_int("ino", *i);
  }
  f->close_section(); // inos
}

void ECommitted::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(reqid, bl);
  ENCODE_FINISH(bl);
} 

void ECommitted::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(reqid, bl);
  DECODE_FINISH(bl);
}

void ECommitted::dump(Formatter *f) const {
  f->dump_stream("stamp") << stamp;
  f->dump_stream("reqid") << reqid;
}

void link_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(3, 2, bl);
  encode(reqid, bl);
  encode(ino, bl);
  encode(was_inc, bl);
  encode(old_ctime, bl);
  encode(old_dir_mtime, bl);
  encode(old_dir_rctime, bl);
  encode(snapbl, bl);
  ENCODE_FINISH(bl);
}

void link_rollback::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(reqid, bl);
  decode(ino, bl);
  decode(was_inc, bl);
  decode(old_ctime, bl);
  decode(old_dir_mtime, bl);
  decode(old_dir_rctime, bl);
  if (struct_v >= 3)
    decode(snapbl, bl);
  DECODE_FINISH(bl);
}

void link_rollback::dump(Formatter *f) const
{
  f->dump_stream("metareqid") << reqid;
  f->dump_int("ino", ino);
  f->dump_string("was incremented", was_inc ? "true" : "false");
  f->dump_stream("old_ctime") << old_ctime;
  f->dump_stream("old_dir_mtime") << old_dir_mtime;
  f->dump_stream("old_dir_rctime") << old_dir_rctime;
}

void rmdir_rollback::encode(bufferlist& bl) const
{
  ENCODE_START(3, 2, bl);
  encode(reqid, bl);
  encode(src_dir, bl);
  encode(src_dname, bl);
  encode(dest_dir, bl);
  encode(dest_dname, bl);
  encode(snapbl, bl);
  ENCODE_FINISH(bl);
}

void rmdir_rollback::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(reqid, bl);
  decode(src_dir, bl);
  decode(src_dname, bl);
  decode(dest_dir, bl);
  decode(dest_dname, bl);
  if (struct_v >= 3)
    decode(snapbl, bl);
  DECODE_FINISH(bl);
}

void rmdir_rollback::dump(Formatter *f) const
{
  f->dump_stream("metareqid") << reqid;
  f->dump_stream("source directory") << src_dir;
  f->dump_string("source dname", src_dname);
  f->dump_stream("destination directory") << dest_dir;
  f->dump_string("destination dname", dest_dname);
}

void rename_rollback::drec::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  encode(dirfrag, bl);
  encode(dirfrag_old_mtime, bl);
  encode(dirfrag_old_rctime, bl);
  encode(ino, bl);
  encode(remote_ino, bl);
  encode(dname, bl);
  encode(remote_d_type, bl);
  encode(old_ctime, bl);
  ENCODE_FINISH(bl);
}

void rename_rollback::drec::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(dirfrag, bl);
  decode(dirfrag_old_mtime, bl);
  decode(dirfrag_old_rctime, bl);
  decode(ino, bl);
  decode(remote_ino, bl);
  decode(dname, bl);
  decode(remote_d_type, bl);
  decode(old_ctime, bl);
  DECODE_FINISH(bl);
}

void rename_rollback::drec::dump(Formatter *f) const
{
  f->dump_stream("directory fragment") << dirfrag;
  f->dump_stream("directory old mtime") << dirfrag_old_mtime;
  f->dump_stream("directory old rctime") << dirfrag_old_rctime;
  f->dump_int("ino", ino);
  f->dump_int("remote ino", remote_ino);
  f->dump_string("dname", dname);
  uint32_t type = DTTOIF(remote_d_type) & S_IFMT; // convert to type entries
  string type_string;
  switch(type) {
  case S_IFREG:
    type_string = "file"; break;
  case S_IFLNK:
    type_string = "symlink"; break;
  case S_IFDIR:
    type_string = "directory"; break;
  default:
    type_string = "UNKNOWN-" + stringify((int)type); break;
  }
  f->dump_string("remote dtype", type_string);
  f->dump_stream("old ctime") << old_ctime;
}

void rename_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(3, 2, bl);
  encode(reqid, bl);
  encode(orig_src, bl);
  encode(orig_dest, bl);
  encode(stray, bl);
  encode(ctime, bl);
  encode(srci_snapbl, bl);
  encode(desti_snapbl, bl);
  ENCODE_FINISH(bl);
}

void rename_rollback::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(reqid, bl);
  decode(orig_src, bl);
  decode(orig_dest, bl);
  decode(stray, bl);
  decode(ctime, bl);
  if (struct_v >= 3) {
    decode(srci_snapbl, bl);
    decode(desti_snapbl, bl);
  }
  DECODE_FINISH(bl);
}

void rename_rollback::dump(Formatter *f) const
{
  f->dump_stream("request id") << reqid;
  f->open_object_section("original src drec");
  orig_src.dump(f);
  f->close_section(); // original src drec
  f->open_object_section("original dest drec");
  orig_dest.dump(f);
  f->close_section(); // original dest drec
  f->open_object_section("stray drec");
  stray.dump(f);
  f->close_section(); // stray drec
  f->dump_stream("ctime") << ctime;
}

void EPeerUpdate::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(type, bl);
  encode(reqid, bl);
  encode(leader, bl);
  encode(op, bl);
  encode(origop, bl);
  encode(commit, bl, features);
  encode(rollback, bl);
  ENCODE_FINISH(bl);
} 

void EPeerUpdate::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(type, bl);
  decode(reqid, bl);
  decode(leader, bl);
  decode(op, bl);
  decode(origop, bl);
  decode(commit, bl);
  decode(rollback, bl);
  DECODE_FINISH(bl);
}

void EPeerUpdate::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  commit.dump(f);
  f->close_section(); // metablob

  f->dump_int("rollback length", rollback.length());
  f->dump_string("type", type);
  f->dump_stream("metareqid") << reqid;
  f->dump_int("leader", leader);
  f->dump_int("op", op);
  f->dump_int("original op", origop);
}

void ESubtreeMap::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(6, 5, bl);
  encode(stamp, bl);
  encode(metablob, bl, features);
  encode(subtrees, bl);
  encode(ambiguous_subtrees, bl);
  encode(expire_pos, bl);
  encode(seq, bl);
  ENCODE_FINISH(bl);
}
 
void ESubtreeMap::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 5, 5, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(metablob, bl);
  decode(subtrees, bl);
  if (struct_v >= 4)
    decode(ambiguous_subtrees, bl);
  if (struct_v >= 3)
    decode(expire_pos, bl);
  if (struct_v >= 6)
    decode(seq, bl);
  DECODE_FINISH(bl);
}

void ESubtreeMap::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob
  
  f->open_array_section("subtrees");
  for(map<dirfrag_t,vector<dirfrag_t> >::const_iterator i = subtrees.begin();
      i != subtrees.end(); ++i) {
    f->open_object_section("tree");
    f->dump_stream("root dirfrag") << i->first;
    for (vector<dirfrag_t>::const_iterator j = i->second.begin();
	 j != i->second.end(); ++j) {
      f->dump_stream("bound dirfrag") << *j;
    }
    f->close_section(); // tree
  }
  f->close_section(); // subtrees

  f->open_array_section("ambiguous subtrees");
  for(set<dirfrag_t>::const_iterator i = ambiguous_subtrees.begin();
      i != ambiguous_subtrees.end(); ++i) {
    f->dump_stream("dirfrag") << *i;
  }
  f->close_section(); // ambiguous subtrees

  f->dump_int("expire position", expire_pos);
}

void EFragment::encode(bufferlist &bl, uint64_t features) const {
  ENCODE_START(5, 4, bl);
  encode(stamp, bl);
  encode(op, bl);
  encode(ino, bl);
  encode(basefrag, bl);
  encode(bits, bl);
  encode(metablob, bl, features);
  encode(orig_frags, bl);
  encode(rollback, bl);
  ENCODE_FINISH(bl);
}

void EFragment::decode(bufferlist::const_iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  if (struct_v >= 3)
    decode(op, bl);
  decode(ino, bl);
  decode(basefrag, bl);
  decode(bits, bl);
  decode(metablob, bl);
  if (struct_v >= 5) {
    decode(orig_frags, bl);
    decode(rollback, bl);
  }
  DECODE_FINISH(bl);
}

void EFragment::dump(Formatter *f) const
{
  /*f->open_object_section("Metablob");
  metablob.dump(f); // sadly we don't have this; dunno if we'll get it
  f->close_section();*/
  f->dump_string("op", op_name(op));
  f->dump_stream("ino") << ino;
  f->dump_stream("base frag") << basefrag;
  f->dump_int("bits", bits);
}


void dirfrag_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(*fnode, bl);
  ENCODE_FINISH(bl);
}

void dirfrag_rollback::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  {
    auto _fnode = CDir::allocate_fnode();
    decode(*_fnode, bl);
    fnode = std::move(_fnode);
  }
  DECODE_FINISH(bl);
}

void EExport::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(4, 3, bl);
  encode(stamp, bl);
  encode(metablob, bl, features);
  encode(base, bl);
  encode(bounds, bl);
  encode(target, bl);
  ENCODE_FINISH(bl);
}

void EExport::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(metablob, bl);
  decode(base, bl);
  decode(bounds, bl);
  if (struct_v >= 4)
    decode(target, bl);
  DECODE_FINISH(bl);
}

void EExport::dump(Formatter *f) const
{
  f->dump_float("stamp", (double)stamp);
  /*f->open_object_section("Metablob");
  metablob.dump(f); // sadly we don't have this; dunno if we'll get it
  f->close_section();*/
  f->dump_stream("base dirfrag") << base;
  f->open_array_section("bounds dirfrags");
  for (set<dirfrag_t>::const_iterator i = bounds.begin();
      i != bounds.end(); ++i) {
    f->dump_stream("dirfrag") << *i;
  }
  f->close_section(); // bounds dirfrags
}

void EImportStart::encode(bufferlist &bl, uint64_t features) const {
  ENCODE_START(4, 3, bl);
  encode(stamp, bl);
  encode(base, bl);
  encode(metablob, bl, features);
  encode(bounds, bl);
  encode(cmapv, bl);
  encode(client_map, bl);
  encode(from, bl);
  ENCODE_FINISH(bl);
}

void EImportStart::decode(bufferlist::const_iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(base, bl);
  decode(metablob, bl);
  decode(bounds, bl);
  decode(cmapv, bl);
  decode(client_map, bl);
  if (struct_v >= 4)
    decode(from, bl);
  DECODE_FINISH(bl);
}

void EImportStart::dump(Formatter *f) const
{
  f->dump_stream("base dirfrag") << base;
  f->open_array_section("boundary dirfrags");
  for (vector<dirfrag_t>::const_iterator iter = bounds.begin();
      iter != bounds.end(); ++iter) {
    f->dump_stream("frag") << *iter;
  }
  f->close_section();
}

void EImportFinish::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(3, 3, bl);
  encode(stamp, bl);
  encode(base, bl);
  encode(success, bl);
  ENCODE_FINISH(bl);
}

void EImportFinish::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    decode(stamp, bl);
  decode(base, bl);
  decode(success, bl);
  DECODE_FINISH(bl);
}

void EImportFinish::dump(Formatter *f) const
{
  f->dump_stream("base dirfrag") << base;
  f->dump_string("success", success ? "true" : "false");
}
void EImportFinish::generate_test_instances(std::list<EImportFinish*>& ls)
{
  ls.push_back(new EImportFinish);
  ls.push_back(new EImportFinish);
  ls.back()->success = true;
}

void EResetJournal::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  encode(stamp, bl);
  ENCODE_FINISH(bl);
}
 
void EResetJournal::decode(bufferlist::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(stamp, bl);
  DECODE_FINISH(bl);
}

void EResetJournal::dump(Formatter *f) const
{
  f->dump_stream("timestamp") << stamp;
}


void ESegment::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(seq, bl);
  ENCODE_FINISH(bl);
}

void ESegment::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(seq, bl);
  DECODE_FINISH(bl);
}

void ESegment::dump(Formatter *f) const
{
  f->dump_int("seq", seq);
}

void ELid::encode(bufferlist &bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(seq, bl);
  ENCODE_FINISH(bl);
}

void ELid::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(seq, bl);
  DECODE_FINISH(bl);
}

void ELid::dump(Formatter* f) const
{
  f->dump_int("seq", seq);
}

void ENoOp::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(2, 2, bl);
  encode(pad_size, bl);
  uint8_t const pad = 0xff;
  for (unsigned int i = 0; i < pad_size; ++i) {
    encode(pad, bl);
  }
  ENCODE_FINISH(bl);
}

void ENoOp::decode(bufferlist::const_iterator& bl)
{
  DECODE_START(2, bl);
  decode(pad_size, bl);
  if (bl.get_remaining() != pad_size) {
    // This is spiritually an assertion, but expressing in a way that will let
    // journal debug tools catch it and recognise a malformed entry.
    throw buffer::end_of_buffer();
  } else {
    bl += pad_size;
  }
  DECODE_FINISH(bl);
}