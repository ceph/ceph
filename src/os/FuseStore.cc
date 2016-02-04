// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FuseStore.h"
#include "include/stringify.h"
#include "common/errno.h"

#define FUSE_USE_VERSION 30
#include <fuse/fuse.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>           /* Definition of AT_* constants */
#include <sys/stat.h>

#define dout_subsys ceph_subsys_fuse
#include "common/debug.h"
#undef dout_prefix
#define dout_prefix *_dout << "fuse "

// some fuse-y bits of state
struct fs_info {
  struct fuse_args args;
  struct fuse *f;
  struct fuse_chan *ch;
  char *mountpoint;
};

FuseStore::FuseStore(ObjectStore *s, string p)
  : store(s),
    mount_point(p),
    fuse_thread(this)
{
  info = new fs_info;
}

FuseStore::~FuseStore()
{
  delete info;
}

/*
 * / - root directory
 * $cid/
 * $cid/type - objectstore type
 * $cid/bitwise_hash_start = lowest hash value
 * $cid/bitwise_hash_end = highest hash value
 * $cid/bitwise_hash_bits - how many bits are significant
 * $cid/pgmeta/ - pgmeta object
 * $cid/all/ - all objects
 * $cid/all/$obj/
 * $cid/all/$obj/bitwise_hash
 * $cid/all/$obj/data
 * $cid/all/$obj/omap/$key
 * $cid/all/$obj/attr/$name
 * $cid/by_bitwise_hash/$hash/$bits/$obj - all objects with this (bitwise) hash (prefix)
 */
enum {
  FN_ROOT = 1,
  FN_TYPE,
  FN_COLLECTION,
  FN_HASH_START,
  FN_HASH_END,
  FN_HASH_BITS,
  FN_OBJECT,
  FN_OBJECT_HASH,
  FN_OBJECT_DATA,
  FN_OBJECT_OMAP_HEADER,
  FN_OBJECT_OMAP,
  FN_OBJECT_OMAP_VAL,
  FN_OBJECT_ATTR,
  FN_OBJECT_ATTR_VAL,
  FN_ALL,
  FN_HASH_DIR,
  FN_HASH_VAL,
};

static int parse_fn(const char *path, coll_t *cid, ghobject_t *oid, string *key,
		    uint32_t *hash, uint32_t *hash_bits)
{
  list<string> v;
  for (const char *p = path; *p; ++p) {
    if (*p == '/')
      continue;
    const char *e;
    for (e = p + 1; *e && *e != '/'; e++) ;
    string c(p, e-p);
    v.push_back(c);
    p = e;
    if (!*p)
      break;
  }
  dout(10) << __func__ << " path " << path << " -> " << v << dendl;

  if (v.empty())
    return FN_ROOT;

  if (v.front() == "type")
    return FN_TYPE;

  if (!cid->parse(v.front())) {
    return -ENOENT;
  }
  if (v.size() == 1)
    return FN_COLLECTION;
  v.pop_front();

  if (v.front() == "bitwise_hash_start")
    return FN_HASH_START;
  if (v.front() == "bitwise_hash_end")
    return FN_HASH_END;
  if (v.front() == "bitwise_hash_bits")
    return FN_HASH_BITS;
  if (v.front() == "pgmeta") {
    spg_t pgid;
    if (cid->is_pg(&pgid)) {
      *oid = pgid.make_pgmeta_oid();
      v.pop_front();
      if (v.empty())
	return FN_OBJECT;
      goto do_object;
    }
    return -ENOENT;
  }
  if (v.front() == "all") {
    v.pop_front();
    if (v.empty())
      return FN_ALL;
    goto do_dir;
  }
  if (v.front() == "by_bitwise_hash") {
    v.pop_front();
    if (v.empty())
      return FN_HASH_DIR;
    unsigned long hv, hm;
    int r = sscanf(v.front().c_str(), "%lx", &hv);
    if (r != 1)
      return -ENOENT;
    int shift = 32 - v.front().length() * 4;
    v.pop_front();
    if (v.empty())
      return FN_HASH_DIR;
    r = sscanf(v.front().c_str(), "%ld", &hm);
    if (r != 1)
      return -ENOENT;
    if (hm < 1 || hm > 32)
      return -ENOENT;
    v.pop_front();
    *hash = hv << shift;//hobject_t::_reverse_bits(hv << shift);
    *hash_bits = hm;
    if (v.empty())
      return FN_HASH_VAL;
    goto do_dir;
  }
  return -ENOENT;

 do_dir:
  {
    string o = v.front();
    if (!oid->parse(o)) {
      return -ENOENT;
    }
    v.pop_front();
    if (v.empty())
      return FN_OBJECT;
  }

 do_object:
  if (v.front() == "data")
    return FN_OBJECT_DATA;
  if (v.front() == "omap_header")
    return FN_OBJECT_OMAP_HEADER;
  if (v.front() == "omap") {
    v.pop_front();
    if (v.empty())
      return FN_OBJECT_OMAP;
    *key = v.front();
    v.pop_front();
    if (v.empty())
      return FN_OBJECT_OMAP_VAL;
    return -ENOENT;
  }
  if (v.front() == "attr") {
    v.pop_front();
    if (v.empty())
      return FN_OBJECT_ATTR;
    *key = v.front();
    v.pop_front();
    if (v.empty())
      return FN_OBJECT_ATTR_VAL;
    return -ENOENT;
  }
  if (v.front() == "bitwise_hash")
    return FN_OBJECT_HASH;
  return -ENOENT;
}


static int os_getattr(const char *path, struct stat *stbuf)
{
  coll_t cid;
  ghobject_t oid;
  string key;
  uint32_t hash_value, hash_bits;
  int t = parse_fn(path, &cid, &oid, &key, &hash_value, &hash_bits);
  if (t < 0)
    return t;

  fuse_context *fc = fuse_get_context();
  FuseStore *fs = static_cast<FuseStore*>(fc->private_data);

  stbuf->st_size = 0;
  stbuf->st_uid = 0;
  stbuf->st_gid = 0;
  stbuf->st_mode = S_IFREG | 0700;

  switch (t) {
  case FN_OBJECT_OMAP:
  case FN_OBJECT_ATTR:
  case FN_OBJECT:
  case FN_OBJECT_DATA:
  case FN_OBJECT_OMAP_HEADER:
  case FN_OBJECT_OMAP_VAL:
    {
      spg_t pgid;
      if (cid.is_pg(&pgid)) {
	int bits = fs->store->collection_bits(cid);
	if (bits >= 0 && !oid.match(bits, pgid.ps())) {
	  // sorry, not part of this PG
	  return -ENOENT;
	}
      }
    }
    break;
  }

  switch (t) {
  case FN_OBJECT_OMAP:
  case FN_OBJECT_ATTR:
  case FN_OBJECT:
    if (!fs->store->exists(cid, oid))
      return -ENOENT;
    // fall-thru
  case FN_ALL:
  case FN_HASH_DIR:
  case FN_HASH_VAL:
  case FN_COLLECTION:
    if (!fs->store->collection_exists(cid))
      return -ENOENT;
    // fall-thru
  case FN_ROOT:
    stbuf->st_mode = S_IFDIR | 0700;
    return 0;

  case FN_TYPE:
    stbuf->st_size = fs->store->get_type().length() + 1;
    break;

  case FN_OBJECT_HASH:
    if (!fs->store->exists(cid, oid))
      return -ENOENT;
    stbuf->st_size = 9;
    return 0;

  case FN_HASH_END:
    if (!fs->store->collection_exists(cid))
      return -ENOENT;
    if (fs->store->collection_bits(cid) < 0)
      return -ENOENT;
    // fall-thru
  case FN_HASH_START:
    stbuf->st_size = 9;
    return 0;

  case FN_HASH_BITS:
    {
      if (!fs->store->collection_exists(cid))
	return -ENOENT;
      int bits = fs->store->collection_bits(cid);
      if (bits < 0)
	return -ENOENT;
      char buf[8];
      snprintf(buf, sizeof(buf), "%d\n", bits);
      stbuf->st_size = strlen(buf);
    }
    return 0;

  case FN_OBJECT_DATA:
    {
      if (!fs->store->exists(cid, oid))
	return -ENOENT;
      int r = fs->store->stat(cid, oid, stbuf);
      if (r < 0)
	return r;
    }
    break;

  case FN_OBJECT_OMAP_HEADER:
    {
      if (!fs->store->exists(cid, oid))
	return -ENOENT;
      bufferlist bl;
      fs->store->omap_get_header(cid, oid, &bl);
      stbuf->st_size = bl.length();
    }
    break;

  case FN_OBJECT_OMAP_VAL:
    {
      if (!fs->store->exists(cid, oid))
	return -ENOENT;
      set<string> k;
      k.insert(key);
      map<string,bufferlist> v;
      fs->store->omap_get_values(cid, oid, k, &v);
      if (!v.count(key)) {
	return -ENOENT;
      }
      stbuf->st_size = v[key].length();
    }
    break;

  case FN_OBJECT_ATTR_VAL:
    {
      if (!fs->store->exists(cid, oid))
	return -ENOENT;
      bufferptr v;
      int r = fs->store->getattr(cid, oid, key.c_str(), v);
      if (r == -ENODATA)
	r = -ENOENT;
      if (r < 0)
	return r;
      stbuf->st_size = v.length();
    }
    break;

  default:
    return -ENOENT;
  }

  return 0;
}

static int os_readdir(const char *path,
		      void *buf,
		      fuse_fill_dir_t filler,
		      off_t offset,
		      struct fuse_file_info *fi)
{
  coll_t cid;
  ghobject_t oid;
  string key;
  uint32_t hash_value, hash_bits;
  int t = parse_fn(path, &cid, &oid, &key, &hash_value, &hash_bits);
  if (t < 0)
    return t;

  fuse_context *fc = fuse_get_context();
  FuseStore *fs = static_cast<FuseStore*>(fc->private_data);

  // we can't shift 32 bits or else off_t will go negative
  const int hash_shift = 31;

  switch (t) {
  case FN_ROOT:
    {
      filler(buf, "type", NULL, 0);
      vector<coll_t> cls;
      fs->store->list_collections(cls);
      for (auto c : cls) {
	int r = filler(buf, stringify(c).c_str(), NULL, 0);
	if (r > 0)
	  break;
      }
    }
    break;

  case FN_COLLECTION:
    {
      filler(buf, "bitwise_hash_start", NULL, 0);
      if (fs->store->collection_bits(cid) >= 0) {
	filler(buf, "bitwise_hash_end", NULL, 0);
	filler(buf, "bitwise_hash_bits", NULL, 0);
      }
      filler(buf, "all", NULL, 0);
      filler(buf, "by_bitwise_hash", NULL, 0);
      spg_t pgid;
      if (cid.is_pg(&pgid) &&
	  fs->store->exists(cid, pgid.make_pgmeta_oid())) {
	filler(buf, "pgmeta", NULL, 0);
      }
    }
    break;

  case FN_OBJECT:
    {
      filler(buf, "bitwise_hash", NULL, 0);
      filler(buf, "data", NULL, 0);
      filler(buf, "omap", NULL, 0);
      filler(buf, "attr", NULL, 0);
      filler(buf, "omap_header", NULL, 0);
    }
    break;

  case FN_HASH_VAL:
  case FN_ALL:
    {
      uint32_t bitwise_hash = (offset >> hash_shift) & 0xffffffff;
      uint32_t hashoff = offset - (bitwise_hash << hash_shift);
      int skip = hashoff;
      ghobject_t next = cid.get_min_hobj();
      if (offset) {
	// obey the offset
	next.hobj.set_hash(hobject_t::_reverse_bits(bitwise_hash));
      } else if (t == FN_HASH_VAL) {
	next.hobj.set_hash(hobject_t::_reverse_bits(hash_value));
      }
      ghobject_t last;
      if (t == FN_HASH_VAL) {
	last = next;
	uint64_t rev_end = (hash_value | (0xffffffff >> hash_bits)) + 1;
	if (rev_end >= 0x100000000)
	  last = ghobject_t::get_max();
	else
	  last.hobj.set_hash(hobject_t::_reverse_bits(rev_end));
      } else {
	last = ghobject_t::get_max();
      }
      dout(10) << __func__ << std::hex
	       << " offset " << offset << " hash "
	       << hobject_t::_reverse_bits(hash_value)
	       << std::dec
	       << "/" << hash_bits
	       << " first " << next << " last " << last
	       << dendl;
      while (true) {
	vector<ghobject_t> ls;
	int r = fs->store->collection_list(
	  cid, next, last, true, 1000, &ls, &next);
	if (r < 0)
	  return r;
	for (auto p : ls) {
	  if (skip) {
	    --skip;
	    continue;
	  }
	  uint32_t cur_bitwise_hash = p.hobj.get_bitwise_key_u32();
	  if (cur_bitwise_hash != bitwise_hash) {
	    bitwise_hash = cur_bitwise_hash;
	    hashoff = 0;
	  }
	  ++hashoff;
	  uint64_t cur_off = ((uint64_t)bitwise_hash << hash_shift) |
	    (uint64_t)hashoff;
	  string s = stringify(p);
	  r = filler(buf, s.c_str(), NULL, cur_off);
	  if (r)
	    break;
	}
	if (r)
	  break;
	if (next == ghobject_t::get_max() || next == last)
	  break;
      }
    }
    break;

  case FN_OBJECT_OMAP:
    {
      set<string> keys;
      fs->store->omap_get_keys(cid, oid, &keys);
      unsigned skip = offset;
      for (auto k : keys) {
	if (skip) {
	  --skip;
	  continue;
	}
	++offset;
	int r = filler(buf, k.c_str(), NULL, offset);
	if (r)
	  break;
      }
    }
    break;

  case FN_OBJECT_ATTR:
    {
      map<string,bufferptr> aset;
      fs->store->getattrs(cid, oid, aset);
      unsigned skip = offset;
      for (auto a : aset) {
	if (skip) {
	  --skip;
	  continue;
	}
	++offset;
	int r = filler(buf, a.first.c_str(), NULL, offset);
	if (r)
	  break;
      }
    }
    break;
  }
  return 0;
}

static int os_open(const char *path, struct fuse_file_info *fi)
{
  coll_t cid;
  ghobject_t oid;
  string key;
  uint32_t hash_value, hash_bits;
  int t = parse_fn(path, &cid, &oid, &key, &hash_value, &hash_bits);
  if (t < 0)
    return t;

  fuse_context *fc = fuse_get_context();
  FuseStore *fs = static_cast<FuseStore*>(fc->private_data);

  bufferlist *pbl = 0;
  switch (t) {
  case FN_TYPE:
    pbl = new bufferlist;
    pbl->append(fs->store->get_type());
    pbl->append("\n");
    break;

  case FN_HASH_START:
    {
      pbl = new bufferlist;
      spg_t pgid;
      if (cid.is_pg(&pgid)) {
	unsigned long h;
	h = hobject_t::_reverse_bits(pgid.ps());
	char buf[10];
	snprintf(buf, sizeof(buf), "%08lx\n", h);
	pbl->append(buf);
      } else {
	pbl->append("00000000\n");
      }
    }
    break;

  case FN_HASH_END:
    {
      spg_t pgid;
      unsigned long h;
      if (cid.is_pg(&pgid)) {
	int hash_bits = fs->store->collection_bits(cid);
	if (hash_bits >= 0) {
	  uint64_t rev_start = hobject_t::_reverse_bits(pgid.ps());
	  uint64_t rev_end = (rev_start | (0xffffffff >> hash_bits));
	  h = rev_end;
	} else {
	  return -ENOENT;
	}
      } else {
	h = 0xffffffff;
      }
      char buf[10];
      snprintf(buf, sizeof(buf), "%08lx\n", h);
      pbl = new bufferlist;
      pbl->append(buf);
    }
    break;

  case FN_HASH_BITS:
    {
      int r = fs->store->collection_bits(cid);
      if (r < 0)
        return r;
      char buf[8];
      snprintf(buf, sizeof(buf), "%d\n", r);
      pbl = new bufferlist;
      pbl->append(buf);
    }
    break;

  case FN_OBJECT_HASH:
    {
      pbl = new bufferlist;
      char buf[10];
      snprintf(buf, sizeof(buf), "%08x\n",
	       (unsigned)oid.hobj.get_bitwise_key_u32());
      pbl->append(buf);
    }
    break;

  case FN_OBJECT_DATA:
    {
      pbl = new bufferlist;
      int r = fs->store->read(cid, oid, 0, 0, *pbl);
      if (r < 0) {
        delete pbl;
        return r;
      }
    }
    break;

  case FN_OBJECT_ATTR_VAL:
    {
      bufferptr bp;
      int r = fs->store->getattr(cid, oid, key.c_str(), bp);
      if (r < 0)
        return r;
      pbl = new bufferlist;
      pbl->append(bp);
    }
    break;

  case FN_OBJECT_OMAP_VAL:
    {
      set<string> k;
      k.insert(key);
      map<string,bufferlist> v;
      int r = fs->store->omap_get_values(cid, oid, k, &v);
      if (r < 0)
        return r;
      pbl = new bufferlist;
      *pbl = v[key];
    }
    break;

  case FN_OBJECT_OMAP_HEADER:
    {
      bufferlist bl;
      int r = fs->store->omap_get_header(cid, oid, &bl);
      if (r < 0)
       return r;
      pbl = new bufferlist;
      pbl->claim(bl);
    }
    break;
  }

  if (pbl) {
    fi->fh = reinterpret_cast<uint64_t>(pbl);
  }
  return 0;
}

static int os_mkdir(const char *path, mode_t mode)
{
  coll_t cid;
  ghobject_t oid;
  string key;
  uint32_t hash_value, hash_bits;
  int f = parse_fn(path, &cid, &oid, &key, &hash_value, &hash_bits);
  if (f < 0)
    return f;

  fuse_context *fc = fuse_get_context();
  FuseStore *fs = static_cast<FuseStore*>(fc->private_data);

  ObjectStore::Transaction t;
  switch (f) {
  case FN_OBJECT:
    {
      spg_t pgid;
      if (cid.is_pg(&pgid)) {
	int bits = fs->store->collection_bits(cid);
	if (bits >= 0 && !oid.match(bits, pgid.ps())) {
	  // sorry, not part of this PG
	  return -EINVAL;
	}
      }
      t.touch(cid, oid);
    }
    break;

  case FN_COLLECTION:
    if (cid.is_pg()) {
      // use the mode for the bit count.  e.g., mkdir --mode=0003
      // mnt/0.7_head will create 0.7 with bits = 3.
      mode &= 0777;
      if (mode >= 32)
	return -EINVAL;
    } else {
      mode = 0;
    }
    t.create_collection(cid, mode);
    break;

  default:
    return -EPERM;
  }

  if (!t.empty()) {
    ceph::shared_ptr<ObjectStore::Sequencer> osr(
      new ObjectStore::Sequencer("fuse"));
    fs->store->apply_transaction(&*osr, std::move(t));
    C_SaferCond waiter;
    if (!osr->flush_commit(&waiter))
      waiter.wait();
  }

  return 0;
}

static int os_chmod(const char *path, mode_t mode)
{
  return 0;
}

static int os_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  coll_t cid;
  ghobject_t oid;
  string key;
  uint32_t hash_value, hash_bits;
  int f = parse_fn(path, &cid, &oid, &key, &hash_value, &hash_bits);
  if (f < 0)
    return f;

  fuse_context *fc = fuse_get_context();
  FuseStore *fs = static_cast<FuseStore*>(fc->private_data);

  ObjectStore::Transaction t;
  bufferlist *pbl = 0;
  switch (f) {
  case FN_OBJECT_DATA:
    {
      pbl = new bufferlist;
      fs->store->read(cid, oid, 0, 0, *pbl);
    }
    break;

  case FN_OBJECT_ATTR_VAL:
    {
      pbl = new bufferlist;
      bufferptr bp;
      int r = fs->store->getattr(cid, oid, key.c_str(), bp);
      if (r == -ENODATA) {
	bufferlist empty;
	t.setattr(cid, oid, key.c_str(), empty);
      }
      pbl->append(bp);
    }
    break;

  case FN_OBJECT_OMAP_VAL:
    {
      pbl = new bufferlist;
      set<string> k;
      k.insert(key);
      map<string,bufferlist> v;
      fs->store->omap_get_values(cid, oid, k, &v);
      if (v.count(key) == 0) {
	map<string,bufferlist> aset;
	aset[key] = bufferlist();
	t.omap_setkeys(cid, oid, aset);
      } else {
	*pbl = v[key];
      }
    }
    break;
  }

  if (!t.empty()) {
    ceph::shared_ptr<ObjectStore::Sequencer> osr(
      new ObjectStore::Sequencer("fuse"));
    fs->store->apply_transaction(&*osr, std::move(t));
    C_SaferCond waiter;
    if (!osr->flush_commit(&waiter))
      waiter.wait();
  }

  if (pbl) {
    fi->fh = reinterpret_cast<uint64_t>(pbl);
  }
  return 0;
}

static int os_release(const char *path, struct fuse_file_info *fi)
{
  bufferlist *pbl = reinterpret_cast<bufferlist*>(fi->fh);
  delete pbl;
  return 0;
}

static int os_read(const char *path, char *buf, size_t size, off_t offset,
		   struct fuse_file_info *fi)
{
  bufferlist *pbl = reinterpret_cast<bufferlist*>(fi->fh);
  if (!pbl)
    return 0;
  if (offset >= pbl->length())
    return 0;
  if (offset + size > pbl->length())
    size = pbl->length() - offset;
  bufferlist r;
  r.substr_of(*pbl, offset, size);
  memcpy(buf, r.c_str(), r.length());
  return r.length();
}

static int os_write(const char *path, const char *buf, size_t size,
		    off_t offset, struct fuse_file_info *fi)
{
  bufferlist *pbl = reinterpret_cast<bufferlist*>(fi->fh);
  if (!pbl)
    return 0;

  bufferlist final;
  if (offset) {
    if (offset > pbl->length()) {
      final.substr_of(*pbl, 0, offset);
    } else {
      final.claim_append(*pbl);
      size_t zlen = offset - final.length();
      final.append_zero(zlen);
    }
  }
  final.append(buf, size);
  if (offset + size < pbl->length()) {
    bufferlist rest;
    rest.substr_of(*pbl, offset + size, pbl->length() - offset - size);
    final.claim_append(rest);
  }
  *pbl = final;
  return size;
}

int os_flush(const char *path, struct fuse_file_info *fi)
{
  coll_t cid;
  ghobject_t oid;
  string key;
  uint32_t hash_value, hash_bits;
  int f = parse_fn(path, &cid, &oid, &key, &hash_value, &hash_bits);
  if (f < 0)
    return f;

  fuse_context *fc = fuse_get_context();
  FuseStore *fs = static_cast<FuseStore*>(fc->private_data);

  bufferlist *pbl = reinterpret_cast<bufferlist*>(fi->fh);
  if (!pbl)
    return 0;

  ObjectStore::Transaction t;

  switch (f) {
  case FN_OBJECT_DATA:
    t.write(cid, oid, 0, pbl->length(), *pbl);
    break;

  case FN_OBJECT_ATTR_VAL:
    t.setattr(cid, oid, key.c_str(), *pbl);
    break;

  case FN_OBJECT_OMAP_VAL:
    {
      map<string,bufferlist> aset;
      aset[key] = *pbl;
      t.omap_setkeys(cid, oid, aset);
      break;
    }

  case FN_OBJECT_OMAP_HEADER:
    t.omap_setheader(cid, oid, *pbl);
    break;

  default:
    return 0;
  }

  ceph::shared_ptr<ObjectStore::Sequencer> osr(
    new ObjectStore::Sequencer("fuse"));
  fs->store->apply_transaction(&*osr, std::move(t));
  C_SaferCond waiter;
  if (!osr->flush_commit(&waiter))
    waiter.wait();

  return 0;
}

static int os_unlink(const char *path)
{
  coll_t cid;
  ghobject_t oid;
  string key;
  uint32_t hash_value, hash_bits;
  int f = parse_fn(path, &cid, &oid, &key, &hash_value, &hash_bits);
  if (f < 0)
    return f;

  fuse_context *fc = fuse_get_context();
  FuseStore *fs = static_cast<FuseStore*>(fc->private_data);

  ObjectStore::Transaction t;

  switch (f) {
  case FN_OBJECT_OMAP_VAL:
    {
      set<string> keys;
      keys.insert(key);
      t.omap_rmkeys(cid, oid, keys);
    }
    break;

  case FN_OBJECT_ATTR_VAL:
    t.rmattr(cid, oid, key.c_str());
    break;

  case FN_OBJECT_OMAP_HEADER:
    {
      bufferlist empty;
      t.omap_setheader(cid, oid, empty);
    }
    break;

  case FN_OBJECT:
    t.remove(cid, oid);
    break;

  case FN_COLLECTION:
    if (!fs->store->collection_empty(cid))
      return -ENOTEMPTY;
    t.remove_collection(cid);
    break;

  case FN_OBJECT_DATA:
    t.truncate(cid, oid, 0);
    break;

  default:
    return -EPERM;
  }

  ceph::shared_ptr<ObjectStore::Sequencer> osr(
    new ObjectStore::Sequencer("fuse"));
  fs->store->apply_transaction(&*osr, std::move(t));
  C_SaferCond waiter;
  if (!osr->flush_commit(&waiter))
    waiter.wait();

  return 0;
}

static int os_truncate(const char *path, off_t size)
{
  coll_t cid;
  ghobject_t oid;
  string key;
  uint32_t hash_value, hash_bits;
  int f = parse_fn(path, &cid, &oid, &key, &hash_value, &hash_bits);
  if (f < 0)
    return f;

  if (f == FN_OBJECT_OMAP_VAL ||
      f == FN_OBJECT_ATTR_VAL ||
      f == FN_OBJECT_OMAP_HEADER) {
    if (size)
      return -EPERM;
    return 0;
  }
  if (f != FN_OBJECT_DATA)
    return -EPERM;

  fuse_context *fc = fuse_get_context();
  FuseStore *fs = static_cast<FuseStore*>(fc->private_data);

  ObjectStore::Transaction t;
  t.truncate(cid, oid, size);
  ceph::shared_ptr<ObjectStore::Sequencer> osr(
    new ObjectStore::Sequencer("fuse"));
  fs->store->apply_transaction(&*osr, std::move(t));
  C_SaferCond waiter;
  if (!osr->flush_commit(&waiter))
    waiter.wait();
  return 0;
}

static int os_statfs(const char *path, struct statvfs *stbuf)
{
  fuse_context *fc = fuse_get_context();
  FuseStore *fs = static_cast<FuseStore*>(fc->private_data);

  struct statfs s;
  int r = fs->store->statfs(&s);
  if (r < 0)
    return r;
  stbuf->f_bsize = s.f_bsize;
  stbuf->f_blocks = s.f_blocks;
  stbuf->f_bfree = s.f_bfree;
  stbuf->f_bavail = s.f_bavail;
  stbuf->f_files = s.f_files;
  stbuf->f_ffree = s.f_ffree;
  return 0;
}

static struct fuse_operations fs_oper = {
  getattr: os_getattr,
  readlink: 0,
  getdir: 0,
  mknod: 0,
  mkdir: os_mkdir,
  unlink: os_unlink,
  rmdir: os_unlink,
  symlink: 0,
  rename: 0,
  link: 0,
  chmod: os_chmod,
  chown: 0,
  truncate: os_truncate,
  utime: 0,
  open: os_open,
  read: os_read,
  write: os_write,
  statfs: os_statfs,
  flush: os_flush,
  release: os_release,
  fsync: 0,
  setxattr: 0,
  getxattr: 0,
  listxattr: 0,
  removexattr: 0,
  opendir: 0,
  readdir: os_readdir,
  releasedir: 0,
  fsyncdir: 0,
  init: 0,
  destroy: 0,
  access: 0,
  create: os_create,
};

int FuseStore::main()
{
  const char *v[] = {
    "foo",
    mount_point.c_str(),
    "-f",
    "-d", // debug
  };
  int c = 3;
  if (g_conf->fuse_debug)
    ++c;
  return fuse_main(c, (char**)v, &fs_oper, (void*)this);
}

int FuseStore::start()
{
  dout(10) << __func__ << dendl;

  memset(&info->args, 0, sizeof(info->args));
  const char *v[] = {
    "foo",
    mount_point.c_str(),
    "-f", // foreground
    "-d", // debug
  };
  int c = 3;
  if (g_conf->fuse_debug)
    ++c;
  fuse_args a = FUSE_ARGS_INIT(c, (char**)v);
  info->args = a;
  if (fuse_parse_cmdline(&info->args, &info->mountpoint, NULL, NULL) == -1) {
    derr << __func__ << " failed to parse args" << dendl;
    return -EINVAL;
  }

  info->ch = fuse_mount(info->mountpoint, &info->args);
  if (!info->ch) {
    derr << __func__ << " fuse_mount failed" << dendl;
    return -EIO;
  }

  info->f = fuse_new(info->ch, &info->args, &fs_oper, sizeof(fs_oper),
		     (void*)this);
  if (!info->f) {
    fuse_unmount(info->mountpoint, info->ch);
    derr << __func__ << " fuse_new failed" << dendl;
    return -EIO;
  }

  fuse_thread.create("fusestore");
  dout(10) << __func__ << " done" << dendl;
  return 0;
}

int FuseStore::loop()
{
  dout(10) << __func__ << " enter" << dendl;
  int r = fuse_loop(info->f);
  if (r)
    derr << __func__ << " got " << cpp_strerror(r) << dendl;
  dout(10) << __func__ << " exit" << dendl;
  return r;
}

int FuseStore::stop()
{
  dout(10) << __func__ << " enter" << dendl;
  fuse_unmount(info->mountpoint, info->ch);
  fuse_thread.join();
  fuse_destroy(info->f);
  dout(10) << __func__ << " exit" << dendl;
  return 0;
}
