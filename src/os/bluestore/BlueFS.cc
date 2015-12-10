// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BlueFS.h"

#include "common/debug.h"
#include "common/errno.h"
#include "BlockDevice.h"
#include "Allocator.h"
#include "StupidAllocator.h"


#define dout_subsys ceph_subsys_bluefs
#undef dout_prefix
#define dout_prefix *_dout << "bluefs "

BlueFS::BlueFS()
  : lock("BlueFS::lock"),
    ino_last(0),
    log_seq(0),
    log_writer(NULL)
{
}

BlueFS::~BlueFS()
{
  for (auto p : bdev) {
    p->close();
    delete p;
  }
  for (auto p : ioc) {
    delete p;
  }
}

int BlueFS::add_block_device(unsigned id, string path)
{
  dout(10) << __func__ << " bdev " << id << " path " << path << dendl;
  assert(id == bdev.size());
  BlockDevice *b = new BlockDevice(NULL, NULL);  // no aio callback; use ioc
  int r = b->open(path);
  if (r < 0) {
    delete b;
    return r;
  }
  dout(1) << __func__ << " bdev " << id << " path " << path
	  << " size " << pretty_si_t(b->get_size()) << "B" << dendl;
  bdev.push_back(b);
  ioc.push_back(new IOContext(NULL));
  block_all.resize(bdev.size());
  return 0;
}

void BlueFS::add_block_extent(unsigned id, uint64_t offset, uint64_t length)
{
  Mutex::Locker l(lock);
  dout(1) << __func__ << " bdev " << id << " " << offset << "~" << length
	  << dendl;
  assert(id < bdev.size());
  assert(bdev[id]->get_size() >= offset + length);
  block_all[id].insert(offset, length);

  if (alloc.size()) {
    log_t.op_alloc_add(id, offset, length);
    int r = _flush_log();
    assert(r == 0);
    alloc[id]->init_add_free(offset, length);
  }
  dout(10) << __func__ << " done" << dendl;
}

uint64_t BlueFS::get_total(unsigned id)
{
  Mutex::Locker l(lock);
  assert(id < block_all.size());
  uint64_t r = 0;
  interval_set<uint64_t>& p = block_all[id];
  for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
    r += q.get_len();
  }
  return r;
}

uint64_t BlueFS::get_free(unsigned id)
{
  Mutex::Locker l(lock);
  assert(id < alloc.size());
  return alloc[id]->get_free();
}

int BlueFS::get_block_extents(unsigned id, interval_set<uint64_t> *extents)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " bdev " << id << dendl;
  if (id >= block_all.size())
    return -EINVAL;
  *extents = block_all[id];
  return 0;
}

int BlueFS::mkfs(uint64_t super_offset_a, uint64_t super_offset_b)
{
  dout(1) << __func__
	  << " super offsets " << super_offset_a << " " << super_offset_b
	  << dendl;
  assert(bdev.size() >= 1);

  _init_alloc();

  super.uuid.generate_random();
  dout(1) << __func__ << " uuid " << super.uuid << dendl;

  // init log
  File *log_file = new File;
  log_file->fnode.ino = 1;
  _allocate(0, g_conf->bluefs_max_log_runway, &log_file->fnode.extents);
  log_writer = new FileWriter(log_file);

  // initial txn
  log_t.op_init();
  for (unsigned bdev = 0; bdev < block_all.size(); ++bdev) {
    interval_set<uint64_t>& p = block_all[bdev];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      dout(20) << __func__ << " op_alloc_add " << bdev << " " << q.get_start()
	       << "~" << q.get_len() << dendl;
      log_t.op_alloc_add(bdev, q.get_start(), q.get_len());
    }
  }
  _flush_log();

  // write supers
  super.version = 0;
  super.super_a_offset = super_offset_a;
  super.super_b_offset = super_offset_b;
  super.block_size = bdev[0]->get_block_size();
  super.log_fnode = log_file->fnode;
  _write_super();
  super.version = 1;
  _write_super();
  _flush_bdev();

  // clean up
  super = bluefs_super_t();
  delete log_writer;
  log_writer = NULL;
  delete log_file;
  block_all.clear();
  alloc.clear();

  dout(10) << __func__ << " success" << dendl;
  return 0;
}

void BlueFS::_init_alloc()
{
  dout(20) << __func__ << dendl;
  alloc.resize(bdev.size());
  for (unsigned id = 0; id < bdev.size(); ++id) {
    alloc[id] = new StupidAllocator;
    interval_set<uint64_t>& p = block_all[id];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      alloc[id]->init_add_free(q.get_start(), q.get_len());
    }
  }
}

int BlueFS::mount(uint64_t super_offset_a, uint64_t super_offset_b)
{
  dout(1) << __func__ << " super offsets " << super_offset_a
	  << " " << super_offset_b << dendl;
  assert(!bdev.empty());

  int r = _open_super(super_offset_a, super_offset_b);
  if (r < 0) {
    derr << __func__ << " failed to open super: " << cpp_strerror(r) << dendl;
    goto out;
  }

  block_all.clear();
  block_all.resize(bdev.size());
  _init_alloc();

  r = _replay();
  if (r < 0) {
    derr << __func__ << " failed to replay log: " << cpp_strerror(r) << dendl;
    goto out;
  }

  // init freelist
  for (auto p : file_map) {
    dout(30) << __func__ << " noting alloc for " << p.second->fnode << dendl;
    for (auto q : p.second->fnode.extents) {
      alloc[q.bdev]->init_rm_free(q.offset, q.length);
    }
  }

  // set up the log for future writes
  log_writer = new FileWriter(_get_file(1));
  assert(log_writer->file->fnode.ino == 1);
  log_writer->pos = log_writer->file->fnode.size;
  dout(10) << __func__ << " log write pos set to " << log_writer->pos << dendl;
  return 0;

 out:
  super = bluefs_super_t();
  return r;
}

void BlueFS::umount()
{
  dout(1) << __func__ << dendl;
  sync_metadata();

  delete log_writer;
  log_writer = NULL;
  for (auto p : alloc) {
    delete p;
  }
  alloc.clear();
  block_all.clear();
  for (auto p : file_map) {
    delete p.second;
  }
  for (auto p : dir_map) {
    delete p.second;
  }
  super = bluefs_super_t();
  log_t.clear();
}

int BlueFS::_write_super()
{
  // build superblock
  bufferlist bl;
  ::encode(super, bl);
  uint32_t crc = bl.crc32c(-1);
  ::encode(crc, bl);
  assert(bl.length() <= super.block_size);
  bufferptr z(super.block_size - bl.length());
  z.zero();
  bl.append(z);
  bl.rebuild();

  uint64_t off = (super.version & 1) ?
    super.super_b_offset : super.super_a_offset;
  bdev[0]->aio_write(off, bl, ioc[0]);
  _submit_bdev();
  dout(20) << __func__ << " v " << super.version << " crc " << crc
	   << " offset " << off << dendl;
  return 0;
}

int BlueFS::_open_super(uint64_t super_offset_a, uint64_t super_offset_b)
{
  dout(10) << __func__ << dendl;

  bufferlist abl, bbl, t;
  bluefs_super_t a, b;
  uint32_t a_crc, b_crc, crc;
  int r;

  r = bdev[0]->read(super_offset_a, bdev[0]->get_block_size(), &abl, ioc[0]);
  if (r < 0)
    return r;
  r = bdev[0]->read(super_offset_b, bdev[0]->get_block_size(), &bbl, ioc[0]);
  if (r < 0)
    return r;

  bufferlist::iterator p = abl.begin();
  ::decode(a, p);
  {
    bufferlist t;
    t.substr_of(abl, 0, p.get_off());
    crc = t.crc32c(-1);
  }
  ::decode(a_crc, p);
  if (crc != a_crc) {
    derr << __func__ << " bad crc on superblock a, expected " << a_crc
	 << " != actual " << crc << dendl;
    return -EIO;
  }

  p = bbl.begin();
  ::decode(b, p);
  {
    bufferlist t;
    t.substr_of(bbl, 0, p.get_off());
    crc = t.crc32c(-1);
  }
  ::decode(b_crc, p);
  if (crc != b_crc) {
    derr << __func__ << " bad crc on superblock a, expected " << a_crc
	 << " != actual " << crc << dendl;
    return -EIO;
  }

  dout(10) << __func__ << " superblock a " << a.version
	   << " b " << b.version
	   << dendl;

  if (a.version == b.version + 1) {
    dout(10) << __func__ << " using a" << dendl;
    super = a;
  } else if (b.version == a.version + 1) {
    dout(10) << __func__ << " using b" << dendl;
    super = b;
  } else {
    derr << __func__ << " non-adjacent superblock versions "
	 << a.version << " and " << b.version << dendl;
    return -EIO;
  }
  dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
  return 0;
}

int BlueFS::_replay()
{
  dout(10) << __func__ << dendl;
  ino_last = 1;  // by the log
  log_seq = 0;

  File *log_file = _get_file(1);
  log_file->fnode = super.log_fnode;

  FileReader *log_reader = new FileReader(
    log_file, g_conf->bluefs_alloc_size,
    true);  // ignore eof
  while (true) {
    assert((log_reader->pos & ~super.block_mask()) == 0);
    uint64_t pos = log_reader->pos;
    bufferlist bl;
    {
      bufferptr bp;
      int r = _read(log_reader, pos, super.block_size, &bp, NULL);
      assert(r == (int)super.block_size);
      bl.append(bp);
    }
    uint64_t more = 0;
    uint64_t seq;
    uuid_d uuid;
    {
      bufferlist::iterator p = bl.begin();
      __u8 a, b;
      uint32_t len;
      ::decode(a, p);
      ::decode(b, p);
      ::decode(len, p);
      ::decode(uuid, p);
      ::decode(seq, p);
      if (len + 6 > bl.length()) {
	more = ROUND_UP_TO(len + 6 - bl.length(), super.block_size);
      }
    }
    if (uuid != super.uuid) {
      dout(10) << __func__ << " " << pos << ": stop: uuid " << uuid
	       << " != super.uuid " << super.uuid << dendl;
      break;
    }
    if (seq != log_seq + 1) {
      dout(10) << __func__ << " " << pos << ": stop: seq " << seq
	       << " != expected " << log_seq + 1 << dendl;
      break;
    }
    if (more) {
      dout(20) << __func__ << "  need " << more << " more bytes" << dendl;
      bufferptr bp;
      int r = _read(log_reader, pos + super.block_size, more, &bp, NULL);
      if (r < (int)more) {
	dout(10) << __func__ << " " << pos << ": stop: len is "
		 << bl.length() + more << ", which is past eof" << dendl;
	break;
      }
      assert(r == (int)more);
      bl.append(bp);
    }
    bluefs_transaction_t t;
    try {
      bufferlist::iterator p = bl.begin();
      ::decode(t, p);
    }
    catch (buffer::error& e) {
      dout(10) << __func__ << " " << pos << ": stop: failed to decode: "
	       << e.what() << dendl;
      break;
    }
    assert(seq == t.seq);
    dout(10) << __func__ << " " << pos << ": " << t << dendl;

    bufferlist::iterator p = t.op_bl.begin();
    while (!p.end()) {
      __u8 op;
      ::decode(op, p);
      switch (op) {

      case bluefs_transaction_t::OP_INIT:
	dout(20) << __func__ << " " << pos << ":  op_init" << dendl;
	assert(t.seq == 1);
	break;

      case bluefs_transaction_t::OP_ALLOC_ADD:
        {
	  __u8 id;
	  uint64_t offset, length;
	  ::decode(id, p);
	  ::decode(offset, p);
	  ::decode(length, p);
	  dout(20) << __func__ << " " << pos << ":  op_alloc_add "
		   << " " << (int)id << ":" << offset << "~" << length << dendl;
	  block_all[id].insert(offset, length);
	  alloc[id]->init_add_free(offset, length);
	}
	break;

      case bluefs_transaction_t::OP_ALLOC_RM:
        {
	  __u8 id;
	  uint64_t offset, length;
	  ::decode(id, p);
	  ::decode(offset, p);
	  ::decode(length, p);
	  dout(20) << __func__ << " " << pos << ":  op_alloc_add "
		   << " " << (int)id << ":" << offset << "~" << length << dendl;
	  block_all[id].erase(offset, length);
	  alloc[id]->init_rm_free(offset, length);
	}
	break;

      case bluefs_transaction_t::OP_DIR_LINK:
        {
	  string dirname, filename;
	  uint64_t ino;
	  ::decode(dirname, p);
	  ::decode(filename, p);
	  ::decode(ino, p);
	  dout(20) << __func__ << " " << pos << ":  op_dir_link "
		   << " " << dirname << "/" << filename << " to " << ino
		   << dendl;
	  File *file = _get_file(ino);
	  assert(file->fnode.ino);
	  map<string,Dir*>::iterator q = dir_map.find(dirname);
	  assert(q != dir_map.end());
	  map<string,File*>::iterator r = q->second->file_map.find(filename);
	  assert(r == q->second->file_map.end());
	  q->second->file_map[filename] = file;
	  ++file->refs;
	}
	break;

      case bluefs_transaction_t::OP_DIR_UNLINK:
        {
	  string dirname, filename;
	  ::decode(dirname, p);
	  ::decode(filename, p);
	  dout(20) << __func__ << " " << pos << ":  op_dir_unlink "
		   << " " << dirname << "/" << filename << dendl;
	  map<string,Dir*>::iterator q = dir_map.find(dirname);
	  assert(q != dir_map.end());
	  map<string,File*>::iterator r = q->second->file_map.find(filename);
	  assert(r != q->second->file_map.end());
	  q->second->file_map.erase(r);
	}
	break;

      case bluefs_transaction_t::OP_DIR_CREATE:
        {
	  string dirname;
	  ::decode(dirname, p);
	  dout(20) << __func__ << " " << pos << ":  op_dir_create " << dirname
		   << dendl;
	  map<string,Dir*>::iterator q = dir_map.find(dirname);
	  assert(q == dir_map.end());
	  dir_map[dirname] = new Dir;
	}
	break;

      case bluefs_transaction_t::OP_DIR_REMOVE:
        {
	  string dirname;
	  ::decode(dirname, p);
	  dout(20) << __func__ << " " << pos << ":  op_dir_remove " << dirname
		   << dendl;
	  map<string,Dir*>::iterator q = dir_map.find(dirname);
	  assert(q != dir_map.end());
	  assert(q->second->file_map.empty());
	  delete q->second;
	  dir_map.erase(q);
	}
	break;

      case bluefs_transaction_t::OP_FILE_UPDATE:
        {
	  bluefs_fnode_t fnode;
	  ::decode(fnode, p);
	  dout(20) << __func__ << " " << pos << ":  op_file_update "
		   << " " << fnode << dendl;
	  File *f = _get_file(fnode.ino);
	  f->fnode = fnode;
	  if (fnode.ino > ino_last) {
	    ino_last = fnode.ino;
	  }
	}
	break;

      case bluefs_transaction_t::OP_FILE_REMOVE:
        {
	  uint64_t ino;
	  ::decode(ino, p);
	  dout(20) << __func__ << " " << pos << ":  op_file_remove " << ino
		   << dendl;
	  auto p = file_map.find(ino);
	  assert(p != file_map.end());
	  delete p->second;
	  file_map.erase(p);
	}
	break;

      default:
	derr << __func__ << " " << pos << ": stop: unrecognized op " << (int)op
	     << dendl;
	return -EIO;
      }
    }
    assert(p.end());

    // we successfully replayed the transaction; bump the seq and log size
    ++log_seq;
    log_file->fnode.size = log_reader->pos;
  }

  dout(10) << __func__ << " log file size was " << log_file->fnode.size << dendl;
  delete log_reader;

  dout(10) << __func__ << " done" << dendl;
  return 0;
}

BlueFS::File *BlueFS::_get_file(uint64_t ino)
{
  File *f;
  auto p = file_map.find(ino);
  if (p == file_map.end()) {
    f = new File;
    file_map[ino] = f;
    dout(30) << __func__ << " ino " << ino << " = " << f
	     << " (new)" << dendl;
    return f;
  } else {
    dout(30) << __func__ << " ino " << ino << " = " << p->second << dendl;
    return p->second;
  }
}

void BlueFS::_drop_link(File *file)
{
  dout(20) << __func__ << " had refs " << file->refs
	   << " on " << file->fnode << dendl;
  --file->refs;
  if (file->refs == 0) {
    dout(20) << __func__ << " destroying " << file->fnode << dendl;
    log_t.op_file_remove(file->fnode.ino);
    for (auto r : file->fnode.extents) {
      alloc[r.bdev]->release(r.offset, r.length);
    }
    file_map.erase(file->fnode.ino);
    delete file;
  }
}

int BlueFS::_read(
  FileReader *h,  ///< [in] read from here
  uint64_t off,   ///< [in] offset
  size_t len,     ///< [in] this many bytes
  bufferptr *bp,  ///< [out] optional: reference the result here
  char *out)      ///< [out] optional: or copy it here
{
  Mutex::Locker l(h->lock);
  dout(10) << __func__ << " h " << h << " " << off << "~" << len
	   << " from " << h->file->fnode << dendl;
  if (!h->ignore_eof &&
      off + len > h->file->fnode.size) {
    len = h->file->fnode.size - off;
    dout(20) << __func__ << " reaching eof, len clipped to " << len << dendl;
  }
  if (len == 0)
    return 0;

  int left;
  if (off < h->bl_off || off >= h->get_buf_end()) {
    h->bl.clear();
    h->bl_off = off & super.block_mask();
    uint64_t x_off = 0;
    vector<bluefs_extent_t>::iterator p = h->file->fnode.seek(h->bl_off, &x_off);
    uint64_t want = ROUND_UP_TO(len + (off & ~super.block_mask()),
				super.block_size);
    want = MAX(want, h->max_prefetch);
    uint64_t l = MIN(p->length - x_off, want);
    uint64_t eof_offset = ROUND_UP_TO(h->file->fnode.size, super.block_size);
    if (!h->ignore_eof &&
	h->bl_off + l > eof_offset) {
      l = eof_offset - h->bl_off;
    }
    dout(20) << __func__ << " fetching " << x_off << "~" << l << " of "
	     << *p << dendl;
    int r = bdev[p->bdev]->read(p->offset + x_off, l, &h->bl, ioc[p->bdev]);
    assert(r == 0);
  }
  left = h->get_buf_remaining(off);
  dout(20) << __func__ << " left " << left << dendl;

  int r = MIN(len, left);
  // NOTE: h->bl is normally a contiguous buffer so c_str() is free.
  if (bp)
    *bp = bufferptr(h->bl.c_str() + off - h->bl_off, r);
  if (out)
    memcpy(out, h->bl.c_str() + off - h->bl_off, r);

  dout(30) << __func__ << " result (" << r << " bytes):\n";
  bufferlist t;
  t.substr_of(h->bl, off - h->bl_off, r);
  t.hexdump(*_dout);
  *_dout << dendl;

  h->pos = off + r;
  dout(20) << __func__ << " got " << r << dendl;
  return r;
}

void BlueFS::_invalidate_cache(File *f, uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " file " << f->fnode
	   << " " << offset << "~" << length << dendl;
#warning implement _invalidate_cache
}

int BlueFS::_flush_log()
{
  log_t.seq = ++log_seq;
  log_t.uuid = super.uuid;
  dout(10) << __func__ << " " << log_t << dendl;
  assert(!log_t.empty());

  // allocate some more space (before we run out)?
  uint64_t runway = log_writer->file->fnode.get_allocated() - log_writer->pos;
  if (runway < g_conf->bluefs_min_log_runway) {
    dout(10) << __func__ << " allocating more log runway ("
	     << runway << " remaining" << dendl;
    int r = _allocate(0, g_conf->bluefs_max_log_runway,
		      &log_writer->file->fnode.extents);
    assert(r == 0);
    log_t.op_file_update(log_writer->file->fnode);
  }

  bufferlist bl;
  ::encode(log_t, bl);

  // pad to block boundary
  uint64_t partial = bl.length() % super.block_size;
  if (partial) {
    bufferptr z(super.block_size - partial);
    dout(10) << __func__ << " padding with " << z.length() << " zeros" << dendl;
    z.zero();
    bufferlist zbl;
    zbl.append(z);
    bl.append(z);
  }
  log_writer->append(bl);

  log_t.clear();
  log_t.seq = 0;  // just so debug output is less confusing

  _flush_bdev();
  int r = _flush(log_writer);
  assert(r == 0);
  _flush_bdev();

  // clean dirty files
  dirty_file_list_t::iterator p = dirty_files.begin();
  while (p != dirty_files.end()) {
    File *file = &(*p);
    assert(file->dirty);
    dout(20) << __func__ << " cleaned file " << file->fnode << dendl;
    file->dirty = false;
    dirty_files.erase(p++);
  }

  return 0;
}

int BlueFS::_flush_range(FileWriter *h, uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " " << h << " pos " << h->pos
	   << " " << offset << "~" << length
	   << " to " << h->file->fnode << dendl;

  if (offset + length <= h->pos)
    return 0;
  if (offset < h->pos) {
    length -= h->pos - offset;
    offset = h->pos;
    dout(10) << " still need " << offset << "~" << length << dendl;
  }
  assert(offset <= h->file->fnode.size);

  uint64_t allocated = h->file->fnode.get_allocated();
  if (allocated < offset + length) {
    int r = _allocate(0, offset + length - allocated, &h->file->fnode.extents);
    if (r < 0)
      return r;
  }
  if (h->file->fnode.size < offset + length)
    h->file->fnode.size = offset + length;
  h->file->fnode.mtime = ceph_clock_now(NULL);
  log_t.op_file_update(h->file->fnode);
  if (!h->file->dirty) {
    h->file->dirty = true;
    dirty_files.push_back(*h->file);
  }
  dout(20) << __func__ << " file now " << h->file->fnode << dendl;

  uint64_t x_off = 0;
  vector<bluefs_extent_t>::iterator p = h->file->fnode.seek(offset, &x_off);
  assert(p != h->file->fnode.extents.end());
  dout(20) << __func__ << " in " << *p << " x_off " << x_off << dendl;

  unsigned partial = x_off & ~super.block_mask();
  bufferlist bl;
  if (partial) {
    dout(20) << __func__ << " using partial tail " << partial << dendl;
    assert(h->tail_block.length() == partial);
    bl.claim_append(h->tail_block);
    x_off -= partial;
    offset -= partial;
    length += partial;
  }
  if (length == partial + h->buffer.length()) {
    bl.claim_append(h->buffer);
  } else {
    bufferlist t;
    t.substr_of(h->buffer, 0, length);
    bl.claim_append(t);
    t.substr_of(h->buffer, length, h->buffer.length() - length);
    h->buffer.swap(t);
    dout(20) << " leaving " << h->buffer.length() << " unflushed" << dendl;
  }
  assert(bl.length() == length);

  dout(30) << "dump:\n";
  bl.hexdump(*_dout);
  *_dout << dendl;

  h->pos = offset + length;
  h->tail_block.clear();

  uint64_t bloff = 0;
  while (length > 0) {
    uint64_t wlen = MIN(p->length, length);
    bufferlist t;
    t.substr_of(bl, bloff, wlen);
    unsigned tail = wlen & ~super.block_mask();
    if (tail) {
      dout(20) << __func__ << " caching tail of " << tail
	       << " and padding block with zeros" << dendl;
      h->tail_block.substr_of(bl, bl.length() - tail, tail);
      bufferptr z(super.block_size - tail);
      z.zero();
      t.append(z);
    }
    bdev[0]->aio_write(p->offset + x_off, t, ioc[0]);
    bloff += wlen;
    length -= wlen;
    ++p;
    x_off = 0;
  }
  _submit_bdev();
  dout(20) << __func__ << " h " << h << " pos now " << h->pos << dendl;

  return 0;
}

int BlueFS::_flush(FileWriter *h)
{
  uint64_t length = h->buffer.length();
  uint64_t offset = h->pos;
  if (length == 0) {
    dout(10) << __func__ << " " << h << " no dirty data on "
	     << h->file->fnode << dendl;
    return 0;
  }
  dout(10) << __func__ << " " << h << " " << offset << "~" << length
	   << " to " << h->file->fnode << dendl;
  assert(h->pos <= h->file->fnode.size);
  return _flush_range(h, offset, length);
}

int BlueFS::_truncate(FileWriter *h, uint64_t offset)
{
  dout(10) << __func__ << " " << offset << " file " << h->file->fnode << dendl;

  // truncate off unflushed data?
  if (h->pos < offset &&
      h->pos + h->buffer.length() > offset) {
    bufferlist t;
    dout(20) << __func__ << " tossing out last " << offset - h->pos
	     << " unflushed bytes" << dendl;
    t.substr_of(h->buffer, 0, offset - h->pos);
    h->buffer.swap(t);
    assert(0 == "actually this shouldn't happen");
  }
  if (h->buffer.length()) {
    int r = _flush(h);
    if (r < 0)
      return r;
  }
  if (offset == h->file->fnode.size) {
    return 0;  // no-op!
  }
  if (offset > h->file->fnode.size) {
    assert(0 == "truncate up not supported");
  }
  assert(h->file->fnode.size >= offset);
  h->file->fnode.size = offset;
  log_t.op_file_update(h->file->fnode);
  return 0;
}

void BlueFS::_fsync(FileWriter *h)
{
  dout(10) << __func__ << " " << h << " " << h->file->fnode << dendl;
  _flush(h);
  if (h->file->dirty) {
    _flush_log();
    assert(!h->file->dirty);
  }
}

void BlueFS::_submit_bdev()
{
  dout(20) << __func__ << dendl;
  for (unsigned i = 0; i < bdev.size(); ++i) {
    bdev[i]->aio_submit(ioc[i]);
  }
}

void BlueFS::_flush_bdev()
{
  dout(20) << __func__ << dendl;
  for (auto p : ioc) {
    p->aio_wait();
  }
  for (auto p : bdev) {
    p->flush();
  }
}

int BlueFS::_allocate(unsigned id, uint64_t len, vector<bluefs_extent_t> *ev)
{
  dout(10) << __func__ << " len " << len << " from " << id << dendl;
  assert(id < alloc.size());

  uint64_t left = ROUND_UP_TO(len, g_conf->bluefs_alloc_size);
  int r = alloc[id]->reserve(left);
  if (r < 0) {
    derr << __func__ << " failed to allocate " << left << " on bdev " << id
	 << ", free " << alloc[id]->get_free() << dendl;
    return r;
  }

  uint64_t hint = 0;
  if (!ev->empty()) {
    hint = ev->back().end();
  }
  while (left > 0) {
    bluefs_extent_t e;
    e.bdev = id;
    int r = alloc[id]->allocate(left, g_conf->bluefs_alloc_size, hint,
				&e.offset, &e.length);
    if (r < 0)
      return r;
    ev->push_back(e);
    if (e.length >= left)
      break;
    left -= e.length;
    hint = e.end();
  }
  return 0;
}

int BlueFS::_preallocate(File *f, uint64_t off, uint64_t len)
{
  dout(10) << __func__ << " file " << f->fnode << " "
	   << off << "~" << len << dendl;
  uint64_t allocated = f->fnode.get_allocated();
  if (off + len > allocated) {
    uint64_t want = off + len - allocated;
    int r = _allocate(0, want, &f->fnode.extents);
    if (r < 0)
      return r;
    log_t.op_file_update(f->fnode);
  }
  return 0;
}

void BlueFS::sync_metadata()
{
  Mutex::Locker l(lock);
  if (log_t.empty()) {
    dout(10) << __func__ << " - no pending log events" << dendl;
    return;
  }
  dout(10) << __func__ << dendl;
  utime_t start = ceph_clock_now(NULL);
  for (auto p : alloc) {
    p->commit_start();
  }
  _flush_log();
  for (auto p : alloc) {
    p->commit_finish();
  }
  utime_t end = ceph_clock_now(NULL);
  utime_t dur = end - start;
  dout(10) << __func__ << " done in " << dur << dendl;
}

int BlueFS::open_for_write(
  const string& dirname,
  const string& filename,
  FileWriter **h,
  bool overwrite)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  Dir *dir;
  if (p == dir_map.end()) {
    // implicitly create the dir
    dout(20) << __func__ << "  dir " << dirname
	     << " does not exist" << dendl;
    return -ENOENT;
  } else {
    dir = p->second;
  }

  File *file;
  map<string,File*>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    if (overwrite) {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " does not exist" << dendl;
      return -ENOENT;
    }
    file = new File;
    file->fnode.ino = ++ino_last;
    file->fnode.mtime = ceph_clock_now(NULL);
    file_map[ino_last] = file;
    dir->file_map[filename] = file;
    ++file->refs;
  } else {
    // overwrite existing file?
    if (!overwrite) {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " already exists" << dendl;
      return -EEXIST;
    }
    file = q->second;
  }

  log_t.op_file_update(file->fnode);
  log_t.op_dir_link(dirname, filename, file->fnode.ino);

  *h = new FileWriter(file);
  dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
  return 0;
}

int BlueFS::open_for_read(
  const string& dirname,
  const string& filename,
  FileReader **h,
  bool random)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename
	   << (random ? " (random)":" (sequential)") << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  Dir *dir = p->second;

  map<string,File*>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second;

  *h = new FileReader(file, random ? 4096 : g_conf->bluefs_max_prefetch);
  dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
  return 0;
}

int BlueFS::rename(
  const string& old_dirname, const string& old_filename,
  const string& new_dirname, const string& new_filename)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << old_dirname << "/" << old_filename
	   << " -> " << new_dirname << "/" << new_filename << dendl;
  map<string,Dir*>::iterator p = dir_map.find(old_dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << old_dirname << " not found" << dendl;
    return -ENOENT;
  }
  Dir *old_dir = p->second;
  map<string,File*>::iterator q = old_dir->file_map.find(old_filename);
  if (q == old_dir->file_map.end()) {
    dout(20) << __func__ << " dir " << old_dirname << " (" << old_dir
	     << ") file " << old_filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second;

  p = dir_map.find(new_dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << new_dirname << " not found" << dendl;
    return -ENOENT;
  }
  Dir *new_dir = p->second;
  q = new_dir->file_map.find(new_filename);
  if (q != new_dir->file_map.end()) {
    dout(20) << __func__ << " dir " << new_dirname << " (" << old_dir
	     << ") file " << new_filename
	     << " already exists, unlinking" << dendl;
    assert(q->second != file);
    log_t.op_dir_unlink(new_dirname, new_filename);
    _drop_link(q->second);
  }

  dout(10) << __func__ << " " << new_dirname << "/" << new_filename << " "
	   << " " << file->fnode << dendl;

  new_dir->file_map[new_filename] = file;
  old_dir->file_map.erase(old_filename);

  log_t.op_dir_link(new_dirname, new_filename, file->fnode.ino);
  log_t.op_dir_unlink(old_dirname, old_filename);
  return 0;
}

int BlueFS::mkdir(const string& dirname)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  if (p != dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " exists" << dendl;
    return -EEXIST;
  }
  dir_map[dirname] = new Dir;
  log_t.op_dir_create(dirname);
  return 0;
}

int BlueFS::rmdir(const string& dirname)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " does not exist" << dendl;
    return -ENOENT;
  }
  Dir *dir = p->second;
  if (!dir->file_map.empty()) {
    dout(20) << __func__ << " dir " << dirname << " not empty" << dendl;
    return -ENOTEMPTY;
  }
  dir_map.erase(dirname);
  log_t.op_dir_remove(dirname);
  return 0;
}

bool BlueFS::dir_exists(const string& dirname)
{
  Mutex::Locker l(lock);
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  bool exists = p != dir_map.end();
  dout(10) << __func__ << " " << dirname << " = " << (int)exists << dendl;
  return exists;
}

int BlueFS::stat(const string& dirname, const string& filename,
		 uint64_t *size, utime_t *mtime)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  Dir *dir = p->second;
  map<string,File*>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second;
  if (size)
    *size = file->fnode.size;
  if (mtime)
    *mtime = file->fnode.mtime;
  return 0;
}

int BlueFS::lock_file(const string& dirname, const string& filename,
		      FileLock **plock)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  Dir *dir = p->second;
  map<string,File*>::iterator q = dir->file_map.find(filename);
  File *file;
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found, creating" << dendl;
    file = new File;
    file->fnode.ino = ++ino_last;
    file->fnode.mtime = ceph_clock_now(NULL);
    file_map[ino_last] = file;
    dir->file_map[filename] = file;
    ++file->refs;
    log_t.op_file_update(file->fnode);
    log_t.op_dir_link(dirname, filename, file->fnode.ino);
  } else {
    file = q->second;
  }
  if (file->locked) {
    dout(10) << __func__ << " already locked" << dendl;
    return -EBUSY;
  }
  file->locked = true;
  *plock = new FileLock(file);
  dout(10) << __func__ << " locked " << file->fnode
	   << " with " << *plock << dendl;
  return 0;
}

int BlueFS::unlock_file(FileLock *fl)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << fl << " on " << fl->file->fnode << dendl;
  assert(fl->file->locked);
  fl->file->locked = false;
  delete fl;
  return 0;
}

int BlueFS::readdir(const string& dirname, vector<string> *ls)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  Dir *dir = p->second;
  ls->reserve(dir->file_map.size() + 2);
  for (auto q : dir->file_map) {
    ls->push_back(q.first);
  }
  ls->push_back(".");
  ls->push_back("..");
  return 0;
}

int BlueFS::unlink(const string& dirname, const string& filename)
{
  Mutex::Locker l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,Dir*>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  Dir *dir = p->second;
  map<string,File*>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " file " << dirname << "/" << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second;
  dir->file_map.erase(filename);
  log_t.op_dir_unlink(dirname, filename);
  _drop_link(file);
  return 0;
}
