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
  : ino_last(0),
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

/*static void aio_cb(void *priv, void *priv2)
{
  BlueFS *fs = static_cast<BlueFS*>(priv);
  if (priv2)
    fs->_aio_finish(priv2);
    }*/

int BlueFS::add_block_device(unsigned id, string path)
{
  dout(10) << __func__ << " bdev " << id << " path " << path << dendl;
  assert(id == bdev.size());
  BlockDevice *b = BlockDevice::create(path, NULL, NULL); //aio_cb, this);
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

uint64_t BlueFS::get_block_device_size(unsigned id)
{
  return bdev[id]->get_size();
}

void BlueFS::add_block_extent(unsigned id, uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
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

int BlueFS::reclaim_blocks(unsigned id, uint64_t want,
			   uint64_t *offset, uint32_t *length)
{
  std::lock_guard<std::mutex> l(lock);
  dout(1) << __func__ << " bdev " << id << " want " << want << dendl;
  assert(id < alloc.size());
  int r = alloc[id]->reserve(want);
  assert(r == 0); // caller shouldn't ask for more than they can get

  r = alloc[id]->allocate(want, g_conf->bluefs_alloc_size, 0,
			    offset, length);
  assert(r >= 0);
  if (*length < want)
    alloc[id]->unreserve(want - *length);

  block_all[id].erase(*offset, *length);
  log_t.op_alloc_rm(id, *offset, *length);
  r = _flush_log();
  assert(r == 0);

  dout(1) << __func__ << " bdev " << id << " want " << want
	  << " got " << *offset << "~" << *length << dendl;
  return 0;
}

uint64_t BlueFS::get_total(unsigned id)
{
  std::lock_guard<std::mutex> l(lock);
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
  std::lock_guard<std::mutex> l(lock);
  assert(id < alloc.size());
  return alloc[id]->get_free();
}

void BlueFS::get_usage(vector<pair<uint64_t,uint64_t>> *usage)
{
  std::lock_guard<std::mutex> l(lock);
  usage->resize(bdev.size());
  for (unsigned id = 0; id < bdev.size(); ++id) {
    uint64_t total = 0;
    interval_set<uint64_t>& p = block_all[id];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      total += q.get_len();
    }
    (*usage)[id].first = alloc[id]->get_free();
    (*usage)[id].second = total;
    uint64_t used = (total - (*usage)[id].first) * 100 / total;
    dout(10) << __func__ << " bdev " << id
	     << " free " << (*usage)[id].first
	     << " (" << pretty_si_t((*usage)[id].first) << "B)"
	     << " / " << (*usage)[id].second
	     << " (" << pretty_si_t((*usage)[id].second) << "B)"
	     << ", used " << used << "%"
	     << dendl;
  }
}

int BlueFS::get_block_extents(unsigned id, interval_set<uint64_t> *extents)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " bdev " << id << dendl;
  if (id >= block_all.size())
    return -EINVAL;
  *extents = block_all[id];
  return 0;
}

int BlueFS::mkfs(uuid_d osd_uuid)
{
  dout(1) << __func__
	  << " osd_uuid " << osd_uuid
	  << dendl;
  assert(bdev.size() >= 1);

  _init_alloc();

  super.version = 1;
  super.block_size = bdev[0]->get_block_size();
  super.osd_uuid = osd_uuid;
  super.uuid.generate_random();
  dout(1) << __func__ << " uuid " << super.uuid << dendl;

  // init log
  FileRef log_file = new File;
  log_file->fnode.ino = 1;
  log_file->fnode.prefer_bdev = bdev.size() - 1;
  int r = _allocate(log_file->fnode.prefer_bdev,
	    g_conf->bluefs_max_log_runway,
	    &log_file->fnode.extents);
  assert(r == 0);
  log_writer = new FileWriter(log_file, bdev.size());

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
  super.log_fnode = log_file->fnode;
  _write_super();
  _flush_bdev();

  // clean up
  super = bluefs_super_t();
  _close_writer(log_writer);
  log_writer = NULL;
  block_all.clear();
  _stop_alloc();

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

void BlueFS::_stop_alloc()
{
  dout(20) << __func__ << dendl;
  for (auto p : alloc) {
    delete p;
  }
  alloc.clear();
}

int BlueFS::mount()
{
  dout(1) << __func__ << dendl;
  assert(!bdev.empty());

  int r = _open_super();
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
    _stop_alloc();
    goto out;
  }

  // init freelist
  for (auto& p : file_map) {
    dout(30) << __func__ << " noting alloc for " << p.second->fnode << dendl;
    for (auto& q : p.second->fnode.extents) {
      alloc[q.bdev]->init_rm_free(q.offset, q.length);
    }
  }

  // set up the log for future writes
  log_writer = new FileWriter(_get_file(1), bdev.size());
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

  _close_writer(log_writer);
  log_writer = NULL;

  block_all.clear();
  _stop_alloc();
  file_map.clear();
  dir_map.clear();
  super = bluefs_super_t();
  log_t.clear();
}

int BlueFS::fsck()
{
  std::lock_guard<std::mutex> l(lock);
  dout(1) << __func__ << dendl;
  // hrm, i think we check everything on mount...
  return 0;
}

int BlueFS::_write_super()
{
  // build superblock
  bufferlist bl;
  ::encode(super, bl);
  uint32_t crc = bl.crc32c(-1);
  ::encode(crc, bl);
  assert(bl.length() <= get_super_length());
  bl.append_zero(get_super_length() - bl.length());
  bl.rebuild();

  IOContext ioc(NULL);
  bdev[0]->aio_write(get_super_offset(), bl, &ioc, false);
  bdev[0]->aio_submit(&ioc);
  ioc.aio_wait();
  dout(20) << __func__ << " v " << super.version << " crc " << crc
	   << " offset " << get_super_offset() << dendl;
  return 0;
}

int BlueFS::_open_super()
{
  dout(10) << __func__ << dendl;

  bufferlist bl;
  uint32_t expected_crc, crc;
  int r;

  // always the second block
  r = bdev[0]->read(get_super_offset(), get_super_length(),
		    &bl, ioc[0], false);
  if (r < 0)
    return r;

  bufferlist::iterator p = bl.begin();
  ::decode(super, p);
  {
    bufferlist t;
    t.substr_of(bl, 0, p.get_off());
    crc = t.crc32c(-1);
  }
  ::decode(expected_crc, p);
  if (crc != expected_crc) {
    derr << __func__ << " bad crc on superblock, expected " << expected_crc
	 << " != actual " << crc << dendl;
    return -EIO;
  }
  dout(10) << __func__ << " superblock " << super.version << dendl;
  dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
  return 0;
}

int BlueFS::_replay()
{
  dout(10) << __func__ << dendl;
  ino_last = 1;  // by the log
  log_seq = 0;

  FileRef log_file = _get_file(1);
  log_file->fnode = super.log_fnode;

  FileReader *log_reader = new FileReader(
    log_file, g_conf->bluefs_alloc_size,
    false,  // !random
    true);  // ignore eof
  while (true) {
    assert((log_reader->buf.pos & ~super.block_mask()) == 0);
    uint64_t pos = log_reader->buf.pos;
    bufferlist bl;
    {
      int r = _read(log_reader, &log_reader->buf, pos, super.block_size,
		    &bl, NULL);
      assert(r == (int)super.block_size);
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
      bufferlist t;
      int r = _read(log_reader, &log_reader->buf, pos + super.block_size, more,
		    &t, NULL);
      if (r < (int)more) {
	dout(10) << __func__ << " " << pos << ": stop: len is "
		 << bl.length() + more << ", which is past eof" << dendl;
	break;
      }
      assert(r == (int)more);
      bl.claim_append(t);
    }
    bluefs_transaction_t t;
    try {
      bufferlist::iterator p = bl.begin();
      ::decode(t, p);
    }
    catch (buffer::error& e) {
      dout(10) << __func__ << " " << pos << ": stop: failed to decode: "
	       << e.what() << dendl;
      delete log_reader;
      return -EIO;
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

      case bluefs_transaction_t::OP_JUMP_SEQ:
        {
	  uint64_t next_seq;
	  ::decode(next_seq, p);
	  dout(20) << __func__ << " " << pos << ":  op_jump_seq "
		   << next_seq << dendl;
	  assert(next_seq >= log_seq);
	  log_seq = next_seq - 1; // we will increment it below
	}
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
	  dout(20) << __func__ << " " << pos << ":  op_alloc_rm "
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
	  FileRef file = _get_file(ino);
	  assert(file->fnode.ino);
	  map<string,DirRef>::iterator q = dir_map.find(dirname);
	  assert(q != dir_map.end());
	  map<string,FileRef>::iterator r = q->second->file_map.find(filename);
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
	  map<string,DirRef>::iterator q = dir_map.find(dirname);
	  assert(q != dir_map.end());
	  map<string,FileRef>::iterator r = q->second->file_map.find(filename);
	  assert(r != q->second->file_map.end());
	  --r->second->refs;
	  q->second->file_map.erase(r);
	}
	break;

      case bluefs_transaction_t::OP_DIR_CREATE:
        {
	  string dirname;
	  ::decode(dirname, p);
	  dout(20) << __func__ << " " << pos << ":  op_dir_create " << dirname
		   << dendl;
	  map<string,DirRef>::iterator q = dir_map.find(dirname);
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
	  map<string,DirRef>::iterator q = dir_map.find(dirname);
	  assert(q != dir_map.end());
	  assert(q->second->file_map.empty());
	  dir_map.erase(q);
	}
	break;

      case bluefs_transaction_t::OP_FILE_UPDATE:
        {
	  bluefs_fnode_t fnode;
	  ::decode(fnode, p);
	  dout(20) << __func__ << " " << pos << ":  op_file_update "
		   << " " << fnode << dendl;
	  FileRef f = _get_file(fnode.ino);
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
	  file_map.erase(p);
	}
	break;

      default:
	derr << __func__ << " " << pos << ": stop: unrecognized op " << (int)op
	     << dendl;
	delete log_reader;
        return -EIO;
      }
    }
    assert(p.end());

    // we successfully replayed the transaction; bump the seq and log size
    ++log_seq;
    log_file->fnode.size = log_reader->buf.pos;
  }

  dout(10) << __func__ << " log file size was " << log_file->fnode.size << dendl;
  delete log_reader;

  // verify file link counts are all >0
  for (auto& p : file_map) {
    if (p.second->refs == 0 &&
	p.second->fnode.ino > 1) {
      derr << __func__ << " file with link count 0: " << p.second->fnode
	   << dendl;
      return -EIO;
    }
  }

  dout(10) << __func__ << " done" << dendl;
  return 0;
}

BlueFS::FileRef BlueFS::_get_file(uint64_t ino)
{
  auto p = file_map.find(ino);
  if (p == file_map.end()) {
    FileRef f = new File;
    file_map[ino] = f;
    dout(30) << __func__ << " ino " << ino << " = " << f
	     << " (new)" << dendl;
    return f;
  } else {
    dout(30) << __func__ << " ino " << ino << " = " << p->second << dendl;
    return p->second;
  }
}

void BlueFS::_drop_link(FileRef file)
{
  dout(20) << __func__ << " had refs " << file->refs
	   << " on " << file->fnode << dendl;
  --file->refs;
  if (file->refs == 0) {
    dout(20) << __func__ << " destroying " << file->fnode << dendl;
    assert(file->num_reading.load() == 0);
    log_t.op_file_remove(file->fnode.ino);
    for (auto& r : file->fnode.extents) {
      alloc[r.bdev]->release(r.offset, r.length);
    }
    file_map.erase(file->fnode.ino);
    file->deleted = true;
    if (file->dirty) {
      file->dirty = false;
      dirty_files.erase(dirty_files.iterator_to(*file));
    }
  }
}

int BlueFS::_read_random(
  FileReader *h,         ///< [in] read from here
  uint64_t off,          ///< [in] offset
  size_t len,            ///< [in] this many bytes
  char *out)             ///< [out] optional: or copy it here
{
  dout(10) << __func__ << " h " << h << " " << off << "~" << len
	   << " from " << h->file->fnode << dendl;

  ++h->file->num_reading;

  if (!h->ignore_eof &&
      off + len > h->file->fnode.size) {
    if (off > h->file->fnode.size)
      len = 0;
    else
      len = h->file->fnode.size - off;
    dout(20) << __func__ << " reaching (or past) eof, len clipped to "
	     << len << dendl;
  }

  int ret = 0;
  while (len > 0) {
    uint64_t x_off = 0;
    vector<bluefs_extent_t>::iterator p = h->file->fnode.seek(off, &x_off);
    uint64_t l = MIN(p->length - x_off, len);
    if (!h->ignore_eof &&
	off + l > h->file->fnode.size) {
      l = h->file->fnode.size - off;
    }
    dout(20) << __func__ << " read buffered " << x_off << "~" << l << " of "
	       << *p << dendl;
    int r = bdev[p->bdev]->read_buffered(p->offset + x_off, l, out);
    assert(r == 0);
    off += l;
    len -= l;
    ret += l;
    out += l;
  }

  dout(20) << __func__ << " got " << ret << dendl;
  --h->file->num_reading;
  return ret;
}

int BlueFS::_read(
  FileReader *h,         ///< [in] read from here
  FileReaderBuffer *buf, ///< [in] reader state
  uint64_t off,          ///< [in] offset
  size_t len,            ///< [in] this many bytes
  bufferlist *outbl,     ///< [out] optional: reference the result here
  char *out)             ///< [out] optional: or copy it here
{
  dout(10) << __func__ << " h " << h << " " << off << "~" << len
	   << " from " << h->file->fnode << dendl;

  ++h->file->num_reading;

  if (!h->ignore_eof &&
      off + len > h->file->fnode.size) {
    if (off > h->file->fnode.size)
      len = 0;
    else
      len = h->file->fnode.size - off;
    dout(20) << __func__ << " reaching (or past) eof, len clipped to "
	     << len << dendl;
  }
  if (outbl)
    outbl->clear();

  int ret = 0;
  while (len > 0) {
    size_t left;
    if (off < buf->bl_off || off >= buf->get_buf_end()) {
      buf->bl.clear();
      buf->bl_off = off & super.block_mask();
      uint64_t x_off = 0;
      vector<bluefs_extent_t>::iterator p =
	h->file->fnode.seek(buf->bl_off, &x_off);
      uint64_t want = ROUND_UP_TO(len + (off & ~super.block_mask()),
				  super.block_size);
      want = MAX(want, buf->max_prefetch);
      uint64_t l = MIN(p->length - x_off, want);
      uint64_t eof_offset = ROUND_UP_TO(h->file->fnode.size, super.block_size);
      if (!h->ignore_eof &&
	  buf->bl_off + l > eof_offset) {
	l = eof_offset - buf->bl_off;
      }
      dout(20) << __func__ << " fetching " << x_off << "~" << l << " of "
	       << *p << dendl;
      int r = bdev[p->bdev]->read(p->offset + x_off, l, &buf->bl, ioc[p->bdev],
				  true);
      assert(r == 0);
    }
    left = buf->get_buf_remaining(off);
    dout(20) << __func__ << " left " << left << " len " << len << dendl;

    int r = MIN(len, left);
    if (outbl) {
      bufferlist t;
      t.substr_of(buf->bl, off - buf->bl_off, r);
      outbl->claim_append(t);
    }
    if (out) {
      // NOTE: h->bl is normally a contiguous buffer so c_str() is free.
      memcpy(out, buf->bl.c_str() + off - buf->bl_off, r);
      out += r;
    }

    dout(30) << __func__ << " result chunk (" << r << " bytes):\n";
    bufferlist t;
    t.substr_of(buf->bl, off - buf->bl_off, r);
    t.hexdump(*_dout);
    *_dout << dendl;

    off += r;
    len -= r;
    ret += r;
    buf->pos += r;
  }

  dout(20) << __func__ << " got " << ret << dendl;
  assert(!outbl || (int)outbl->length() == ret);
  --h->file->num_reading;
  return ret;
}

void BlueFS::_invalidate_cache(FileRef f, uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " file " << f->fnode
	   << " " << offset << "~" << length << dendl;
  if (offset & ~super.block_mask()) {
    offset &= super.block_mask();
    length = ROUND_UP_TO(length, super.block_size);
  }
  uint64_t x_off = 0;
  vector<bluefs_extent_t>::iterator p = f->fnode.seek(offset, &x_off);
  while (length > 0 && p != f->fnode.extents.end()) {
    uint64_t x_len = MIN(p->length - x_off, length);
    bdev[p->bdev]->invalidate_cache(p->offset + x_off, x_len);
    dout(20) << "  " << x_off << "~" << x_len << " of " << *p << dendl;
    offset += x_len;
    length -= x_len;
  }
}

uint64_t BlueFS::_estimate_log_size()
{
  int avg_dir_size = 40;  // fixme
  int avg_file_size = 12;
  uint64_t size = 4096 * 2;
  size += file_map.size() * (1 + sizeof(bluefs_fnode_t));
  for (auto& p : block_all)
    size += p.num_intervals() * (1 + 1 + sizeof(uint64_t) * 2);
  size += dir_map.size() + (1 + avg_dir_size);
  size += file_map.size() * (1 + avg_dir_size + avg_file_size);
  return ROUND_UP_TO(size, super.block_size);
}

void BlueFS::_maybe_compact_log()
{
  uint64_t current = log_writer->file->fnode.size;
  uint64_t expected = _estimate_log_size();
  float ratio = (float)current / (float)expected;
  dout(10) << __func__ << " current " << current
	   << " expected " << expected
	   << " ratio " << ratio << dendl;
  if (current < g_conf->bluefs_log_compact_min_size ||
      ratio < g_conf->bluefs_log_compact_min_ratio)
    return;
  _compact_log();
  dout(20) << __func__ << " done, actual " << log_writer->file->fnode.size
	   << " vs expected " << expected << dendl;
}

void BlueFS::_compact_log()
{
  // FIXME: we currently hold the lock while writing out the compacted log,
  // which may mean a latency spike.  we could drop the lock while writing out
  // the big compacted log, while continuing to log at the end of the old log
  // file, and once it's done swap out the old log extents for the new ones.
  dout(10) << __func__ << dendl;
  File *log_file = log_writer->file.get();

  // clear out log (be careful who calls us!!!)
  log_t.clear();

  bluefs_transaction_t t;
  t.seq = 1;
  t.uuid = super.uuid;
  dout(20) << __func__ << " op_init" << dendl;
  t.op_init();
  for (unsigned bdev = 0; bdev < block_all.size(); ++bdev) {
    interval_set<uint64_t>& p = block_all[bdev];
    for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
      dout(20) << __func__ << " op_alloc_add " << bdev << " " << q.get_start()
	       << "~" << q.get_len() << dendl;
      t.op_alloc_add(bdev, q.get_start(), q.get_len());
    }
  }
  for (auto& p : file_map) {
    if (p.first == 1)
      continue;
    dout(20) << __func__ << " op_file_update " << p.second->fnode << dendl;
    t.op_file_update(p.second->fnode);
  }
  for (auto& p : dir_map) {
    dout(20) << __func__ << " op_dir_create " << p.first << dendl;
    t.op_dir_create(p.first);
    for (auto& q : p.second->file_map) {
      dout(20) << __func__ << " op_dir_link " << p.first << "/" << q.first
	       << " to " << q.second->fnode.ino << dendl;
      t.op_dir_link(p.first, q.first, q.second->fnode.ino);
    }
  }
  dout(20) << __func__ << " op_jump_seq " << log_seq << dendl;
  t.op_jump_seq(log_seq);

  bufferlist bl;
  ::encode(t, bl);
  _pad_bl(bl);

  uint64_t need = bl.length() + g_conf->bluefs_max_log_runway;
  dout(20) << __func__ << " need " << need << dendl;

  vector<bluefs_extent_t> old_extents;
  old_extents.swap(log_file->fnode.extents);
  while (log_file->fnode.get_allocated() < need) {
    int r = _allocate(log_file->fnode.prefer_bdev,
		      need - log_file->fnode.get_allocated(),
		      &log_file->fnode.extents);
    assert(r == 0);
  }

  _close_writer(log_writer);

  log_file->fnode.size = bl.length();
  log_writer = new FileWriter(log_file, bdev.size());
  log_writer->append(bl);
  int r = _flush(log_writer, true);
  assert(r == 0);
  _flush_wait(log_writer);

  dout(10) << __func__ << " writing super" << dendl;
  super.log_fnode = log_file->fnode;
  ++super.version;
  _write_super();
  _flush_bdev();

  dout(10) << __func__ << " release old log extents " << old_extents << dendl;
  for (auto& r : old_extents) {
    alloc[r.bdev]->release(r.offset, r.length);
  }
}

void BlueFS::_pad_bl(bufferlist& bl)
{
  uint64_t partial = bl.length() % super.block_size;
  if (partial) {
    dout(10) << __func__ << " padding with " << super.block_size - partial
	     << " zeros" << dendl;
    bl.append_zero(super.block_size - partial);
  }
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
    int r = _allocate(log_writer->file->fnode.prefer_bdev,
		      g_conf->bluefs_max_log_runway,
		      &log_writer->file->fnode.extents);
    assert(r == 0);
    log_t.op_file_update(log_writer->file->fnode);
  }

  bufferlist bl;
  ::encode(log_t, bl);

  // pad to block boundary
  _pad_bl(bl);
  log_writer->append(bl);

  log_t.clear();
  log_t.seq = 0;  // just so debug output is less confusing

  _flush_bdev();
  int r = _flush(log_writer, true);
  assert(r == 0);
  _flush_wait(log_writer);
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
  assert(!h->file->deleted);
  assert(h->file->num_readers.load() == 0);

  if (offset + length <= h->pos)
    return 0;
  if (offset < h->pos) {
    length -= h->pos - offset;
    offset = h->pos;
    dout(10) << " still need " << offset << "~" << length << dendl;
  }
  assert(offset <= h->file->fnode.size);

  uint64_t allocated = h->file->fnode.get_allocated();

  // do not bother to dirty the file if we are overwriting
  // previously allocated extents.
  bool must_dirty = false;
  if (allocated < offset + length) {
    int r = _allocate(h->file->fnode.prefer_bdev,
		      offset + length - allocated,
		      &h->file->fnode.extents);
    if (r < 0)
      return r;
    must_dirty = true;
  }
  if (h->file->fnode.size < offset + length) {
    h->file->fnode.size = offset + length;
    must_dirty = true;
  }
  if (must_dirty) {
    h->file->fnode.mtime = ceph_clock_now(NULL);
    log_t.op_file_update(h->file->fnode);
    if (!h->file->dirty) {
      h->file->dirty = true;
      dirty_files.push_back(*h->file);
    }
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
    dout(20) << __func__ << " waiting for previous aio to complete" << dendl;
    for (auto p : h->iocv) {
      p->aio_wait();
    }
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
    uint64_t x_len = MIN(p->length - x_off, length);
    bufferlist t;
    t.substr_of(bl, bloff, x_len);
    unsigned tail = x_len & ~super.block_mask();
    if (tail) {
      dout(20) << __func__ << " caching tail of " << tail
	       << " and padding block with zeros" << dendl;
      h->tail_block.substr_of(bl, bl.length() - tail, tail);
      t.append_zero(super.block_size - tail);
    }
    bdev[p->bdev]->aio_write(p->offset + x_off, t, h->iocv[p->bdev], true);
    bloff += x_len;
    length -= x_len;
    ++p;
    x_off = 0;
  }
  for (unsigned i = 0; i < bdev.size(); ++i) {
    if (h->iocv[i]->has_aios()) {
      bdev[i]->aio_submit(h->iocv[i]);
    }
  }
  dout(20) << __func__ << " h " << h << " pos now " << h->pos << dendl;
  return 0;
}

void BlueFS::_flush_wait(FileWriter *h)
{
  dout(10) << __func__ << " " << h << dendl;
  utime_t start = ceph_clock_now(NULL);
  for (auto p : h->iocv) {
    p->aio_wait();
  }
  utime_t end = ceph_clock_now(NULL);
  utime_t dur = end - start;
  dout(10) << __func__ << " " << h << " done in " << dur << dendl;
}

int BlueFS::_flush(FileWriter *h, bool force)
{
  uint64_t length = h->buffer.length();
  uint64_t offset = h->pos;
  if (!force &&
      length < g_conf->bluefs_min_flush_size) {
    dout(10) << __func__ << " " << h << " ignoring, length " << length
	     << " < min_flush_size " << g_conf->bluefs_min_flush_size
	     << dendl;
    return 0;
  }
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
    int r = _flush(h, true);
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
  _flush(h, true);
  _flush_wait(h);
  if (h->file->dirty) {
    dout(20) << __func__ << " file metadata is dirty, flushing log on "
	     << h->file->fnode << dendl;
    _flush_log();
    assert(!h->file->dirty);
  }
}

void BlueFS::_flush_bdev()
{
  dout(20) << __func__ << dendl;
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
    if (id) {
      derr << __func__ << " failed to allocate " << left << " on bdev " << id
	   << ", free " << alloc[id]->get_free()
	   << "; fallback to bdev 0" << dendl;
      return _allocate(0, len, ev);
    }
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
    if (r < 0) {
      assert(0 == "allocate failed... wtf");
      return r;
    }
    if (!ev->empty() && ev->back().end() == e.offset)
      ev->back().length += e.length;
    else
      ev->push_back(e);
    if (e.length >= left)
      break;
    left -= e.length;
    hint = e.end();
  }
  return 0;
}

int BlueFS::_preallocate(FileRef f, uint64_t off, uint64_t len)
{
  dout(10) << __func__ << " file " << f->fnode << " "
	   << off << "~" << len << dendl;
  uint64_t allocated = f->fnode.get_allocated();
  if (off + len > allocated) {
    uint64_t want = off + len - allocated;
    int r = _allocate(f->fnode.prefer_bdev, want, &f->fnode.extents);
    if (r < 0)
      return r;
    log_t.op_file_update(f->fnode);
  }
  return 0;
}

void BlueFS::sync_metadata()
{
  std::lock_guard<std::mutex> l(lock);
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
  _maybe_compact_log();
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
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  DirRef dir;
  if (p == dir_map.end()) {
    // implicitly create the dir
    dout(20) << __func__ << "  dir " << dirname
	     << " does not exist" << dendl;
    return -ENOENT;
  } else {
    dir = p->second;
  }

  FileRef file;
  bool create = false;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
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
    create = true;
  } else {
    // overwrite existing file?
    file = q->second;
    if (overwrite) {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " already exists, overwrite in place" << dendl;
    } else {
      dout(20) << __func__ << " dir " << dirname << " (" << dir
	       << ") file " << filename
	       << " already exists, truncate + overwrite" << dendl;
      file->fnode.size = 0;
      for (auto& p : file->fnode.extents) {
        alloc[p.bdev]->release(p.offset, p.length);
      }
      file->fnode.extents.clear();
    }
    file->fnode.mtime = ceph_clock_now(NULL);
  }

  if (dirname.length() > 5) {
    // the "db.slow" and "db.wal" directory names are hard-coded at
    // match up with bluestore.  the slow device is always the second
    // one (when a dedicated block.db device is present and used at
    // bdev 0).  the wal device is always last.
    if (strcmp(dirname.c_str() + dirname.length() - 5, ".slow") == 0) {
      assert(bdev.size() > 1);
      dout(20) << __func__ << " mapping " << dirname << "/" << filename
	       << " to bdev 1" << dendl;
      file->fnode.prefer_bdev = 1;
    } else if (strcmp(dirname.c_str() + dirname.length() - 4, ".wal") == 0) {
      assert(bdev.size() > 1);
      file->fnode.prefer_bdev = bdev.size() - 1;
      dout(20) << __func__ << " mapping " << dirname << "/" << filename
	       << " to bdev " << (int)file->fnode.prefer_bdev << dendl;
    }
  }

  log_t.op_file_update(file->fnode);
  if (create)
    log_t.op_dir_link(dirname, filename, file->fnode.ino);

  *h = new FileWriter(file, bdev.size());
  dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
  return 0;
}

void BlueFS::_close_writer(FileWriter *h)
{
  dout(10) << __func__ << " " << h << dendl;
  for (unsigned i=0; i<bdev.size(); ++i) {
    h->iocv[i]->aio_wait();
    bdev[i]->queue_reap_ioc(h->iocv[i]);
  }
  h->iocv.clear();
  delete h;
}

int BlueFS::open_for_read(
  const string& dirname,
  const string& filename,
  FileReader **h,
  bool random)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename
	   << (random ? " (random)":" (sequential)") << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;

  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second.get();

  *h = new FileReader(file, random ? 4096 : g_conf->bluefs_max_prefetch,
		      random, false);
  dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
  return 0;
}

int BlueFS::rename(
  const string& old_dirname, const string& old_filename,
  const string& new_dirname, const string& new_filename)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << old_dirname << "/" << old_filename
	   << " -> " << new_dirname << "/" << new_filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(old_dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << old_dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef old_dir = p->second;
  map<string,FileRef>::iterator q = old_dir->file_map.find(old_filename);
  if (q == old_dir->file_map.end()) {
    dout(20) << __func__ << " dir " << old_dirname << " (" << old_dir
	     << ") file " << old_filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  FileRef file = q->second;

  p = dir_map.find(new_dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << new_dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef new_dir = p->second;
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
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
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
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " does not exist" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
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
  std::lock_guard<std::mutex> l(lock);
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  bool exists = p != dir_map.end();
  dout(10) << __func__ << " " << dirname << " = " << (int)exists << dendl;
  return exists;
}

int BlueFS::stat(const string& dirname, const string& filename,
		 uint64_t *size, utime_t *mtime)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " (" << dir
	     << ") file " << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  File *file = q->second.get();
  dout(10) << __func__ << " " << dirname << "/" << filename
	   << " " << file->fnode << dendl;
  if (size)
    *size = file->fnode.size;
  if (mtime)
    *mtime = file->fnode.mtime;
  return 0;
}

int BlueFS::lock_file(const string& dirname, const string& filename,
		      FileLock **plock)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
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
    file = q->second.get();
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
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << fl << " on " << fl->file->fnode << dendl;
  assert(fl->file->locked);
  fl->file->locked = false;
  delete fl;
  return 0;
}

int BlueFS::readdir(const string& dirname, vector<string> *ls)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << dendl;
  if (dirname.size() == 0) {
    // list dirs
    ls->reserve(dir_map.size() + 2);
    for (auto& q : dir_map) {
      ls->push_back(q.first);
    }
  } else {
    // list files in dir
    map<string,DirRef>::iterator p = dir_map.find(dirname);
    if (p == dir_map.end()) {
      dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
      return -ENOENT;
    }
    DirRef dir = p->second;
    ls->reserve(dir->file_map.size() + 2);
    for (auto& q : dir->file_map) {
      ls->push_back(q.first);
    }
  }
  ls->push_back(".");
  ls->push_back("..");
  return 0;
}

int BlueFS::unlink(const string& dirname, const string& filename)
{
  std::lock_guard<std::mutex> l(lock);
  dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
  map<string,DirRef>::iterator p = dir_map.find(dirname);
  if (p == dir_map.end()) {
    dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
    return -ENOENT;
  }
  DirRef dir = p->second;
  map<string,FileRef>::iterator q = dir->file_map.find(filename);
  if (q == dir->file_map.end()) {
    dout(20) << __func__ << " file " << dirname << "/" << filename
	     << " not found" << dendl;
    return -ENOENT;
  }
  FileRef file = q->second;
  dir->file_map.erase(filename);
  log_t.op_dir_unlink(dirname, filename);
  _drop_link(file);
  return 0;
}
