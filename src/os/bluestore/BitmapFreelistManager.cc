// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BitmapFreelistManager.h"
#include "kv/KeyValueDB.h"
#include "kv.h"

#include "common/debug.h"

#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "freelist "

void make_offset_key(uint64_t offset, std::string *key)
{
  key->reserve(10);
  _key_encode_u64(offset, key);
}

struct XorMergeOperator : public KeyValueDB::MergeOperator {
  virtual void merge_nonexistant(
    const char *rdata, size_t rlen, std::string *new_value) override {
    *new_value = std::string(rdata, rlen);
  }
  virtual void merge(
    const char *ldata, size_t llen,
    const char *rdata, size_t rlen,
    std::string *new_value) {
    assert(llen == rlen);
    *new_value = std::string(ldata, llen);
    for (size_t i = 0; i < rlen; ++i) {
      (*new_value)[i] ^= rdata[i];
    }
  }
  // We use each operator name and each prefix to construct the
  // overall RocksDB operator name for consistency check at open time.
  virtual string name() const {
    return "bitwise_xor";
  }
};

void BitmapFreelistManager::setup_merge_operator(KeyValueDB *db, string prefix)
{
  ceph::shared_ptr<XorMergeOperator> merge_op(new XorMergeOperator);
  db->set_merge_operator(prefix, merge_op);
}

BitmapFreelistManager::BitmapFreelistManager(KeyValueDB *db,
					     string meta_prefix,
					     string bitmap_prefix)
  : meta_prefix(meta_prefix),
    bitmap_prefix(bitmap_prefix),
    kvdb(db)
{
}

int BitmapFreelistManager::create(uint64_t new_size, KeyValueDB::Transaction txn)
{
  size = new_size;
  bytes_per_block = g_conf->bdev_block_size;
  blocks_per_key = g_conf->bluestore_freelist_blocks_per_key;

  _init_misc();

  blocks = size / bytes_per_block;
  if (blocks / blocks_per_key * blocks_per_key != blocks) {
    blocks = (blocks / blocks_per_key + 1) * blocks_per_key;
    dout(10) << __func__ << " rounding blocks up from " << size
	     << " to " << (blocks * bytes_per_block)
	     << " (" << blocks << " blocks)" << dendl;
    // set past-eof blocks as allocated
    _xor(size, blocks * bytes_per_block - size, txn);
  }
  dout(10) << __func__
	   << " size " << size
	   << " bytes_per_block " << bytes_per_block
	   << " blocks " << blocks
	   << " blocks_per_key " << blocks_per_key
	   << dendl;
  {
    bufferlist bl;
    ::encode(bytes_per_block, bl);
    txn->set(meta_prefix, "bytes_per_block", bl);
  }
  {
    bufferlist bl;
    ::encode(blocks_per_key, bl);
    txn->set(meta_prefix, "blocks_per_key", bl);
  }
  {
    bufferlist bl;
    ::encode(blocks, bl);
    txn->set(meta_prefix, "blocks", bl);
  }
  {
    bufferlist bl;
    ::encode(size, bl);
    txn->set(meta_prefix, "size", bl);
  }
  return 0;
}

int BitmapFreelistManager::init()
{
  dout(1) << __func__ << dendl;

  KeyValueDB::Iterator it = kvdb->get_iterator(meta_prefix);
  it->lower_bound(string());

  // load meta
  while (it->valid()) {
    string k = it->key();
    if (k == "bytes_per_block") {
      bufferlist bl = it->value();
      bufferlist::iterator p = bl.begin();
      ::decode(bytes_per_block, p);
      dout(10) << __func__ << " bytes_per_block " << bytes_per_block << dendl;
    } else if (k == "blocks") {
      bufferlist bl = it->value();
      bufferlist::iterator p = bl.begin();
      ::decode(blocks, p);
      dout(10) << __func__ << " blocks " << blocks << dendl;
    } else if (k == "size") {
      bufferlist bl = it->value();
      bufferlist::iterator p = bl.begin();
      ::decode(size, p);
      dout(10) << __func__ << " size " << size << dendl;
    } else if (k == "blocks_per_key") {
      bufferlist bl = it->value();
      bufferlist::iterator p = bl.begin();
      ::decode(blocks_per_key, p);
      dout(10) << __func__ << " blocks_per_key " << blocks_per_key << dendl;
    } else {
      derr << __func__ << " unrecognized meta " << k << dendl;
      return -EIO;
    }
    it->next();
  }

  dout(10) << __func__
	   << " size " << size
	   << " bytes_per_block " << bytes_per_block
	   << " blocks " << blocks
	   << " blocks_per_key " << blocks_per_key
	   << dendl;
  _init_misc();
  return 0;
}

void BitmapFreelistManager::_init_misc()
{
  bufferptr z(blocks_per_key >> 3);
  memset(z.c_str(), 0xff, z.length());
  all_set_bl.clear();
  all_set_bl.append(z);

  block_mask = ~(bytes_per_block - 1);

  uint64_t bytes_per_key = bytes_per_block * blocks_per_key;
  key_mask = ~(bytes_per_key - 1);
  dout(10) << __func__ << " bytes_per_key " << bytes_per_key
	   << ", key_mask 0x" << std::hex << key_mask << std::dec
	   << dendl;
}

void BitmapFreelistManager::shutdown()
{
  dout(1) << __func__ << dendl;
}

void BitmapFreelistManager::enumerate_reset()
{
  std::lock_guard<std::mutex> l(lock);
  enumerate_offset = 0;
  enumerate_bl.clear();
}

int get_next_clear_bit(bufferlist& bl, int start)
{
  const char *p = bl.c_str();
  int bits = bl.length() << 3;
  while (start < bits) {
    int byte = start >> 3;
    unsigned char mask = 1 << (start & 7);
    if ((p[byte] & mask) == 0) {
      return start;
    }
    ++start;
  }
  return -1; // not found
}

int get_next_set_bit(bufferlist& bl, int start)
{
  const char *p = bl.c_str();
  int bits = bl.length() << 3;
  while (start < bits) {
    int byte = start >> 3;
    unsigned char mask = 1 << (start & 7);
    if (p[byte] & mask) {
      return start;
    }
    ++start;
  }
  return -1; // not found
}

bool BitmapFreelistManager::enumerate_next(uint64_t *offset, uint64_t *length)
{
  std::lock_guard<std::mutex> l(lock);

  // initial base case is a bit awkward
  if (enumerate_offset == 0) {
    dout(10) << __func__ << " start" << dendl;
    enumerate_p = kvdb->get_iterator(bitmap_prefix);
    enumerate_p->lower_bound(string());
    // we assert that the first block is always allocated; it's true,
    // and it simplifies our lives a bit.
    assert(enumerate_p->valid());
    string k = enumerate_p->key();
    const char *p = k.c_str() + 1;
    _key_decode_u64(p, &enumerate_offset);
    enumerate_bl = enumerate_p->value();
    enumerate_bl_pos = 0;
    assert(enumerate_offset == 0);
    assert(get_next_set_bit(enumerate_bl, 0) == 0);
  }

  if (enumerate_offset >= size) {
    dout(10) << __func__ << " end" << dendl;
    return false;
  }

  // skip set bits to find offset
  while (true) {
    enumerate_bl_pos = get_next_clear_bit(enumerate_bl, enumerate_bl_pos);
    if (enumerate_bl_pos >= 0) {
      *offset = get_offset(enumerate_offset, enumerate_bl_pos);
      dout(30) << __func__ << " found clear bit, key " << enumerate_offset
	       << " bit " << enumerate_bl_pos
	       << " offset " << *offset
	       << dendl;
      break;
    }
    dout(30) << " no more clear bits in " << enumerate_offset << dendl;
    enumerate_p->next();
    enumerate_bl.clear();
    if (!enumerate_p->valid()) {
      enumerate_offset += bytes_per_block * blocks_per_key;
      enumerate_bl_pos = 0;
      *offset = get_offset(enumerate_offset, enumerate_bl_pos);
      break;
    }
    string k = enumerate_p->key();
    const char *p = k.c_str() + 1;
    uint64_t next = enumerate_offset + bytes_per_block * blocks_per_key;
    _key_decode_u64(p, &enumerate_offset);
    enumerate_bl = enumerate_p->value();
    enumerate_bl_pos = 0;
    if (enumerate_offset > next) {
      dout(30) << " no key at " << next << ", got " << enumerate_offset << dendl;
      *offset = next;
      break;
    }
  }

  // skip clear bits to find the end
  uint64_t end = 0;
  if (enumerate_p->valid()) {
    while (true) {
      enumerate_bl_pos = get_next_set_bit(enumerate_bl, enumerate_bl_pos);
      if (enumerate_bl_pos >= 0) {
	end = get_offset(enumerate_offset, enumerate_bl_pos);
	dout(30) << __func__ << " found set bit, key " << enumerate_offset
		 << " bit " << enumerate_bl_pos
		 << " offset " << end
		 << dendl;
	*length = end - *offset;
	dout(10) << __func__ << " " << *offset << "~" << *length << dendl;
	return true;
      }
      dout(30) << " no more set bits in " << enumerate_offset << dendl;
      enumerate_p->next();
      enumerate_bl.clear();
      enumerate_bl_pos = 0;
      if (!enumerate_p->valid()) {
	break;
      }
      string k = enumerate_p->key();
      const char *p = k.c_str() + 1;
      _key_decode_u64(p, &enumerate_offset);
      enumerate_bl = enumerate_p->value();
    }
  }

  end = size;
  if (enumerate_offset < end) {
    *length = end - enumerate_offset;
    dout(10) << __func__ << " " << *offset << "~" << *length << dendl;
    enumerate_offset = end;
    return true;
  }

  dout(10) << __func__ << " end" << dendl;
  return false;
}

void BitmapFreelistManager::dump()
{
  enumerate_reset();
  uint64_t offset, length;
  while (enumerate_next(&offset, &length)) {
    dout(20) << __func__ << " " << offset << "~" << length << dendl;
  }
}

void BitmapFreelistManager::allocate(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  dout(10) << __func__ << " " << offset << "~" << length << dendl;
  _xor(offset, length, txn);
}

void BitmapFreelistManager::release(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  dout(10) << __func__ << " " << offset << "~" << length << dendl;
  _xor(offset, length, txn);
}

void BitmapFreelistManager::_xor(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  // must be block aligned
  assert((offset & block_mask) == offset);
  assert((length & block_mask) == length);

  uint64_t first_key = offset & key_mask;
  uint64_t last_key = (offset + length - 1) & key_mask;
  dout(20) << __func__ << " first_key " << first_key << " last_key " << last_key
	   << dendl;

  if (first_key == last_key) {
    bufferptr p(blocks_per_key >> 3);
    p.zero();
    unsigned s = (offset & ~key_mask) / bytes_per_block;
    unsigned e = ((offset + length - 1) & ~key_mask) / bytes_per_block;
    for (unsigned i = s; i <= e; ++i) {
      p[i >> 3] ^= 1ull << (i & 7);
    }
    string k;
    make_offset_key(first_key, &k);
    bufferlist bl;
    bl.append(p);
    dout(30) << __func__ << " " << first_key << ": ";
    bl.hexdump(*_dout);
    *_dout << dendl;
    txn->merge(bitmap_prefix, k, bl);
  } else {
    // first key
    {
      bufferptr p(blocks_per_key >> 3);
      p.zero();
      unsigned s = (offset & ~key_mask) / bytes_per_block;
      unsigned e = blocks_per_key;
      for (unsigned i = s; i < e; ++i) {
	p[i >> 3] ^= 1ull << (i & 7);
      }
      string k;
      make_offset_key(first_key, &k);
      bufferlist bl;
      bl.append(p);
      dout(30) << __func__ << " " << first_key << ": ";
      bl.hexdump(*_dout);
      *_dout << dendl;
      txn->merge(bitmap_prefix, k, bl);
      first_key += bytes_per_block * blocks_per_key;
    }
    // middle keys
    if (first_key < last_key) {
      while (first_key < last_key) {
	string k;
	make_offset_key(first_key, &k);
	dout(30) << __func__ << " " << first_key << ": ";
	all_set_bl.hexdump(*_dout);
	*_dout << dendl;
	txn->merge(bitmap_prefix, k, all_set_bl);
	first_key += bytes_per_block * blocks_per_key;
      }
    }
    assert(first_key == last_key);
    {
      bufferptr p(blocks_per_key >> 3);
      p.zero();
      unsigned e = ((offset + length - 1) & ~key_mask) / bytes_per_block;
      for (unsigned i = 0; i <= e; ++i) {
	p[i >> 3] ^= 1ull << (i & 7);
      }
      string k;
      make_offset_key(first_key, &k);
      bufferlist bl;
      bl.append(p);
      dout(30) << __func__ << " " << first_key << ": ";
      bl.hexdump(*_dout);
      *_dout << dendl;
      txn->merge(bitmap_prefix, k, bl);
    }
  }
}
