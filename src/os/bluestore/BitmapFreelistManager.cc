// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BitmapFreelistManager.h"
#include "kv/KeyValueDB.h"
#include "os/kv.h"

#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "freelist "

void make_offset_key(uint64_t offset, std::string *key)
{
  key->reserve(10);
  _key_encode_u64(offset, key);
}

struct XorMergeOperator : public KeyValueDB::MergeOperator {
  void merge_nonexistent(
    const char *rdata, size_t rlen, std::string *new_value) override {
    *new_value = std::string(rdata, rlen);
  }
  void merge(
    const char *ldata, size_t llen,
    const char *rdata, size_t rlen,
    std::string *new_value) override {
    ceph_assert(llen == rlen);
    *new_value = std::string(ldata, llen);
    for (size_t i = 0; i < rlen; ++i) {
      (*new_value)[i] ^= rdata[i];
    }
  }
  // We use each operator name and each prefix to construct the
  // overall RocksDB operator name for consistency check at open time.
  const char *name() const override {
    return "bitwise_xor";
  }
};

void BitmapFreelistManager::setup_merge_operator(KeyValueDB *db, string prefix)
{
  std::shared_ptr<XorMergeOperator> merge_op(new XorMergeOperator);
  db->set_merge_operator(prefix, merge_op);
}

BitmapFreelistManager::BitmapFreelistManager(CephContext* cct,
					     string meta_prefix,
					     string bitmap_prefix)
  : FreelistManager(cct),
    meta_prefix(meta_prefix),
    bitmap_prefix(bitmap_prefix),
    enumerate_bl_pos(0)
{
}

int BitmapFreelistManager::create(uint64_t new_size, uint64_t granularity,
				  KeyValueDB::Transaction txn)
{
  bytes_per_block = granularity;
  ceph_assert(isp2(bytes_per_block));
  size = p2align(new_size, bytes_per_block);
  blocks_per_key = cct->_conf->bluestore_freelist_blocks_per_key;

  _init_misc();

  blocks = size / bytes_per_block;
  if (blocks / blocks_per_key * blocks_per_key != blocks) {
    blocks = (blocks / blocks_per_key + 1) * blocks_per_key;
    dout(10) << __func__ << " rounding blocks up from 0x" << std::hex << size
	     << " to 0x" << (blocks * bytes_per_block)
	     << " (0x" << blocks << " blocks)" << std::dec << dendl;
    // set past-eof blocks as allocated
    _xor(size, blocks * bytes_per_block - size, txn);
  }
  dout(10) << __func__
	   << " size 0x" << std::hex << size
	   << " bytes_per_block 0x" << bytes_per_block
	   << " blocks 0x" << blocks
	   << " blocks_per_key 0x" << blocks_per_key
	   << std::dec << dendl;
  {
    bufferlist bl;
    encode(bytes_per_block, bl);
    txn->set(meta_prefix, "bytes_per_block", bl);
  }
  {
    bufferlist bl;
    encode(blocks_per_key, bl);
    txn->set(meta_prefix, "blocks_per_key", bl);
  }
  {
    bufferlist bl;
    encode(blocks, bl);
    txn->set(meta_prefix, "blocks", bl);
  }
  {
    bufferlist bl;
    encode(size, bl);
    txn->set(meta_prefix, "size", bl);
  }
  return 0;
}

int BitmapFreelistManager::expand(uint64_t new_size, KeyValueDB::Transaction txn)
{
  assert(new_size > size);
  ceph_assert(isp2(bytes_per_block));

  uint64_t blocks0 = size / bytes_per_block;
  if (blocks0 / blocks_per_key * blocks_per_key != blocks0) {
    blocks0 = (blocks / blocks_per_key + 1) * blocks_per_key;
    dout(10) << __func__ << " rounding blocks up from 0x" << std::hex << size
	     << " to 0x" << (blocks0 * bytes_per_block)
	     << " (0x" << blocks0 << " blocks)" << std::dec << dendl;
    // reset past-eof blocks to unallocated
    _xor(size, blocks0 * bytes_per_block - size, txn);
  }

  size = p2align(new_size, bytes_per_block);
  blocks = size / bytes_per_block;

  if (blocks / blocks_per_key * blocks_per_key != blocks) {
    blocks = (blocks / blocks_per_key + 1) * blocks_per_key;
    dout(10) << __func__ << " rounding blocks up from 0x" << std::hex << size
	     << " to 0x" << (blocks * bytes_per_block)
	     << " (0x" << blocks << " blocks)" << std::dec << dendl;
    // set past-eof blocks as allocated
    _xor(size, blocks * bytes_per_block - size, txn);
  }

  dout(10) << __func__
	   << " size 0x" << std::hex << size
	   << " bytes_per_block 0x" << bytes_per_block
	   << " blocks 0x" << blocks
	   << " blocks_per_key 0x" << blocks_per_key
	   << std::dec << dendl;
  {
    bufferlist bl;
    encode(blocks, bl);
    txn->set(meta_prefix, "blocks", bl);
  }
  {
    bufferlist bl;
    encode(size, bl);
    txn->set(meta_prefix, "size", bl);
  }
  return 0;
}

int BitmapFreelistManager::init(KeyValueDB *kvdb)
{
  dout(1) << __func__ << dendl;

  KeyValueDB::Iterator it = kvdb->get_iterator(meta_prefix);
  it->lower_bound(string());

  // load meta
  while (it->valid()) {
    string k = it->key();
    if (k == "bytes_per_block") {
      bufferlist bl = it->value();
      auto p = bl.cbegin();
      decode(bytes_per_block, p);
      dout(10) << __func__ << " bytes_per_block 0x" << std::hex
	       << bytes_per_block << std::dec << dendl;
    } else if (k == "blocks") {
      bufferlist bl = it->value();
      auto p = bl.cbegin();
      decode(blocks, p);
      dout(10) << __func__ << " blocks 0x" << std::hex << blocks << std::dec
	       << dendl;
    } else if (k == "size") {
      bufferlist bl = it->value();
      auto p = bl.cbegin();
      decode(size, p);
      dout(10) << __func__ << " size 0x" << std::hex << size << std::dec
	       << dendl;
    } else if (k == "blocks_per_key") {
      bufferlist bl = it->value();
      auto p = bl.cbegin();
      decode(blocks_per_key, p);
      dout(10) << __func__ << " blocks_per_key 0x" << std::hex << blocks_per_key
	       << std::dec << dendl;
    } else {
      derr << __func__ << " unrecognized meta " << k << dendl;
      return -EIO;
    }
    it->next();
  }

  dout(10) << __func__ << std::hex
	   << " size 0x" << size
	   << " bytes_per_block 0x" << bytes_per_block
	   << " blocks 0x" << blocks
	   << " blocks_per_key 0x" << blocks_per_key
	   << std::dec << dendl;
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

  bytes_per_key = bytes_per_block * blocks_per_key;
  key_mask = ~(bytes_per_key - 1);
  dout(10) << __func__ << std::hex << " bytes_per_key 0x" << bytes_per_key
	   << ", key_mask 0x" << key_mask << std::dec
	   << dendl;
}

void BitmapFreelistManager::shutdown()
{
  dout(1) << __func__ << dendl;
}

void BitmapFreelistManager::enumerate_reset()
{
  std::lock_guard l(lock);
  enumerate_offset = 0;
  enumerate_bl_pos = 0;
  enumerate_bl.clear();
  enumerate_p.reset();
}

int get_next_clear_bit(bufferlist& bl, int start)
{
  const char *p = bl.c_str();
  int bits = bl.length() << 3;
  while (start < bits) {
    // byte = start / 8 (or start >> 3)
    // bit = start % 8 (or start & 7)
    unsigned char byte_mask = 1 << (start & 7);
    if ((p[start >> 3] & byte_mask) == 0) {
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
    int which_byte = start / 8;
    int which_bit = start % 8;
    unsigned char byte_mask = 1 << which_bit;
    if (p[which_byte] & byte_mask) {
      return start;
    }
    ++start;
  }
  return -1; // not found
}

bool BitmapFreelistManager::enumerate_next(KeyValueDB *kvdb, uint64_t *offset, uint64_t *length)
{
  std::lock_guard l(lock);

  // initial base case is a bit awkward
  if (enumerate_offset == 0 && enumerate_bl_pos == 0) {
    dout(10) << __func__ << " start" << dendl;
    enumerate_p = kvdb->get_iterator(bitmap_prefix);
    enumerate_p->lower_bound(string());
    // we assert that the first block is always allocated; it's true,
    // and it simplifies our lives a bit.
    ceph_assert(enumerate_p->valid());
    string k = enumerate_p->key();
    const char *p = k.c_str();
    _key_decode_u64(p, &enumerate_offset);
    enumerate_bl = enumerate_p->value();
    ceph_assert(enumerate_offset == 0);
    ceph_assert(get_next_set_bit(enumerate_bl, 0) == 0);
  }

  if (enumerate_offset >= size) {
    dout(10) << __func__ << " end" << dendl;
    return false;
  }

  // skip set bits to find offset
  while (true) {
    enumerate_bl_pos = get_next_clear_bit(enumerate_bl, enumerate_bl_pos);
    if (enumerate_bl_pos >= 0) {
      *offset = _get_offset(enumerate_offset, enumerate_bl_pos);
      dout(30) << __func__ << " found clear bit, key 0x" << std::hex
	       << enumerate_offset << " bit 0x" << enumerate_bl_pos
	       << " offset 0x" << *offset
	       << std::dec << dendl;
      break;
    }
    dout(30) << " no more clear bits in 0x" << std::hex << enumerate_offset
	     << std::dec << dendl;
    enumerate_p->next();
    enumerate_bl.clear();
    if (!enumerate_p->valid()) {
      enumerate_offset += bytes_per_key;
      enumerate_bl_pos = 0;
      *offset = _get_offset(enumerate_offset, enumerate_bl_pos);
      break;
    }
    string k = enumerate_p->key();
    const char *p = k.c_str();
    uint64_t next = enumerate_offset + bytes_per_key;
    _key_decode_u64(p, &enumerate_offset);
    enumerate_bl = enumerate_p->value();
    enumerate_bl_pos = 0;
    if (enumerate_offset > next) {
      dout(30) << " no key at 0x" << std::hex << next << ", got 0x"
	       << enumerate_offset << std::dec << dendl;
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
	end = _get_offset(enumerate_offset, enumerate_bl_pos);
	dout(30) << __func__ << " found set bit, key 0x" << std::hex
		 << enumerate_offset << " bit 0x" << enumerate_bl_pos
		 << " offset 0x" << end << std::dec
		 << dendl;
	end = std::min(get_alloc_units() * bytes_per_block, end);
	*length = end - *offset;
        dout(10) << __func__ << std::hex << " 0x" << *offset << "~" << *length
		 << std::dec << dendl;
	return true;
      }
      dout(30) << " no more set bits in 0x" << std::hex << enumerate_offset
	       << std::dec << dendl;
      enumerate_p->next();
      enumerate_bl.clear();
      enumerate_bl_pos = 0;
      if (!enumerate_p->valid()) {
	break;
      }
      string k = enumerate_p->key();
      const char *p = k.c_str();
      _key_decode_u64(p, &enumerate_offset);
      enumerate_bl = enumerate_p->value();
    }
  }

  if (enumerate_offset < size) {
    end = get_alloc_units() * bytes_per_block;
    *length = end - *offset;
    dout(10) << __func__ << std::hex << " 0x" << *offset << "~" << *length
	     << std::dec << dendl;
    enumerate_offset = size;
    enumerate_bl_pos = blocks_per_key;
    return true;
  }

  dout(10) << __func__ << " end" << dendl;
  return false;
}

void BitmapFreelistManager::dump(KeyValueDB *kvdb)
{
  enumerate_reset();
  uint64_t offset, length;
  while (enumerate_next(kvdb, &offset, &length)) {
    dout(20) << __func__ << " 0x" << std::hex << offset << "~" << length
	     << std::dec << dendl;
  }
}

void BitmapFreelistManager::_verify_range(KeyValueDB *kvdb,
					  uint64_t offset, uint64_t length,
					  int val)
{
  unsigned errors = 0;
  uint64_t first_key = offset & key_mask;
  uint64_t last_key = (offset + length - 1) & key_mask;
  if (first_key == last_key) {
    string k;
    make_offset_key(first_key, &k);
    bufferlist bl;
    kvdb->get(bitmap_prefix, k, &bl);
    if (bl.length() > 0) {
      const char *p = bl.c_str();
      unsigned s = (offset & ~key_mask) / bytes_per_block;
      unsigned e = ((offset + length - 1) & ~key_mask) / bytes_per_block;
      for (unsigned i = s; i <= e; ++i) {
	int has = !!(p[i >> 3] & (1ull << (i & 7)));
	if (has != val) {
	  derr << __func__ << " key 0x" << std::hex << first_key << " bit 0x"
	       << i << " has 0x" << has << " expected 0x" << val
	       << std::dec << dendl;
	  ++errors;
	}
      }
    } else {
      if (val) {
	derr << __func__ << " key 0x" << std::hex << first_key
	     << " not present, expected 0x" << val << std::dec << dendl;
	++errors;
      }
    }
  } else {
    // first key
    {
      string k;
      make_offset_key(first_key, &k);
      bufferlist bl;
      kvdb->get(bitmap_prefix, k, &bl);
      if (bl.length()) {
	const char *p = bl.c_str();
	unsigned s = (offset & ~key_mask) / bytes_per_block;
	unsigned e = blocks_per_key;
	for (unsigned i = s; i < e; ++i) {
	  int has = !!(p[i >> 3] & (1ull << (i & 7)));
	  if (has != val) {
	    derr << __func__ << " key 0x" << std::hex << first_key << " bit 0x"
		 << i << " has 0x" << has << " expected 0x" << val << std::dec
		 << dendl;
	    ++errors;
	  }
	}
      } else {
	if (val) {
	  derr << __func__ << " key 0x" << std::hex << first_key
	       << " not present, expected 0x" << val << std::dec << dendl;
	  ++errors;
	}
      }
      first_key += bytes_per_key;
    }
    // middle keys
    if (first_key < last_key) {
      while (first_key < last_key) {
	string k;
	make_offset_key(first_key, &k);
	bufferlist bl;
	kvdb->get(bitmap_prefix, k, &bl);
	if (bl.length() > 0) {
	  const char *p = bl.c_str();
	  for (unsigned i = 0; i < blocks_per_key; ++i) {
	    int has = !!(p[i >> 3] & (1ull << (i & 7)));
	    if (has != val) {
	      derr << __func__ << " key 0x" << std::hex << first_key << " bit 0x"
		   << i << " has 0x" << has << " expected 0x" << val
		   << std::dec << dendl;
	      ++errors;
	    }
	  }
	} else {
	  if (val) {
	    derr << __func__ << " key 0x" << std::hex << first_key
		 << " not present, expected 0x" << val << std::dec << dendl;
	    ++errors;
	  }
	}
	first_key += bytes_per_key;
      }
    }
    ceph_assert(first_key == last_key);
    {
      string k;
      make_offset_key(first_key, &k);
      bufferlist bl;
      kvdb->get(bitmap_prefix, k, &bl);
      if (bl.length() > 0) {
	const char *p = bl.c_str();
	unsigned e = ((offset + length - 1) & ~key_mask) / bytes_per_block;
	for (unsigned i = 0; i < e; ++i) {
	  int has = !!(p[i >> 3] & (1ull << (i & 7)));
	  if (has != val) {
	    derr << __func__ << " key 0x" << std::hex << first_key << " bit 0x"
		 << i << " has 0x" << has << " expected 0x" << val << std::dec
		 << dendl;
	    ++errors;
	  }
	}
      } else {
	if (val) {
	  derr << __func__ << " key 0x" << std::hex << first_key
	       << " not present, expected 0x" << val << std::dec << dendl;
	  ++errors;
	}
      }
    }
  }
  if (errors) {
    derr << __func__ << " saw " << errors << " errors" << dendl;
    ceph_abort_msg("bitmap freelist errors");
  }
}

void BitmapFreelistManager::allocate(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  _xor(offset, length, txn);
}

void BitmapFreelistManager::release(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
  _xor(offset, length, txn);
}

void BitmapFreelistManager::_xor(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  // must be block aligned
  ceph_assert((offset & block_mask) == offset);
  ceph_assert((length & block_mask) == length);

  uint64_t first_key = offset & key_mask;
  uint64_t last_key = (offset + length - 1) & key_mask;
  dout(20) << __func__ << " first_key 0x" << std::hex << first_key
	   << " last_key 0x" << last_key << std::dec << dendl;

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
    dout(30) << __func__ << " 0x" << std::hex << first_key << std::dec << ": ";
    bl.hexdump(*_dout, false);
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
      dout(30) << __func__ << " 0x" << std::hex << first_key << std::dec << ": ";
      bl.hexdump(*_dout, false);
      *_dout << dendl;
      txn->merge(bitmap_prefix, k, bl);
      first_key += bytes_per_key;
    }
    // middle keys
    while (first_key < last_key) {
      string k;
      make_offset_key(first_key, &k);
      dout(30) << __func__ << " 0x" << std::hex << first_key << std::dec
      	 << ": ";
      all_set_bl.hexdump(*_dout, false);
      *_dout << dendl;
      txn->merge(bitmap_prefix, k, all_set_bl);
      first_key += bytes_per_key;
    }
    ceph_assert(first_key == last_key);
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
      dout(30) << __func__ << " 0x" << std::hex << first_key << std::dec << ": ";
      bl.hexdump(*_dout, false);
      *_dout << dendl;
      txn->merge(bitmap_prefix, k, bl);
    }
  }
}
