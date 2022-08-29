// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FileFreelistManager.h"
#include "kv/KeyValueDB.h"
#include "os/kv.h"
#include "include/stringify.h"

#include "BlueStore.h"

#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "filefl "

using std::string;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::decode;
using ceph::encode;

FileFreelistManager::FileFreelistManager(BlueStore* store)
  : FreelistManager(store)
{
}

uint64_t FileFreelistManager::size_2_block_count(uint64_t target_size) const
{
  auto target_blocks = target_size / bytes_per_block;
  if (target_blocks / blocks_per_key * blocks_per_key != target_blocks) {
    target_blocks = (target_blocks / blocks_per_key + 1) * blocks_per_key;
  }
  return target_blocks;
}

int FileFreelistManager::create(uint64_t new_size, uint64_t granularity,
				  uint64_t zone_size, uint64_t first_sequential_zone,
				  KeyValueDB::Transaction txn)
{
  size = new_size;
  bytes_per_block = granularity;
  dout(1) << __func__
	   << " size 0x" << std::hex << size
	   << std::dec << dendl;
  blocks_per_key = 1;
  blocks = size_2_block_count(size);
  dout(1) << __func__
	   << " size 0x" << std::hex << size
	   << " bytes_per_block 0x" << bytes_per_block
	   << " blocks 0x" << blocks
	   << " blocks_per_key 0x" << blocks_per_key
	   << std::dec << dendl;
  std::string meta_prefix = "B"; //copied, to be deleted in future
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

int FileFreelistManager::init(KeyValueDB *kvdb, bool db_in_read_only,
  std::function<int(const std::string&, std::string*)> cfg_reader)
{
  dout(1) << __func__ << dendl;
  int r = _read_cfg(cfg_reader);
  if (r != 0) {
    derr << __func__ << " failed to read cfg" << dendl;
    return r;
  }
  dout(10) << __func__ << std::hex
	   << " size 0x" << size
	   << " bytes_per_block 0x" << bytes_per_block
	   << " blocks 0x" << blocks
	   << " blocks_per_key 0x" << blocks_per_key
	   << std::dec << dendl;
  return 0;
}

int FileFreelistManager::_read_cfg(
  std::function<int(const std::string&, std::string*)> cfg_reader)
{
  dout(1) << __func__ << dendl;

  string err;

  const size_t key_count = 4;
  string keys[key_count] = {
    "bfm_size",
    "bfm_blocks",
    "bfm_bytes_per_block",
    "bfm_blocks_per_key"};
  uint64_t* vals[key_count] = {
    &size,
    &blocks,
    &bytes_per_block,
    &blocks_per_key};

  for (size_t i = 0; i < key_count; i++) {
    string val;
    int r = cfg_reader(keys[i], &val);
    if (r == 0) {
      *(vals[i]) = strict_iecstrtoll(val, &err);
      if (!err.empty()) {
        derr << __func__ << " Failed to parse - "
          << keys[i] << ":" << val
          << ", error: " << err << dendl;
        return -EINVAL;
      }
    } else {
      // this is expected for legacy deployed OSDs
      dout(0) << __func__ << " " << keys[i] << " not found in bdev meta" << dendl;
      return r;
    }
  }

  return 0;
}

void FileFreelistManager::sync(KeyValueDB* kvdb)
{
}

void FileFreelistManager::shutdown()
{
  dout(1) << __func__ << dendl;
}

void FileFreelistManager::enumerate_reset()
{
}


bool FileFreelistManager::enumerate_next(KeyValueDB *kvdb, uint64_t *offset, uint64_t *length)
{
  return true;
}

void FileFreelistManager::dump(KeyValueDB *kvdb)
{
}

void FileFreelistManager::allocate(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
}

void FileFreelistManager::release(
  uint64_t offset, uint64_t length,
  KeyValueDB::Transaction txn)
{
  dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
}

//TODO get_meta copied from BitmapFreelistManager is adapted for block device expand
//TODO needs to be simplified
void FileFreelistManager::get_meta(
  uint64_t target_size,
  std::vector<std::pair<string, string>>* res) const
{
  if (target_size == 0) {
    res->emplace_back("bfm_blocks", stringify(blocks));
    res->emplace_back("bfm_size", stringify(size));
  } else {
    target_size = p2align(target_size, bytes_per_block);
    auto target_blocks = size_2_block_count(target_size);

    res->emplace_back("bfm_blocks", stringify(target_blocks));
    res->emplace_back("bfm_size", stringify(target_size));
  }
  res->emplace_back("bfm_bytes_per_block", stringify(bytes_per_block));
  res->emplace_back("bfm_blocks_per_key", stringify(blocks_per_key));
}
