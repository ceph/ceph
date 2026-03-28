// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <stdio.h>
#include <string.h>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <memory>
#include <boost/random/mersenne_twister.hpp>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"

#include "os/bluestore/BlueStore.h"
#include "common/debug.h"

using namespace std;


int BlueStore::create_bdev_labels(CephContext *cct,
                                 const std::string& path,
                                 const std::vector<std::string>& devs,
				 std::vector<uint64_t>* valid_positions,
				 bool force)
{
  std::vector<std::string> metadata_files = {
    "bfm_blocks",
    "bfm_blocks_per_key",
    "bfm_bytes_per_block",
    "bfm_size",
    "bluefs",
    "ceph_fsid",
    "ceph_version_when_created",
    "created_at",
    "elastic_shared_blobs",
    "fsid",
    "kv_backend",
    "magic",
    "osd_key",
    "ready",
    "require_osd_release",
    "type",
    "whoami"
  };

  unique_ptr<BlockDevice> bdev(BlockDevice::create(cct, devs.front(), nullptr, nullptr, nullptr, nullptr));
  int r = bdev->open(devs.front());
  if (r < 0) {
    return r;
  }
  uint64_t size = bdev->get_size();

  if (bdev->supported_bdev_label() && !force) {
    bdev->close();
    return -EPERM;
  }

  bdev->close();

  bluestore_bdev_label_t label;
  std::vector<uint64_t> out_positions;
  bool is_multi = false;
  int64_t epoch = -1;

  r = BlueStore::read_bdev_label(cct, devs.front(), &label,
    &out_positions, &is_multi, &epoch);

  if (r == 0 && !force)
    return -EEXIST;

  label = bluestore_bdev_label_t();
  label.btime = ceph_clock_now();

  if (devs.front().ends_with("block")) {
    label.description = "main";
  } else if (devs.front().ends_with("block.db")) {
    label.description = "bluefs db";
  } else if (devs.front().ends_with("block.wal")) {
    label.description = "bluefs wal";
  }

  label.size = size;

  for (const auto& file : metadata_files) {
    std::ifstream infile(path + "/" + file);
    if (infile) {
      std::string value((std::istreambuf_iterator<char>(infile)), std::istreambuf_iterator<char>());
      value.erase(std::remove(value.begin(), value.end(), '\n'), value.end());
      if (file == "fsid") {
        label.osd_uuid.parse(value.c_str());
      } else if (label.description == "main") {
        label.meta[file] = value;
      }
    } else {
      cerr << "Warning: unable to read metadata file: " << file << std::endl;
    }
  }

  bool wrote_at_least_one = false;
  for (uint64_t position : *valid_positions) {
    r = BlueStore::write_bdev_label(cct, devs.front(), label, position);
    if (r < 0) {
      cerr << "unable to write label at 0x" << std::hex << position << std::dec
           << ": " << cpp_strerror(r) << std::endl;
    } else {
      wrote_at_least_one = true;
    }
  }
  return wrote_at_least_one ? 0 : -EIO;
}

static constexpr std::string vault_dir = "vault.slow";

#define dout_context cct
#define dout_subsys ceph_subsys_bluefs
#undef dout_prefix
#define dout_prefix *_dout << "bluefs-vault "

int BlueFS::vault_add(size_t new_reserve)
{
  static constexpr size_t MB_1 = 1024 * 1024;
  new_reserve = p2roundup<size_t>(new_reserve, MB_1); //operate in MBs
  size_t reserves;
  int last_file;
  int r;
  r = vault_inspect(reserves, last_file);
  if (r != 0) {
    r = mkdir(vault_dir);
    if (r != 0) {
      // cannot create dir, complain!
      derr << "Unable to create " << vault_dir << ", error=" << r << " " << strerror(r) << dendl;
      return -EIO;
    }
  }
  std::string mode = cct->_conf.get_val<std::string>("bluestore_vault_mode");
  if (mode != "0" && mode != "random" && mode != "reserve") {
    // bad mode, complain
    derr << "Unrecognized mode='" << mode << "'" << dendl;
    return -EINVAL;
  }

  BlueFS::FileWriter *h = nullptr;
  r = open_for_write(vault_dir, to_string(last_file + 1), &h, false);
  if (r != 0) {
    // report error and exit function
    derr << "Cannot create " << vault_dir << "/" << to_string(last_file + 1)
         << ", error=" << r << " " << strerror(r) << dendl;
    return -EIO;
  }
  ceph_assert(h != nullptr);
  if (mode == "0")
  {
    std::vector<char> buffer(MB_1);
    ceph_assert(buffer.size() == MB_1);
    size_t done_size = 0;
    reserve_on_main(h->file, new_reserve, false);
    while (done_size < new_reserve) {
      append_try_flush(h, buffer.data(), MB_1);
      done_size += MB_1;
    }
    close_writer(h);
  }
  else if (mode == "random")
  {
    boost::mt19937 random_source(0);
    std::vector<char> buffer(MB_1);
    ceph_assert(buffer.size() == MB_1);
    size_t done_size = 0;
    reserve_on_main(h->file, new_reserve, false);
    while (done_size < new_reserve) {
      random_source.generate(buffer.begin(), buffer.end());
      append_try_flush(h, buffer.data(), MB_1);
      done_size += MB_1;
    }
    close_writer(h);
  }
  else if (mode == "reserve")
  {
    reserve_on_main(h->file, new_reserve, true);
    close_writer(h);
  }
  else
  {
    ceph_abort();
  }
  vault_size += new_reserve;
  return 0;
}

int BlueFS::vault_release(size_t to_release, bool safe_mode)
{
  size_t released = 0;
  std::vector<std::string> files;
  readdir(vault_dir, &files);

  BlueFS::FileWriter* h = nullptr;
  // 1. Collect extents to discard into dirty.pending_release.
  for (auto& x : files) {
    size_t s;
    int r = stat(vault_dir, x, &s, nullptr);
    if (r != 0) {
      // skip dirs
      continue;
    }
    if (s + released <= to_release) {
      unlink(vault_dir, x);
      released += s;
    } else {
      // Truncate at proper offset to get proper release size.
      size_t offset = s - (to_release - released);
      open_for_write(vault_dir, x, &h, true);
      truncate(h, offset);
      if (safe_mode) {
        close_writer(h);
        h = nullptr;
      } else {
        // Have to wait to close writer to after we finish discarding.
        // The logic is to discard and release before we would (maybe) require allocation.
      }
      released += (s - offset);
      break;
    }
  }
  if (!safe_mode) {
    // 2. bluefs->dirty.pending_release has all pending allocations
    //    Instead of releasing it, just do discard, for now.
    discard_dirty_pending_release();
  }
  if (h) {
    // finally, close the writer
    close_writer(h);
    h = nullptr;
  }
  // Hopefully there is enough disk space to finish the transaction.
  sync_metadata(true);
  vault_size -= released;
  return 0;
}

int BlueFS::vault_inspect(size_t& reserve_size, int& last_file)
{
  std::vector<std::string> files;
  int r;
  reserve_size = 0;
  last_file = -1;
  r = readdir(vault_dir, &files);
  if (r != 0) {
    return r;
  }
  for (auto& x : files) {
    size_t s;
    if (stat(vault_dir, x, &s, nullptr) == 0) {
      // only interested in files, not dirs
      reserve_size += s;
      last_file = std::max(std::stoi(x), last_file);
    }
  }
  return 0;
}
