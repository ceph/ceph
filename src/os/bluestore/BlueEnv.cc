// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <string.h>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"

#include "os/bluestore/BlueStore.h"

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
