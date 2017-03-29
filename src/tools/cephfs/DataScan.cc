// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/errno.h"
#include "common/ceph_argparse.h"
#include <fstream>
#include "include/util.h"

#include "mds/CInode.h"
#include "cls/cephfs/cls_cephfs_client.h"

#include "DataScan.h"
#include "include/compat.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "datascan." << __func__ << ": "

void DataScan::usage()
{
  std::cout << "Usage: \n"
    << "  cephfs-data-scan init [--force-init]\n"
    << "  cephfs-data-scan scan_extents [--force-pool] <data pool name>\n"
    << "  cephfs-data-scan scan_inodes [--force-pool] [--force-corrupt] <data pool name>\n"
    << "\n"
    << "    --force-corrupt: overrite apparently corrupt structures\n"
    << "    --force-init: write root inodes even if they exist\n"
    << "    --force-pool: use data pool even if it is not in FSMap\n"
    << "\n"
    << "  cephfs-data-scan scan_frags [--force-corrupt]\n"
    << "\n"
    << "  cephfs-data-scan tmap_upgrade <metadata_pool>\n"
    << std::endl;

  generic_client_usage();
}

bool DataScan::parse_kwarg(
    const std::vector<const char*> &args,
    std::vector<const char *>::const_iterator &i,
    int *r)
{
  if (i + 1 == args.end()) {
    return false;
  }

  const std::string arg(*i);
  const std::string val(*(i + 1));

  if (arg == std::string("--output-dir")) {
    if (driver != NULL) {
      derr << "Unexpected --output-dir: output already selected!" << dendl;
      *r = -EINVAL;
      return false;
    }
    dout(4) << "Using local file output to '" << val << "'" << dendl;
    driver = new LocalFileDriver(val, data_io);
    return true;
  } else if (arg == std::string("--worker_n")) {
    std::string err;
    n = strict_strtoll(val.c_str(), 10, &err);
    if (!err.empty()) {
      std::cerr << "Invalid worker number '" << val << "'" << std::endl;
      *r = -EINVAL;
      return false;
    }
    return true;
  } else if (arg == std::string("--worker_m")) {
    std::string err;
    m = strict_strtoll(val.c_str(), 10, &err);
    if (!err.empty()) {
      std::cerr << "Invalid worker count '" << val << "'" << std::endl;
      *r = -EINVAL;
      return false;
    }
    return true;
  } else if (arg == std::string("--filter-tag")) {
    filter_tag = val;
    dout(10) << "Applying tag filter: '" << filter_tag << "'" << dendl;
    return true;
  } else if (arg == std::string("--filesystem")) {
    std::shared_ptr<const Filesystem> fs;
    *r = fsmap->parse_filesystem(val, &fs);
    if (*r != 0) {
      std::cerr << "Invalid filesystem '" << val << "'" << std::endl;
      return false;
    }
    fscid = fs->fscid;
    return true;
  } else {
    return false;
  }
}

bool DataScan::parse_arg(
    const std::vector<const char*> &args,
    std::vector<const char *>::const_iterator &i)
{
  const std::string arg(*i);
  if (arg == "--force-pool") {
    force_pool = true;
    return true;
  } else if (arg == "--force-corrupt") {
    force_corrupt = true;
    return true;
  } else if (arg == "--force-init") {
    force_init = true;
    return true;
  } else {
    return false;
  }
}

int DataScan::main(const std::vector<const char*> &args)
{
  // Parse args
  // ==========
  if (args.size() < 1) {
    usage();
    return -EINVAL;
  }

  // Common RADOS init: open metadata pool
  // =====================================
  librados::Rados rados;
  int r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "RADOS unavailable" << dendl;
    return r;
  }

  std::string const &command = args[0];
  std::string data_pool_name;
  std::string metadata_pool_name;

  // Consume any known --key val or --flag arguments
  for (std::vector<const char *>::const_iterator i = args.begin() + 1;
       i != args.end(); ++i) {
    if (parse_kwarg(args, i, &r)) {
      // Skip the kwarg value field
      ++i;
      continue;
    } else if (r) {
      return r;
    }

    if (parse_arg(args, i)) {
      continue;
    }

    // Trailing positional argument
    if (i + 1 == args.end() &&
        (command == "scan_inodes" || command == "scan_extents")) {
      data_pool_name = *i;
      continue;
    }

    // Trailing positional argument
    if (i + 1 == args.end() && (command == "tmap_upgrade")) {
      metadata_pool_name = *i;
      continue;
    }

    // Fall through: unhandled
    std::cerr << "Unknown argument '" << *i << "'" << std::endl;
    return -EINVAL;
  }

  if (command == "tmap_upgrade") {
    // Special case tmap_upgrade away from other modes, as this is a
    // specialized command that will only exist in the Jewel series,
    // and doesn't require the initialization of the `driver` member
    // that is done below.
    rados.connect();

    // Initialize metadata_io from pool on command line
    if (metadata_pool_name.empty()) {
      std::cerr << "Metadata pool not specified" << std::endl;
      usage();
      return -EINVAL;
    }

    long metadata_pool_id = rados.pool_lookup(metadata_pool_name.c_str());
    if (metadata_pool_id < 0) {
      std::cerr << "Pool '" << metadata_pool_name << "' not found!" << std::endl;
      return -ENOENT;
    } else {
      dout(4) << "pool '" << metadata_pool_name
        << "' has ID " << metadata_pool_id << dendl;
    }

    r = rados.ioctx_create(metadata_pool_name.c_str(), metadata_io);
    if (r != 0) {
      return r;
    }
    std::cerr << "Created ioctx for " << metadata_pool_name << std::endl;

    return tmap_upgrade();
  }

  // If caller didn't specify a namespace, try to pick
  // one if only one exists
  if (fscid == FS_CLUSTER_ID_NONE) {
    if (fsmap->filesystem_count() == 1) {
      fscid = fsmap->get_filesystem()->fscid;
    } else {
      std::cerr << "Specify a filesystem with --filesystem" << std::endl;
      return -EINVAL;
    }
  }
  auto fs =  fsmap->get_filesystem(fscid);
  assert(fs != nullptr);

  // Default to output to metadata pool
  if (driver == NULL) {
    driver = new MetadataDriver();
    driver->set_force_corrupt(force_corrupt);
    driver->set_force_init(force_init);
    dout(4) << "Using metadata pool output" << dendl;
  }

  dout(4) << "connecting to RADOS..." << dendl;
  rados.connect();
  r = driver->init(rados, fsmap, fscid);
  if (r < 0) {
    return r;
  }

  // Initialize data_io for those commands that need it
  if (command == "scan_inodes"
     || command == "scan_extents") {
    if (data_pool_name.empty()) {
      std::cerr << "Data pool not specified" << std::endl;
      usage();
      return -EINVAL;
    }

    data_pool_id = rados.pool_lookup(data_pool_name.c_str());
    if (data_pool_id < 0) {
      std::cerr << "Data pool '" << data_pool_name << "' not found!" << std::endl;
      return -ENOENT;
    } else {
      dout(4) << "data pool '" << data_pool_name
        << "' has ID " << data_pool_id << dendl;
    }

    if (!fs->mds_map.is_data_pool(data_pool_id)) {
      std::cerr << "Warning: pool '" << data_pool_name << "' is not a "
        "CephFS data pool!" << std::endl;
      if (!force_pool) {
        std::cerr << "Use --force-pool to continue" << std::endl;
        return -EINVAL;
      }
    }

    dout(4) << "opening data pool '" << data_pool_name << "'" << dendl;
    r = rados.ioctx_create(data_pool_name.c_str(), data_io);
    if (r != 0) {
      return r;
    }
  }

  // Initialize metadata_io from MDSMap for scan_frags
  if (command == "scan_frags") {
    const auto fs = fsmap->get_filesystem(fscid);
    if (fs == nullptr) {
      std::cerr << "Filesystem id " << fscid << " does not exist" << std::endl;
      return -ENOENT;
    }
    int const metadata_pool_id = fs->mds_map.get_metadata_pool();

    dout(4) << "resolving metadata pool " << metadata_pool_id << dendl;
    std::string metadata_pool_name;
    int r = rados.pool_reverse_lookup(metadata_pool_id, &metadata_pool_name);
    if (r < 0) {
      std::cerr << "Pool " << metadata_pool_id
        << " identified in MDS map not found in RADOS!" << std::endl;
      return r;
    }

    r = rados.ioctx_create(metadata_pool_name.c_str(), metadata_io);
    if (r != 0) {
      return r;
    }
  }

  // Finally, dispatch command
  if (command == "scan_inodes") {
    return scan_inodes();
  } else if (command == "scan_extents") {
    return scan_extents();
  } else if (command == "scan_frags") {
    return scan_frags();
  } else if (command == "init") {
    return driver->init_roots(fs->mds_map.get_first_data_pool());
  } else {
    std::cerr << "Unknown command '" << command << "'" << std::endl;
    return -EINVAL;
  }
}

int MetadataDriver::inject_unlinked_inode(
    inodeno_t inono, int mode, int64_t data_pool_id)
{
  const object_t oid = InodeStore::get_object_name(inono, frag_t(), ".inode");

  // Skip if exists
  bool already_exists = false;
  int r = root_exists(inono, &already_exists);
  if (r) {
    return r;
  }
  if (already_exists && !force_init) {
    std::cerr << "Inode 0x" << std::hex << inono << std::dec << " already"
               " exists, skipping create.  Use --force-init to overwrite"
               " the existing object." << std::endl;
    return 0;
  }

  // Compose
  InodeStore inode;
  inode.inode.ino = inono;
  inode.inode.version = 1;
  inode.inode.xattr_version = 1;
  inode.inode.mode = 0500 | mode;
  // Fake dirstat.nfiles to 1, so that the directory doesn't appear to be empty
  // (we won't actually give the *correct* dirstat here though)
  inode.inode.dirstat.nfiles = 1;

  inode.inode.ctime = 
    inode.inode.mtime = ceph_clock_now(g_ceph_context);
  inode.inode.nlink = 1;
  inode.inode.truncate_size = -1ull;
  inode.inode.truncate_seq = 1;
  inode.inode.uid = g_conf->mds_root_ino_uid;
  inode.inode.gid = g_conf->mds_root_ino_gid;

  // Force layout to default: should we let users override this so that
  // they don't have to mount the filesystem to correct it?
  inode.inode.layout = file_layout_t::get_default();
  inode.inode.layout.pool_id = data_pool_id;
  inode.inode.dir_layout.dl_dir_hash = g_conf->mds_default_dir_hash;

  // Assume that we will get our stats wrong, and that we may
  // be ignoring dirfrags that exist
  inode.damage_flags |= (DAMAGE_STATS | DAMAGE_RSTATS | DAMAGE_FRAGTREE);

  // Serialize
  bufferlist inode_bl;
  ::encode(std::string(CEPH_FS_ONDISK_MAGIC), inode_bl);
  inode.encode(inode_bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

  // Write
  r = metadata_io.write_full(oid.name, inode_bl);
  if (r != 0) {
    derr << "Error writing '" << oid.name << "': " << cpp_strerror(r) << dendl;
    return r;
  }

  return r;
}

int MetadataDriver::root_exists(inodeno_t ino, bool *result)
{
  object_t oid = InodeStore::get_object_name(ino, frag_t(), ".inode");
  uint64_t size;
  time_t mtime;
  int r = metadata_io.stat(oid.name, &size, &mtime);
  if (r == -ENOENT) {
    *result = false;
    return 0;
  } else if (r < 0) {
    return r;
  }

  *result = true;
  return 0;
}

int MetadataDriver::init_roots(int64_t data_pool_id)
{
  int r = 0;
  r = inject_unlinked_inode(MDS_INO_ROOT, S_IFDIR|0755, data_pool_id);
  if (r != 0) {
    return r;
  }
  r = inject_unlinked_inode(MDS_INO_MDSDIR(0), S_IFDIR, data_pool_id);
  if (r != 0) {
    return r;
  }
  bool created = false;
  r = find_or_create_dirfrag(MDS_INO_MDSDIR(0), frag_t(), &created);
  if (r != 0) {
    return r;
  }

  return 0;
}

int MetadataDriver::check_roots(bool *result)
{
  int r;
  r = root_exists(MDS_INO_ROOT, result);
  if (r != 0) {
    return r;
  }
  if (!*result) {
    return 0;
  }

  r = root_exists(MDS_INO_MDSDIR(0), result);
  if (r != 0) {
    return r;
  }
  if (!*result) {
    return 0;
  }

  return 0;
}

/**
 * Stages:
 *
 * SERIAL init
 *  0. Create root inodes if don't exist
 * PARALLEL scan_extents
 *  1. Size and mtime recovery: scan ALL objects, and update 0th
 *   objects with max size and max mtime seen.
 * PARALLEL scan_inodes
 *  2. Inode recovery: scan ONLY 0th objects, and inject metadata
 *   into dirfrag OMAPs, creating blank dirfrags as needed.  No stats
 *   or rstats at this stage.  Inodes without backtraces go into
 *   lost+found
 * TODO: SERIAL "recover stats"
 *  3. Dirfrag statistics: depth first traverse into metadata tree,
 *    rebuilding dir sizes.
 * TODO PARALLEL "clean up"
 *  4. Cleanup; go over all 0th objects (and dirfrags if we tagged
 *   anything onto them) and remove any of the xattrs that we
 *   used for accumulating.
 */


int parse_oid(const std::string &oid, uint64_t *inode_no, uint64_t *obj_id)
{
  if (oid.find(".") == std::string::npos || oid.find(".") == oid.size() - 1) {
    return -EINVAL;
  }

  std::string err;
  std::string inode_str = oid.substr(0, oid.find("."));
  *inode_no = strict_strtoll(inode_str.c_str(), 16, &err);
  if (!err.empty()) {
    return -EINVAL;
  }

  std::string pos_string = oid.substr(oid.find(".") + 1);
  *obj_id = strict_strtoll(pos_string.c_str(), 16, &err);
  if (!err.empty()) {
    return -EINVAL;
  }

  return 0;
}


int DataScan::scan_extents()
{
  return forall_objects(data_io, false, [this](
        std::string const &oid,
        uint64_t obj_name_ino,
        uint64_t obj_name_offset) -> int
  {
    // Read size
    uint64_t size;
    time_t mtime;
    int r = data_io.stat(oid, &size, &mtime);
    if (r != 0) {
      dout(4) << "Cannot stat '" << oid << "': skipping" << dendl;
      return r;
    }

    // I need to keep track of
    //  * The highest object ID seen
    //  * The size of the highest object ID seen
    //  * The largest object seen
    //
    //  Given those things, I can later infer the object chunking
    //  size, the offset of the last object (chunk size * highest ID seen)
    //  and the actual size (offset of last object + size of highest ID seen)
    //
    //  This logic doesn't take account of striping.
    r = ClsCephFSClient::accumulate_inode_metadata(
        data_io,
        obj_name_ino,
        obj_name_offset,
        size,
        mtime);
    if (r < 0) {
      derr << "Failed to accumulate metadata data from '"
        << oid << "': " << cpp_strerror(r) << dendl;
      return r;
    }

    return r;
  });
}

int DataScan::probe_filter(librados::IoCtx &ioctx)
{
  bufferlist filter_bl;
  ClsCephFSClient::build_tag_filter("test", &filter_bl);
  librados::ObjectCursor range_i;
  librados::ObjectCursor range_end;

  std::vector<librados::ObjectItem> tmp_result;
  librados::ObjectCursor tmp_next;
  int r = ioctx.object_list(ioctx.object_list_begin(), ioctx.object_list_end(),
                            1, filter_bl, &tmp_result, &tmp_next);

  return r >= 0;
}

int DataScan::forall_objects(
    librados::IoCtx &ioctx,
    bool untagged_only,
    std::function<int(std::string, uint64_t, uint64_t)> handler
    )
{
  librados::ObjectCursor range_i;
  librados::ObjectCursor range_end;
  ioctx.object_list_slice(
      ioctx.object_list_begin(),
      ioctx.object_list_end(),
      n,
      m,
      &range_i,
      &range_end);


  bufferlist filter_bl;

  bool legacy_filtering = false;
  if (untagged_only) {
    // probe to deal with older OSDs that don't support
    // the cephfs pgls filtering mode
    legacy_filtering = !probe_filter(ioctx);
    if (!legacy_filtering) {
      ClsCephFSClient::build_tag_filter(filter_tag, &filter_bl);
    }
  }

  int r = 0;
  while(range_i < range_end) {
    std::vector<librados::ObjectItem> result;
    int r = ioctx.object_list(range_i, range_end, 1,
                                filter_bl, &result, &range_i);
    if (r < 0) {
      derr << "Unexpected error listing objects: " << cpp_strerror(r) << dendl;
      return r;
    }

    for (const auto &i : result) {
      const std::string &oid = i.oid;
      uint64_t obj_name_ino = 0;
      uint64_t obj_name_offset = 0;
      r = parse_oid(oid, &obj_name_ino, &obj_name_offset);
      if (r != 0) {
        dout(4) << "Bad object name '" << oid << "', skipping" << dendl;
        continue;
      }

      if (untagged_only && legacy_filtering) {
        dout(20) << "Applying filter to " << oid << dendl;

        // We are only interested in 0th objects during this phase: we touched
        // the other objects during scan_extents
        if (obj_name_offset != 0) {
          dout(20) << "Non-zeroth object" << dendl;
          continue;
        }

        bufferlist scrub_tag_bl;
        int r = ioctx.getxattr(oid, "scrub_tag", scrub_tag_bl);
        if (r >= 0) {
          std::string read_tag;
          bufferlist::iterator q = scrub_tag_bl.begin();
          try {
            ::decode(read_tag, q);
            if (read_tag == filter_tag) {
              dout(20) << "skipping " << oid << " because it has the filter_tag"
                       << dendl;
              continue;
            }
          } catch (const buffer::error &err) {
          }
          dout(20) << "read non-matching tag '" << read_tag << "'" << dendl;
        } else {
          dout(20) << "no tag read (" << r << ")" << dendl;
        }

      } else if (untagged_only) {
        assert(obj_name_offset == 0);
        dout(20) << "OSD matched oid " << oid << dendl;
      }

      int this_oid_r = handler(oid, obj_name_ino, obj_name_offset);
      if (r == 0 && this_oid_r < 0) {
        r = this_oid_r;
      }
    }
  }

  return r;
}

int DataScan::scan_inodes()
{
  bool roots_present;
  int r = driver->check_roots(&roots_present);
  if (r != 0) {
    derr << "Unexpected error checking roots: '"
      << cpp_strerror(r) << "'" << dendl;
    return r;
  }

  if (!roots_present) {
    std::cerr << "Some or all system inodes are absent.  Run 'init' from "
      "one node before running 'scan_inodes'" << std::endl;
    return -EIO;
  }

  return forall_objects(data_io, true, [this](
        std::string const &oid,
        uint64_t obj_name_ino,
        uint64_t obj_name_offset) -> int
  {
    int r = 0;

    AccumulateResult accum_res;
    inode_backtrace_t backtrace;
    file_layout_t loaded_layout = file_layout_t::get_default();
    r = ClsCephFSClient::fetch_inode_accumulate_result(
        data_io, oid, &backtrace, &loaded_layout, &accum_res);

    if (r == -EINVAL) {
      dout(4) << "Accumulated metadata missing from '"
              << oid << ", did you run scan_extents?" << dendl;
      return r;
    } else if (r < 0) {
      dout(4) << "Unexpected error loading accumulated metadata from '"
              << oid << "': " << cpp_strerror(r) << dendl;
      // FIXME: this creates situation where if a client has a corrupt
      // backtrace/layout, we will fail to inject it.  We should (optionally)
      // proceed if the backtrace/layout is corrupt but we have valid
      // accumulated metadata.
      return r;
    }

    const time_t file_mtime = accum_res.max_mtime;
    uint64_t file_size = 0;
    bool have_backtrace = !(backtrace.ancestors.empty());

    // This is the layout we will use for injection, populated either
    // from loaded_layout or from best guesses
    file_layout_t guessed_layout;
    guessed_layout.pool_id = data_pool_id;

    // Calculate file_size, guess the layout
    if (accum_res.ceiling_obj_index > 0) {
      uint32_t chunk_size = file_layout_t::get_default().object_size;
      // When there are multiple objects, the largest object probably
      // indicates the chunk size.  But not necessarily, because files
      // can be sparse.  Only make this assumption if size seen
      // is a power of two, as chunk sizes typically are.
      if ((accum_res.max_obj_size & (accum_res.max_obj_size - 1)) == 0) {
        chunk_size = accum_res.max_obj_size;
      }

      if (loaded_layout.pool_id == -1) {
        // If no stashed layout was found, guess it
        guessed_layout.object_size = chunk_size;
        guessed_layout.stripe_unit = chunk_size;
        guessed_layout.stripe_count = 1;
      } else if (!loaded_layout.is_valid() ||
          loaded_layout.object_size < accum_res.max_obj_size) {
        // If the max size seen exceeds what the stashed layout claims, then
        // disbelieve it.  Guess instead.  Same for invalid layouts on disk.
        dout(4) << "bogus xattr layout on 0x" << std::hex << obj_name_ino
                << std::dec << ", ignoring in favour of best guess" << dendl;
        guessed_layout.object_size = chunk_size;
        guessed_layout.stripe_unit = chunk_size;
        guessed_layout.stripe_count = 1;
      } else {
        // We have a stashed layout that we can't disprove, so apply it
        guessed_layout = loaded_layout;
        dout(20) << "loaded layout from xattr:"
          << " os: " << guessed_layout.object_size
          << " sc: " << guessed_layout.stripe_count
          << " su: " << guessed_layout.stripe_unit
          << dendl;
        // User might have transplanted files from a pool with a different
        // ID, so whatever the loaded_layout says, we'll force the injected
        // layout to point to the pool we really read from
        guessed_layout.pool_id = data_pool_id;
      }

      if (guessed_layout.stripe_count == 1) {
        // Unstriped file: simple chunking
        file_size = guessed_layout.object_size * accum_res.ceiling_obj_index
                    + accum_res.ceiling_obj_size;
      } else {
        // Striped file: need to examine the last stripe_count objects
        // in the file to determine the size.

        // How many complete (i.e. not last stripe) objects?
        uint64_t complete_objs = 0;
        if (accum_res.ceiling_obj_index > guessed_layout.stripe_count - 1) {
          complete_objs = (accum_res.ceiling_obj_index / guessed_layout.stripe_count) * guessed_layout.stripe_count;
        } else {
          complete_objs = 0;
        }

        // How many potentially-short objects (i.e. last stripe set) objects?
        uint64_t partial_objs = accum_res.ceiling_obj_index + 1 - complete_objs;

        dout(10) << "calculating striped size from complete objs: "
                 << complete_objs << ", partial objs: " << partial_objs
                 << dendl;

        // Maximum amount of data that may be in the incomplete objects
        uint64_t incomplete_size = 0;

        // For each short object, calculate the max file size within it
        // and accumulate the maximum
        for (uint64_t i = complete_objs; i < complete_objs + partial_objs; ++i) {
          char buf[60];
          snprintf(buf, sizeof(buf), "%llx.%08llx",
              (long long unsigned)obj_name_ino, (long long unsigned)i);

          uint64_t osize(0);
          time_t omtime(0);
          r = data_io.stat(std::string(buf), &osize, &omtime);
          if (r == 0) {
            if (osize > 0) {
              // Upper bound within this object
              uint64_t upper_size = (osize - 1) / guessed_layout.stripe_unit
                * (guessed_layout.stripe_unit * guessed_layout.stripe_count)
                + (i % guessed_layout.stripe_count)
                * guessed_layout.stripe_unit + (osize - 1)
                % guessed_layout.stripe_unit + 1;
              incomplete_size = MAX(incomplete_size, upper_size);
            }
          } else if (r == -ENOENT) {
            // Absent object, treat as size 0 and ignore.
          } else {
            // Unexpected error, carry r to outer scope for handling.
            break;
          }
        }
        if (r != 0 && r != -ENOENT) {
          derr << "Unexpected error checking size of ino 0x" << std::hex
               << obj_name_ino << std::dec << ": " << cpp_strerror(r) << dendl;
          return r;
        }
        file_size = complete_objs * guessed_layout.object_size
                    + incomplete_size;
      }
    } else {
      file_size = accum_res.ceiling_obj_size;
      if (loaded_layout.pool_id < 0
          || loaded_layout.object_size < accum_res.max_obj_size) {
        // No layout loaded, or inconsistent layout, use default
        guessed_layout = file_layout_t::get_default();
        guessed_layout.pool_id = data_pool_id;
      } else {
        guessed_layout = loaded_layout;
      }
    }

    // Santity checking backtrace ino against object name
    if (have_backtrace && backtrace.ino != obj_name_ino) {
      dout(4) << "Backtrace ino 0x" << std::hex << backtrace.ino
        << " doesn't match object name ino 0x" << obj_name_ino
        << std::dec << dendl;
      have_backtrace = false;
    }

    InodeStore dentry;
    build_file_dentry(obj_name_ino, file_size, file_mtime, guessed_layout, &dentry);

    // Inject inode to the metadata pool
    if (have_backtrace) {
      inode_backpointer_t root_bp = *(backtrace.ancestors.rbegin());
      if (MDS_INO_IS_MDSDIR(root_bp.dirino)) {
        /* Special case for strays: even if we have a good backtrace,
         * don't put it in the stray dir, because while that would technically
         * give it linkage it would still be invisible to the user */
        r = driver->inject_lost_and_found(obj_name_ino, dentry);
        if (r < 0) {
          dout(4) << "Error injecting 0x" << std::hex << backtrace.ino
            << std::dec << " into lost+found: " << cpp_strerror(r) << dendl;
          if (r == -EINVAL) {
            dout(4) << "Use --force-corrupt to overwrite structures that "
                       "appear to be corrupt" << dendl;
          }
        }
      } else {
        /* Happy case: we will inject a named dentry for this inode */
        r = driver->inject_with_backtrace(backtrace, dentry);
        if (r < 0) {
          dout(4) << "Error injecting 0x" << std::hex << backtrace.ino
            << std::dec << " with backtrace: " << cpp_strerror(r) << dendl;
          if (r == -EINVAL) {
            dout(4) << "Use --force-corrupt to overwrite structures that "
                       "appear to be corrupt" << dendl;
          }
        }
      }
    } else {
      /* Backtrace-less case: we will inject a lost+found dentry */
      r = driver->inject_lost_and_found(
          obj_name_ino, dentry);
      if (r < 0) {
        dout(4) << "Error injecting 0x" << std::hex << obj_name_ino
          << std::dec << " into lost+found: " << cpp_strerror(r) << dendl;
        if (r == -EINVAL) {
          dout(4) << "Use --force-corrupt to overwrite structures that "
                     "appear to be corrupt" << dendl;
        }
      }
    }

    return r;
  });
}

bool DataScan::valid_ino(inodeno_t ino) const
{
  return (ino >= inodeno_t((1ull << 40)))
    || (MDS_INO_IS_STRAY(ino))
    || (MDS_INO_IS_MDSDIR(ino))
    || ino == MDS_INO_ROOT
    || ino == MDS_INO_CEPH;
}

int DataScan::tmap_upgrade()
{
  librados::NObjectIterator i = metadata_io.nobjects_begin();
  const librados::NObjectIterator i_end = metadata_io.nobjects_end();

  int overall_r = 0;

  for (; i != i_end; ++i) {
    const std::string oid = i->get_oid();

    uint64_t inode_no = 0;
    uint64_t frag_id = 0;
    int r = parse_oid(oid, &inode_no, &frag_id);
    if (r == -EINVAL) {
      dout(10) << "Not a dirfrag: '" << oid << "'" << dendl;
      continue;
    } else {
      // parse_oid can only do 0 or -EINVAL
      assert(r == 0);
    }

    if (!valid_ino(inode_no)) {
      dout(10) << "Not a difrag (invalid ino): '" << oid << "'" << dendl;
      continue;
    }

    r = metadata_io.tmap_to_omap(oid, true);
    dout(20) << "tmap2omap(" << oid << "): " << r << dendl;
    if (r < 0) {
      derr << "Error converting '" << oid << "': " << cpp_strerror(r) << dendl;
      overall_r = r;
    }
  }

  return overall_r;
}

int DataScan::scan_frags()
{
  bool roots_present;
  int r = driver->check_roots(&roots_present);
  if (r != 0) {
    derr << "Unexpected error checking roots: '"
      << cpp_strerror(r) << "'" << dendl;
    return r;
  }

  if (!roots_present) {
    std::cerr << "Some or all system inodes are absent.  Run 'init' from "
      "one node before running 'scan_inodes'" << std::endl;
    return -EIO;
  }

  return forall_objects(metadata_io, true, [this](
        std::string const &oid,
        uint64_t obj_name_ino,
        uint64_t obj_name_offset) -> int
  {
    int r = 0;
    r = parse_oid(oid, &obj_name_ino, &obj_name_offset);
    if (r != 0) {
      dout(4) << "Bad object name '" << oid << "', skipping" << dendl;
      return r;
    }

    if (obj_name_ino < (1ULL << 40)) {
      // FIXME: we're skipping stray dirs here: if they're
      // orphaned then we should be resetting them some other
      // way
      dout(10) << "Skipping system ino " << obj_name_ino << dendl;
      return 0;
    }

    AccumulateResult accum_res;
    inode_backtrace_t backtrace;

    // Default to inherit layout (i.e. no explicit layout on dir) which is
    // expressed as a zeroed layout struct (see inode_t::has_layout)
    file_layout_t loaded_layout;

    int parent_r = 0;
    bufferlist parent_bl;
    int layout_r = 0;
    bufferlist layout_bl;
    bufferlist op_bl;

    librados::ObjectReadOperation op;
    op.getxattr("parent", &parent_bl, &parent_r);
    op.getxattr("layout", &layout_bl, &layout_r);
    r = metadata_io.operate(oid, &op, &op_bl);
    if (r != 0 && r != -ENODATA) {
      derr << "Unexpected error reading backtrace: " << cpp_strerror(parent_r) << dendl;
      return r;
    }

    if (parent_r != -ENODATA) {
      try {
        bufferlist::iterator q = parent_bl.begin();
        backtrace.decode(q);
      } catch (buffer::error &e) {
        dout(4) << "Corrupt backtrace on '" << oid << "': " << e << dendl;
        if (!force_corrupt) {
          return -EINVAL;
        } else {
          // Treat backtrace as absent: we'll inject into lost+found
          backtrace = inode_backtrace_t();
        }
      }
    }

    if (layout_r != -ENODATA) {
      try {
        bufferlist::iterator q = layout_bl.begin();
        ::decode(loaded_layout, q);
      } catch (buffer::error &e) {
        dout(4) << "Corrupt layout on '" << oid << "': " << e << dendl;
        if (!force_corrupt) {
          return -EINVAL;
        }
      }
    }

    bool have_backtrace = !(backtrace.ancestors.empty());

    // Santity checking backtrace ino against object name
    if (have_backtrace && backtrace.ino != obj_name_ino) {
      dout(4) << "Backtrace ino 0x" << std::hex << backtrace.ino
        << " doesn't match object name ino 0x" << obj_name_ino
        << std::dec << dendl;
      have_backtrace = false;
    }

    uint64_t fnode_version = 0;
    fnode_t fnode;
    r = read_fnode(obj_name_ino, frag_t(), &fnode, &fnode_version);
    if (r == -EINVAL) {
      derr << "Corrupt fnode on " << oid << dendl;
      if (force_corrupt) {
	fnode.fragstat.mtime = 0;
	fnode.fragstat.nfiles = 1;
	fnode.fragstat.nsubdirs = 0;
	fnode.accounted_fragstat = fnode.fragstat;
      } else {
        return r;
      }
    }

    InodeStore dentry;
    build_dir_dentry(obj_name_ino, fnode.accounted_fragstat,
		loaded_layout, &dentry);

    // Inject inode to the metadata pool
    if (have_backtrace) {
      inode_backpointer_t root_bp = *(backtrace.ancestors.rbegin());
      if (MDS_INO_IS_MDSDIR(root_bp.dirino)) {
        /* Special case for strays: even if we have a good backtrace,
         * don't put it in the stray dir, because while that would technically
         * give it linkage it would still be invisible to the user */
        r = driver->inject_lost_and_found(obj_name_ino, dentry);
        if (r < 0) {
          dout(4) << "Error injecting 0x" << std::hex << backtrace.ino
            << std::dec << " into lost+found: " << cpp_strerror(r) << dendl;
          if (r == -EINVAL) {
            dout(4) << "Use --force-corrupt to overwrite structures that "
                       "appear to be corrupt" << dendl;
          }
        }
      } else {
        /* Happy case: we will inject a named dentry for this inode */
        r = driver->inject_with_backtrace(backtrace, dentry);
        if (r < 0) {
          dout(4) << "Error injecting 0x" << std::hex << backtrace.ino
            << std::dec << " with backtrace: " << cpp_strerror(r) << dendl;
          if (r == -EINVAL) {
            dout(4) << "Use --force-corrupt to overwrite structures that "
                       "appear to be corrupt" << dendl;
          }
        }
      }
    } else {
      /* Backtrace-less case: we will inject a lost+found dentry */
      r = driver->inject_lost_and_found(
          obj_name_ino, dentry);
      if (r < 0) {
        dout(4) << "Error injecting 0x" << std::hex << obj_name_ino
          << std::dec << " into lost+found: " << cpp_strerror(r) << dendl;
        if (r == -EINVAL) {
          dout(4) << "Use --force-corrupt to overwrite structures that "
                     "appear to be corrupt" << dendl;
        }
      }
    }

    return r;
  });
}

int MetadataTool::read_fnode(
    inodeno_t ino, frag_t frag, fnode_t *fnode,
    uint64_t *last_version)
{
  assert(fnode != NULL);

  object_t frag_oid = InodeStore::get_object_name(ino, frag, "");
  bufferlist fnode_bl;
  int r = metadata_io.omap_get_header(frag_oid.name, &fnode_bl);
  *last_version = metadata_io.get_last_version();
  if (r < 0) {
    return r;
  }

  bufferlist::iterator old_fnode_iter = fnode_bl.begin();
  try {
    (*fnode).decode(old_fnode_iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return 0;
}

int MetadataTool::read_dentry(inodeno_t parent_ino, frag_t frag,
                const std::string &dname, InodeStore *inode)
{
  assert(inode != NULL);


  std::string key;
  dentry_key_t dn_key(CEPH_NOSNAP, dname.c_str());
  dn_key.encode(key);

  std::set<std::string> keys;
  keys.insert(key);
  std::map<std::string, bufferlist> vals;
  object_t frag_oid = InodeStore::get_object_name(parent_ino, frag, "");
  int r = metadata_io.omap_get_vals_by_keys(frag_oid.name, keys, &vals);  
  dout(20) << "oid=" << frag_oid.name
           << " dname=" << dname
           << " frag=" << frag
           << ", r=" << r << dendl;
  if (r < 0) {
    return r;
  }

  if (vals.find(key) == vals.end()) {
    dout(20) << key << " not found in result" << dendl;
    return -ENOENT;
  }

  try {
    bufferlist::iterator q = vals[key].begin();
    snapid_t dnfirst;
    ::decode(dnfirst, q);
    char dentry_type;
    ::decode(dentry_type, q);
    if (dentry_type == 'I') {
      inode->decode_bare(q);
      return 0;
    } else {
      dout(20) << "dentry type '" << dentry_type << "': cannot"
                  "read an inode out of that" << dendl;
      return -EINVAL;
    }
  } catch (const buffer::error &err) {
    dout(20) << "encoding error in dentry 0x" << std::hex << parent_ino
             << std::dec << "/" << dname << dendl;
    return -EINVAL;
  }

  return 0;
}

int MetadataDriver::inject_lost_and_found(
    inodeno_t ino, const InodeStore &dentry)
{
  // Create lost+found if doesn't exist
  bool created = false;
  int r = find_or_create_dirfrag(CEPH_INO_ROOT, frag_t(), &created);
  if (r < 0) {
    return r;
  }
  InodeStore lf_ino;
  r = read_dentry(CEPH_INO_ROOT, frag_t(), "lost+found", &lf_ino);
  if (r == -ENOENT || r == -EINVAL) {
    if (r == -EINVAL && !force_corrupt) {
      return r;
    }

    // To have a directory not specify a layout, give it zeros (see
    // inode_t::has_layout)
    file_layout_t inherit_layout;

    // Construct LF inode
    frag_info_t fragstat;
    fragstat.nfiles = 1,
    build_dir_dentry(CEPH_INO_LOST_AND_FOUND, fragstat, inherit_layout, &lf_ino);

    // Inject link to LF inode in the root dir
    r = inject_linkage(CEPH_INO_ROOT, "lost+found", frag_t(), lf_ino);
    if (r < 0) {
      return r;
    }
  } else {
    if (!(lf_ino.inode.mode & S_IFDIR)) {
      derr << "lost+found exists but is not a directory!" << dendl;
      // In this case we error out, and the user should do something about
      // this problem.
      return -EINVAL;
    }
  }

  r = find_or_create_dirfrag(CEPH_INO_LOST_AND_FOUND, frag_t(), &created);
  if (r < 0) {
    return r;
  }

  InodeStore recovered_ino;


  const std::string dname = lost_found_dname(ino);

  // Write dentry into lost+found dirfrag
  return inject_linkage(lf_ino.inode.ino, dname, frag_t(), dentry);
}


int MetadataDriver::get_frag_of(
    inodeno_t dirino,
    const std::string &target_dname,
    frag_t *result_ft)
{
  object_t root_frag_oid = InodeStore::get_object_name(dirino, frag_t(), "");

  dout(20) << "dirino=" << dirino << " target_dname=" << target_dname << dendl;

  // Find and load fragtree if existing dirfrag
  // ==========================================
  bool have_backtrace = false; 
  bufferlist parent_bl;
  int r = metadata_io.getxattr(root_frag_oid.name, "parent", parent_bl);
  if (r == -ENODATA) {
    dout(10) << "No backtrace on '" << root_frag_oid << "'" << dendl;
  } else if (r < 0) {
    dout(4) << "Unexpected error on '" << root_frag_oid << "': "
      << cpp_strerror(r) << dendl;
    return r;
  }

  // Deserialize backtrace
  inode_backtrace_t backtrace;
  if (parent_bl.length()) {
    try {
      bufferlist::iterator q = parent_bl.begin();
      backtrace.decode(q);
      have_backtrace = true;
    } catch (buffer::error &e) {
      dout(4) << "Corrupt backtrace on '" << root_frag_oid << "': " << e << dendl;
    }
  }

  if (!(have_backtrace && backtrace.ancestors.size())) {
    // Can't work out fragtree without a backtrace
    dout(4) << "No backtrace on '" << root_frag_oid
            << "': cannot determine fragtree" << dendl;
    return -ENOENT;
  }

  // The parentage of dirino
  const inode_backpointer_t &bp = *(backtrace.ancestors.begin());

  // The inode of dirino's parent
  const inodeno_t parent_ino = bp.dirino;

  // The dname of dirino in its parent.
  const std::string &parent_dname = bp.dname;

  dout(20) << "got backtrace parent " << parent_ino << "/"
           << parent_dname << dendl;

  // The primary dentry for dirino
  InodeStore existing_dentry;

  // See if we can find ourselves in dirfrag zero of the parent: this
  // is a fast path that avoids needing to go further up the tree
  // if the parent isn't fragmented (worst case we would have to
  // go all the way to the root)
  r = read_dentry(parent_ino, frag_t(), parent_dname, &existing_dentry);
  if (r >= 0) {
    // Great, fast path: return the fragtree from here
    if (existing_dentry.inode.ino != dirino) {
      dout(4) << "Unexpected inode in dentry! 0x" << std::hex
              << existing_dentry.inode.ino
              << " vs expected 0x" << dirino << std::dec << dendl;
      return -ENOENT;
    }
    dout(20) << "fast path, fragtree is "
             << existing_dentry.dirfragtree << dendl;
    *result_ft = existing_dentry.pick_dirfrag(target_dname);
    dout(20) << "frag is " << *result_ft << dendl;
    return 0;
  } else if (r != -ENOENT) {
    // Dentry not present in 0th frag, must read parent's fragtree
    frag_t parent_frag;
    r = get_frag_of(parent_ino, parent_dname, &parent_frag);
    if (r == 0) {
      // We have the parent fragtree, so try again to load our dentry
      r = read_dentry(parent_ino, parent_frag, parent_dname, &existing_dentry);
      if (r >= 0) {
        // Got it!
        *result_ft = existing_dentry.pick_dirfrag(target_dname);
        dout(20) << "resolved via parent, frag is " << *result_ft << dendl;
        return 0;
      } else {
        if (r == -EINVAL || r == -ENOENT) {
          return -ENOENT;  // dentry missing or corrupt, so frag is missing
        } else {
          return r;
        }
      }
    } else {
      // Couldn't resolve parent fragtree, so can't find ours.
      return r;
    }
  } else if (r == -EINVAL) {
    // Unreadable dentry, can't know the fragtree.
    return -ENOENT;
  } else {
    // Unexpected error, raise it
    return r;
  }
}


int MetadataDriver::inject_with_backtrace(
    const inode_backtrace_t &backtrace, const InodeStore &dentry)
    
{

  // On dirfrags
  // ===========
  // In order to insert something into a directory, we first (ideally)
  // need to know the fragtree for the directory.  Sometimes we can't
  // get that, in which case we just go ahead and insert it into
  // fragment zero for a good chance of that being the right thing
  // anyway (most moderate-sized dirs aren't fragmented!)

  // On ancestry
  // ===========
  // My immediate ancestry should be correct, so if we can find that
  // directory's dirfrag then go inject it there.  This works well
  // in the case that this inode's dentry was somehow lost and we
  // are recreating it, because the rest of the hierarchy
  // will probably still exist.
  //
  // It's more of a "better than nothing" approach when rebuilding
  // a whole tree, as backtraces will in general not be up to date
  // beyond the first parent, if anything in the trace was ever
  // moved after the file was created.

  // On inode numbers
  // ================
  // The backtrace tells us inodes for each of the parents.  If we are
  // creating those parent dirfrags, then there is a risk that somehow
  // the inode indicated here was also used for data (not a dirfrag) at
  // some stage.  That would be a zany situation, and we don't check
  // for it here, because to do so would require extra IOs for everything
  // we inject, and anyway wouldn't guarantee that the inode number
  // wasn't in use in some dentry elsewhere in the metadata tree that
  // just happened not to have any data objects.

  // On multiple workers touching the same traces
  // ============================================
  // When creating linkage for a directory, *only* create it if we are
  // also creating the object.  That way, we might not manage to get the
  // *right* linkage for a directory, but at least we won't multiply link
  // it.  We assume that if a root dirfrag exists for a directory, then
  // it is linked somewhere (i.e. that the metadata pool is not already
  // inconsistent).
  //
  // Making sure *that* is true is someone else's job!  Probably someone
  // who is not going to run in parallel, so that they can self-consistently
  // look at versions and move things around as they go.
  // Note this isn't 100% safe: if we die immediately after creating dirfrag
  // object, next run will fail to create linkage for the dirfrag object
  // and leave it orphaned.

  inodeno_t ino = backtrace.ino;
  dout(10) << "  inode: 0x" << std::hex << ino << std::dec << dendl;
  for (std::vector<inode_backpointer_t>::const_iterator i = backtrace.ancestors.begin();
      i != backtrace.ancestors.end(); ++i) {
    const inode_backpointer_t &backptr = *i;
    dout(10) << "  backptr: 0x" << std::hex << backptr.dirino << std::dec
      << "/" << backptr.dname << dendl;

    // Examine root dirfrag for parent
    const inodeno_t parent_ino = backptr.dirino;
    const std::string dname = backptr.dname;

    frag_t fragment;
    int r = get_frag_of(parent_ino, dname, &fragment);
    if (r == -ENOENT) {
      // Don't know fragment, fall back to assuming root
      dout(20) << "don't know fragment for 0x" << std::hex <<
        parent_ino << std::dec << "/" << dname << ", will insert to root"
        << dendl;
    }

    // Find or create dirfrag
    // ======================
    bool created_dirfrag;
    r = find_or_create_dirfrag(parent_ino, fragment, &created_dirfrag);
    if (r < 0) {
      return r;
    }

    // Check if dentry already exists
    // ==============================
    InodeStore existing_dentry;
    r = read_dentry(parent_ino, fragment, dname, &existing_dentry);
    bool write_dentry = false;
    if (r == -ENOENT || r == -EINVAL) {
      if (r == -EINVAL && !force_corrupt) {
        return r;
      }
      // Missing or corrupt dentry
      write_dentry = true;
    } else if (r < 0) {
      derr << "Unexpected error reading dentry 0x" << std::hex
        << parent_ino << std::dec << "/"
        << dname << ": " << cpp_strerror(r) << dendl;
      break;
    } else {
      // Dentry already present, does it link to me?
      if (existing_dentry.inode.ino == ino) {
        dout(20) << "Dentry 0x" << std::hex
          << parent_ino << std::dec << "/"
          << dname << " already exists and points to me" << dendl;
      } else {
        derr << "Dentry 0x" << std::hex
          << parent_ino << std::dec << "/"
          << dname << " already exists but points to 0x"
          << std::hex << existing_dentry.inode.ino << std::dec << dendl;
        // Fall back to lost+found!
        return inject_lost_and_found(backtrace.ino, dentry);
      }
    }

    // Inject linkage
    // ==============

    if (write_dentry) {
      if (i == backtrace.ancestors.begin()) {
        // This is the linkage for the file of interest
        dout(10) << "Linking inode 0x" << std::hex << ino
          << " at 0x" << parent_ino << "/" << dname << std::dec
          << " with size=" << dentry.inode.size << " bytes" << dendl;

        r = inject_linkage(parent_ino, dname, fragment, dentry);
      } else {
        // This is the linkage for an ancestor directory
        InodeStore ancestor_dentry;
        ancestor_dentry.inode.mode = 0755 | S_IFDIR;

        // Set nfiles to something non-zero, to fool any other code
        // that tries to ignore 'empty' directories.  This won't be
        // accurate, but it should avoid functional issues.

        ancestor_dentry.inode.dirstat.nfiles = 1;
        ancestor_dentry.inode.dir_layout.dl_dir_hash =
                                               g_conf->mds_default_dir_hash;

        ancestor_dentry.inode.nlink = 1;
        ancestor_dentry.inode.ino = ino;
        ancestor_dentry.inode.uid = g_conf->mds_root_ino_uid;
        ancestor_dentry.inode.gid = g_conf->mds_root_ino_gid;
        ancestor_dentry.inode.version = 1;
        ancestor_dentry.inode.backtrace_version = 1;
        r = inject_linkage(parent_ino, dname, fragment, ancestor_dentry);
      }

      if (r < 0) {
        return r;
      }
    }

    if (!created_dirfrag) {
      // If the parent dirfrag already existed, then stop traversing the
      // backtrace: assume that the other ancestors already exist too.  This
      // is an assumption rather than a truth, but it's a convenient way
      // to avoid the risk of creating multiply-linked directories while
      // injecting data.  If there are in fact missing ancestors, this
      // should be fixed up using a separate tool scanning the metadata
      // pool.
      break;
    } else {
      // Proceed up the backtrace, creating parents
      ino = parent_ino;
    }
  }

  return 0;
}

int MetadataDriver::find_or_create_dirfrag(
    inodeno_t ino,
    frag_t fragment,
    bool *created)
{
  assert(created != NULL);

  fnode_t existing_fnode;
  *created = false;

  uint64_t read_version = 0;
  int r = read_fnode(ino, fragment, &existing_fnode, &read_version);
  dout(10) << "read_version = " << read_version << dendl;

  if (r == -ENOENT || r == -EINVAL) {
    if (r == -EINVAL && !force_corrupt) {
      return r;
    }

    // Missing or corrupt fnode, create afresh
    bufferlist fnode_bl;
    fnode_t blank_fnode;
    blank_fnode.version = 1;
    // mark it as non-empty
    blank_fnode.fragstat.nfiles = 1;
    blank_fnode.accounted_fragstat = blank_fnode.fragstat;
    blank_fnode.damage_flags |= (DAMAGE_STATS | DAMAGE_RSTATS);
    blank_fnode.encode(fnode_bl);


    librados::ObjectWriteOperation op;

    if (read_version) {
      assert(r == -EINVAL);
      // Case A: We must assert that the version isn't changed since we saw the object
      // was unreadable, to avoid the possibility of two data-scan processes
      // both creating the frag.
      op.assert_version(read_version);
    } else {
      assert(r == -ENOENT);
      // Case B: The object didn't exist in read_fnode, so while creating it we must
      // use an exclusive create to correctly populate *creating with
      // whether we created it ourselves or someone beat us to it.
      op.create(true);
    }

    object_t frag_oid = InodeStore::get_object_name(ino, fragment, "");
    op.omap_set_header(fnode_bl);
    r = metadata_io.operate(frag_oid.name, &op);
    if (r == -EOVERFLOW || r == -EEXIST) {
      // Someone else wrote it (see case A above)
      dout(10) << "Dirfrag creation race: 0x" << std::hex
        << ino << " " << fragment << std::dec << dendl;
      *created = false;
      return 0;
    } else if (r < 0) {
      // We were unable to create or write it, error out
      derr << "Failed to create dirfrag 0x" << std::hex
        << ino << std::dec << ": " << cpp_strerror(r) << dendl;
      return r;
    } else {
      // Success: the dirfrag object now exists with a value header
      dout(10) << "Created dirfrag: 0x" << std::hex
        << ino << std::dec << dendl;
      *created = true;
    }
  } else if (r < 0) {
    derr << "Unexpected error reading dirfrag 0x" << std::hex
      << ino << std::dec << " : " << cpp_strerror(r) << dendl;
    return r;
  } else {
    dout(20) << "Dirfrag already exists: 0x" << std::hex
      << ino << " " << fragment << std::dec << dendl;
  }

  return 0;
}

int MetadataDriver::inject_linkage(
    inodeno_t dir_ino, const std::string &dname,
    const frag_t fragment, const InodeStore &inode)
{
  // We have no information about snapshots, so everything goes
  // in as CEPH_NOSNAP
  snapid_t snap = CEPH_NOSNAP;

  object_t frag_oid = InodeStore::get_object_name(dir_ino, fragment, "");

  std::string key;
  dentry_key_t dn_key(snap, dname.c_str());
  dn_key.encode(key);

  bufferlist dentry_bl;
  ::encode(snap, dentry_bl);
  ::encode('I', dentry_bl);
  inode.encode_bare(dentry_bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

  // Write out
  std::map<std::string, bufferlist> vals;
  vals[key] = dentry_bl;
  int r = metadata_io.omap_set(frag_oid.name, vals);
  if (r != 0) {
    derr << "Error writing dentry 0x" << std::hex
      << dir_ino << std::dec << "/"
      << dname << ": " << cpp_strerror(r) << dendl;
    return r;
  } else {
    dout(20) << "Injected dentry 0x" << std::hex
      << dir_ino << "/" << dname << " pointing to 0x"
      << inode.inode.ino << std::dec << dendl;
    return 0;
  }
}


int MetadataDriver::init(
    librados::Rados &rados, const FSMap *fsmap, fs_cluster_id_t fscid)
{
  auto fs =  fsmap->get_filesystem(fscid);
  assert(fs != nullptr);
  int const metadata_pool_id = fs->mds_map.get_metadata_pool();

  dout(4) << "resolving metadata pool " << metadata_pool_id << dendl;
  std::string metadata_pool_name;
  int r = rados.pool_reverse_lookup(metadata_pool_id, &metadata_pool_name);
  if (r < 0) {
    derr << "Pool " << metadata_pool_id
      << " identified in MDS map not found in RADOS!" << dendl;
    return r;
  }
  dout(4) << "found metadata pool '" << metadata_pool_name << "'" << dendl;
  return rados.ioctx_create(metadata_pool_name.c_str(), metadata_io);
}

int LocalFileDriver::init(
    librados::Rados &rados, const FSMap *fsmap, fs_cluster_id_t fscid)
{
  return 0;
}

int LocalFileDriver::inject_data(
    const std::string &file_path,
    uint64_t size,
    uint32_t chunk_size,
    inodeno_t ino)
{
  // Scrape the file contents out of the data pool and into the
  // local filesystem
  std::fstream f;
  f.open(file_path.c_str(), std::fstream::out | std::fstream::binary);

  for (uint64_t offset = 0; offset < size; offset += chunk_size) {
    bufferlist bl;

    char buf[32];
    snprintf(buf, sizeof(buf),
        "%llx.%08llx",
        (unsigned long long)ino,
        (unsigned long long)(offset / chunk_size));
    std::string oid(buf);

    int r = data_io.read(oid, bl, chunk_size, 0);

    if (r <= 0 && r != -ENOENT) {
      derr << "error reading data object '" << oid << "': "
        << cpp_strerror(r) << dendl;
      f.close();
      return r;
    } else if (r >=0) {
      
      f.seekp(offset);
      bl.write_stream(f);
    }
  }
  f.close();

  return 0;
}


int LocalFileDriver::inject_with_backtrace(
    const inode_backtrace_t &bt,
    const InodeStore &dentry)
{
  std::string path_builder = path;

  // Iterate through backtrace creating directory parents
  std::vector<inode_backpointer_t>::const_reverse_iterator i;
  for (i = bt.ancestors.rbegin();
      i != bt.ancestors.rend(); ++i) {

    const inode_backpointer_t &backptr = *i;
    path_builder += "/";
    path_builder += backptr.dname;

    // Last entry is the filename itself
    bool is_file = (i + 1 == bt.ancestors.rend());
    if (is_file) {
      // FIXME: inject_data won't cope with interesting (i.e. striped)
      // layouts (need a librados-compatible Filer to read these)
      inject_data(path_builder, dentry.inode.size,
		  dentry.inode.layout.object_size, bt.ino);
    } else {
      int r = mkdir(path_builder.c_str(), 0755);
      if (r != 0 && r != -EPERM) {
        derr << "error creating directory: '" << path_builder << "': "
          << cpp_strerror(r) << dendl;
        return r;
      }
    }
  }

  return 0;
}

int LocalFileDriver::inject_lost_and_found(
    inodeno_t ino,
    const InodeStore &dentry)
{
  std::string lf_path = path + "/lost+found";
  int r = mkdir(lf_path.c_str(), 0755);
  if (r != 0 && r != -EPERM) {
    derr << "error creating directory: '" << lf_path << "': "
      << cpp_strerror(r) << dendl;
    return r;
  }
  
  std::string file_path = lf_path + "/" + lost_found_dname(ino);
  return inject_data(file_path, dentry.inode.size,
		     dentry.inode.layout.object_size, ino);
}

int LocalFileDriver::init_roots(int64_t data_pool_id)
{
  // Ensure that the path exists and is a directory
  bool exists;
  int r = check_roots(&exists);
  if (r != 0) {
    return r;
  }

  if (exists) {
    return 0;
  } else {
    return ::mkdir(path.c_str(), 0755);
  }
}

int LocalFileDriver::check_roots(bool *result)
{
  // Check if the path exists and is a directory
  DIR *d = ::opendir(path.c_str());
  if (d == NULL) {
    *result = false;
  } else {
    int r = closedir(d);
    if (r != 0) {
      // Weird, but maybe possible with e.g. stale FD on NFS mount?
      *result = false;
    } else {
      *result = true;
    }
  }

  return 0;
}

void MetadataTool::build_file_dentry(
    inodeno_t ino, uint64_t file_size, time_t file_mtime,
    const file_layout_t &layout, InodeStore *out)
{
  assert(out != NULL);

  out->inode.mode = 0500 | S_IFREG;
  out->inode.size = file_size;
  out->inode.max_size_ever = file_size;
  out->inode.mtime.tv.tv_sec = file_mtime;
  out->inode.atime.tv.tv_sec = file_mtime;
  out->inode.ctime.tv.tv_sec = file_mtime;

  out->inode.layout = layout;

  out->inode.truncate_seq = 1;
  out->inode.truncate_size = -1ull;

  out->inode.inline_data.version = CEPH_INLINE_NONE;

  out->inode.nlink = 1;
  out->inode.ino = ino;
  out->inode.version = 1;
  out->inode.backtrace_version = 1;
  out->inode.uid = g_conf->mds_root_ino_uid;
  out->inode.gid = g_conf->mds_root_ino_gid;
}

void MetadataTool::build_dir_dentry(
    inodeno_t ino, const frag_info_t &fragstat,
    const file_layout_t &layout, InodeStore *out)
{
  assert(out != NULL);

  out->inode.mode = 0755 | S_IFDIR;
  out->inode.dirstat = fragstat;
  out->inode.mtime.tv.tv_sec = fragstat.mtime;
  out->inode.atime.tv.tv_sec = fragstat.mtime;
  out->inode.ctime.tv.tv_sec = fragstat.mtime;

  out->inode.layout = layout;
  out->inode.dir_layout.dl_dir_hash = g_conf->mds_default_dir_hash;

  out->inode.truncate_seq = 1;
  out->inode.truncate_size = -1ull;

  out->inode.inline_data.version = CEPH_INLINE_NONE;

  out->inode.nlink = 1;
  out->inode.ino = ino;
  out->inode.version = 1;
  out->inode.backtrace_version = 1;
  out->inode.uid = g_conf->mds_root_ino_uid;
  out->inode.gid = g_conf->mds_root_ino_gid;
}

