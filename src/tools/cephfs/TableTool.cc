// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/JSONFormatter.h"
#include "include/str_list.h"

#include "mds/SessionMap.h"
#include "mds/InoTable.h"
#include "mds/SnapServer.h"
#include "messages/MRemoveSnaps.h"

#include "TableTool.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": "

using namespace std;

void TableTool::usage()
{
  std::cout << "Usage: \n"
    << "  cephfs-table-tool <all|[mds rank]> <reset|show> <session|snap|inode>"
    << "  cephfs-table-tool <all|[mds rank]> <take_inos> <max_ino>"
    << "\n  cephfs-table-tool <all|[mds rank]> purge_snaps <pool> <snapid>[-<snapid>][,...]"
    << " [--yes-i-really-mean-it]"
    << std::endl;

  generic_client_usage();
}


bool TableTool::ms_dispatch(Message *m)
{
  if (m->get_type() != MSG_REMOVE_SNAPS) {
    return MDSUtility::ms_dispatch(m);
  }
  dout(10) << *m << dendl;
  {
    std::lock_guard locker{lock};
    remove_snaps_acked = true;
  }
  remove_snaps_cond.notify_all();
  m->put();
  return true;
}


/**
 * For a function that takes an MDS role as an argument and
 * returns an error code, execute it on the roles specified
 * by `role_selector`.
 */
int TableTool::apply_role_fn(std::function<int(mds_role_t, Formatter *)> fptr, Formatter *f)
{
  ceph_assert(f != NULL);

  int r = 0;
  auto roles = role_selector.get_roles();
  // Start progress tracking for multiple ranks (if more than 1)
  if (roles.size() > 1) {
    progress_tracker->start(roles.size());
  }

  f->open_object_section("ranks");

  for (auto role : roles) {
    std::ostringstream rank_str;
    rank_str << role.rank;
    f->open_object_section(rank_str.str().c_str());

    f->open_object_section("data");
    int rank_r = fptr(role, f);
    f->close_section();
    r = r ? r : rank_r;

    f->dump_int("result", rank_r);
    f->close_section();
    // Update progress after processing each rank (only if multiple ranks)
    if (roles.size() > 1) {
      progress_tracker->increment();
      progress_tracker->display_progress();
    }
  }

  f->close_section();
  
  // Display final summary at the end (only if multiple ranks)
  if (roles.size() > 1) {
    progress_tracker->display_final_summary();
  }

  // Display final summary at the end (only if multiple ranks)
  if (roles.size() > 1) {
    progress_tracker->display_final_summary();
  }

  return r;
}


/**
 * This class wraps an MDS table class (SessionMap, SnapServer, InoTable)
 * with offline load/store code such that we can do offline dumps and resets
 * on those tables.
 */
template <typename A>
class TableHandler
{
protected:
  // The RADOS object ID for the table
  std::string object_name;

  // The role in question (may be NONE)
  mds_role_t role;

  // Whether this is an MDSTable subclass (i.e. has leading version field to decode)
  bool mds_table;

public:
  TableHandler(mds_role_t r, std::string const &name, bool mds_table_)
    : role(r), mds_table(mds_table_)
  {
    // Compose object name of the table we will dump
    std::ostringstream oss;
    oss << "mds";
    if (!role.is_none()) {
      oss << role.rank;
    }
    oss << "_" << name;
    object_name = oss.str();
  }

  int load(librados::IoCtx *io, A *table_inst, version_t *version)
  {
    ceph_assert(io != NULL);

    // Attempt read
    bufferlist table_bl;
    int read_r = io->read(object_name, table_bl, 0, 0);
    if (read_r < 0) {
      derr << "error reading table object " << object_name
        << ": " << cpp_strerror(read_r) << dendl;
      return read_r;
    }
    auto q = table_bl.cbegin();
    try {
      if (mds_table) {
        decode(*version, q);
      }
      table_inst->set_rank(role.rank);
      table_inst->decode(q);
    } catch (buffer::error &e) {
      derr << "table " << object_name << " is corrupt" << dendl;
      return -EIO;
    }
    return 0;
  }

  int load_and_dump(librados::IoCtx *io, Formatter *f)
  {
    ceph_assert(f != NULL);

    A table_inst;
    version_t version;
    int r = load(io, &table_inst, &version);
    if (r < 0) {
      return r;
    }
    if (mds_table) {
      f->dump_int("version", version);
    }
    table_inst.dump(f);
    return 0;
  }

  int reset(librados::IoCtx *io)
  {
    A table_inst;
    // Compose new (blank) table
    table_inst.set_rank(role.rank);
    table_inst.reset_state();
    // Write the table out
    return write(table_inst, io);
  }

protected:

  int write(const A &table_inst, librados::IoCtx *io)
  {
    bufferlist new_bl;
    if (mds_table) {
      version_t version = 1;
      encode(version, new_bl);
    }
    table_inst.encode_state(new_bl);

    // Write out new table
    int r = io->write_full(object_name, new_bl);
    if (r != 0) {
      derr << "error writing table object " << object_name
        << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    return r;
  }
};

template <typename A>
class TableHandlerOmap
{
private:
  // The RADOS object ID for the table
  std::string object_name;

  // The role (rank may be NONE)
  mds_role_t role;

  // Whether this is an MDSTable subclass (i.e. has leading version field to decode)
  bool mds_table;

public:
  TableHandlerOmap(mds_role_t r, std::string const &name, bool mds_table_)
    : role(r), mds_table(mds_table_)
  {
    // Compose object name of the table we will dump
    std::ostringstream oss;
    oss << "mds";
    if (!role.is_none()) {
      oss << role.rank;
    }
    oss << "_" << name;
    object_name = oss.str();
  }

  int load_and_dump(librados::IoCtx *io, Formatter *f)
  {
    ceph_assert(io != NULL);
    ceph_assert(f != NULL);

    // Read in the header
    bufferlist header_bl;
    int r = io->omap_get_header(object_name, &header_bl);
    if (r != 0) {
      derr << "error reading header on '" << object_name << "': "
           << cpp_strerror(r) << dendl;
      return r;
    }

    // Decode the header
    A table_inst;
    table_inst.set_rank(role.rank);
    try {
      table_inst.decode_header(header_bl);
    } catch (buffer::error &e) {
      derr << "table " << object_name << " is corrupt" << dendl;
      return -EIO;
    }

    // Read and decode OMAP values in chunks
    std::string last_key = "";
    while(true) {
      std::map<std::string, bufferlist> values;
      int r = io->omap_get_vals(object_name, last_key,
          g_conf()->mds_sessionmap_keys_per_op, &values);

      if (r != 0) {
        derr << "error reading values: " << cpp_strerror(r) << dendl;
        return r;
      }

      if (values.empty()) {
        break;
      }

      try {
        table_inst.decode_values(values);
      } catch (buffer::error &e) {
        derr << "table " << object_name << " is corrupt" << dendl;
        return -EIO;
      }
      last_key = values.rbegin()->first;
    }

    table_inst.dump(f);

    return 0;
  }

  int reset(librados::IoCtx *io)
  {
    A table_inst;
    table_inst.set_rank(role.rank);
    table_inst.reset_state();
    bufferlist header_bl;
    table_inst.encode_header(&header_bl);

    // Compose a transaction to clear and write header
    librados::ObjectWriteOperation op;
    op.omap_clear();
    op.set_op_flags2(LIBRADOS_OP_FLAG_FAILOK);
    op.omap_set_header(header_bl);
    
    return io->operate(object_name, &op);
  }
};

class InoTableHandler : public TableHandler<InoTable>
{
  public:
  explicit InoTableHandler(mds_role_t r)
    : TableHandler(r, "inotable", true)
  {}

  int take_inos(librados::IoCtx *io, inodeno_t max, Formatter *f)
  {
    InoTable inst;
    inst.set_rank(role.rank);
    inst.reset_state();

    int r = 0;
    if (inst.force_consume_to(max)) {
      r = write(inst, io);
    }

    f->dump_int("version", inst.get_version());
    inst.dump(f);

    return r;
  }
};


/**
 * Ask the monitors to remove snapids from a data pool that was detached from
 * the file system (`fs rm_data_pool`) while snapshots referenced objects in it,
 * so that the OSDs trim the clones the MDS never purged there. Snapids that
 * the snap table in RADOS still references, or that are newer than its
 * last_snap, are skipped; a table older than the MDS journal only makes us
 * skip more.
 */
int TableTool::purge_snaps(const std::string &pool_str,
                           const std::string &snapids_str,
                           bool really, Formatter *f)
{
  interval_set<snapid_t> requested;
  for (const auto &part : get_str_vec(snapids_str, ",")) {
    auto dash = part.find('-');
    std::string err;
    long long first = strict_strtoll(part.substr(0, dash), 10, &err);
    long long last = dash == std::string::npos ? first :
      strict_strtoll(part.substr(dash + 1), 10, &err);
    if (!err.empty() || first <= 0 || last < first) {
      derr << "Bad snapid range '" << part << "'" << dendl;
      return -EINVAL;
    }
    requested.union_insert(first, last - first + 1);
  }

  int64_t pool_id = -ENOENT;
  pg_pool_t pool;
  objecter->with_osdmap([&](const OSDMap &o) {
    pool_id = o.lookup_pg_pool_name(pool_str);
    if (pool_id < 0) {
      std::string err;
      pool_id = strict_strtoll(pool_str, 10, &err);
      if (!err.empty() || !o.have_pg_pool(pool_id)) {
        pool_id = -ENOENT;
        return;
      }
    }
    pool = *o.get_pg_pool(pool_id);
  });
  if (pool_id < 0) {
    derr << "Pool '" << pool_str << "' not found" << dendl;
    return -ENOENT;
  }

  const std::string fs_name =
    fsmap->get_filesystem(role_selector.get_ns()).get_mds_map().get_fs_name();
  if (fsmap->pool_in_use(pool_id)) {
    derr << "Pool " << pool_id << " is in use by a file system, whose MDS "
         << "purges its snapshots" << dendl;
    return -EBUSY;
  }
  if (pool.is_pool_snaps_mode()) {
    derr << "Pool " << pool_id << " uses pool snapshots" << dendl;
    return -EINVAL;
  }
  // `fs rm_data_pool` leaves the tag that `fs add_data_pool` set
  auto app = pool.application_metadata.find(pg_pool_t::APPLICATION_NAME_CEPHFS);
  if (app == pool.application_metadata.end() || app->second["data"] != fs_name) {
    derr << "Pool " << pool_id << " was not a data pool of file system '"
         << fs_name << "'" << dendl;
    return -EINVAL;
  }
  // the selfmanaged snapids of another application can equal ours
  for (const auto &[app_name, app_meta] : pool.application_metadata) {
    if (app_name != pg_pool_t::APPLICATION_NAME_CEPHFS) {
      derr << "Pool " << pool_id << " also has application '" << app_name
           << "' enabled" << dendl;
      return -EINVAL;
    }
  }

  SnapServer table;
  version_t version;
  int r = TableHandler<SnapServer>(
      mds_role_t(), "snaptable", true).load(&io, &table, &version);
  if (r < 0) {
    return r;
  }

  const snapid_t last_snap = table.get_last_snap();
  interval_set<snapid_t> referenced, newer, purge;
  for (auto p = requested.begin(); p != requested.end(); ++p) {
    for (snapid_t s = p.get_start(); s < p.get_end(); ++s) {
      if (s > last_snap) {
        newer.insert(s, p.get_end() - s);
        break;
      }
      if (table.is_snap_referenced(s)) {
        referenced.insert(s, 1);
      } else {
        purge.insert(s, 1);
      }
    }
  }

  f->dump_int("pool", pool_id);
  f->dump_unsigned("last_snap", last_snap);
  f->dump_stream("skipped_referenced") << referenced;
  f->dump_stream("skipped_newer") << newer;
  f->dump_stream("purge") << purge;
  f->dump_bool("dry_run", !really);

  if (!referenced.empty() || !newer.empty()) {
    std::cerr << "If snapshots were deleted recently, run 'ceph tell mds."
              << fs_name << ":0 flush journal' and try again" << std::endl;
  }
  if (purge.empty()) {
    return 0;
  }
  if (!really) {
    std::cerr << "Dry run; add --yes-i-really-mean-it to purge" << std::endl;
    return 0;
  }

  // Unlike the selfmanaged snap pool op, MRemoveSnaps also raises the pool's
  // snap_seq up to the snapids it removes.
  std::map<int, std::vector<snapid_t>> snaps;
  auto &v = snaps[pool_id];
  for (auto p = purge.begin(); p != purge.end(); ++p) {
    for (snapid_t s = p.get_start(); s < p.get_end(); ++s) {
      v.push_back(s);
    }
  }
  monc->send_mon_message(ceph::make_message<MRemoveSnaps>(snaps).detach());

  // the monitors drop the request without replying if our caps lack
  // 'osd pool rmsnap'
  auto timeout = g_conf().get_val<std::chrono::seconds>("rados_mon_op_timeout");
  auto acked = [this] { return remove_snaps_acked; };
  std::unique_lock locker{lock};
  if (timeout.count() == 0) {
    remove_snaps_cond.wait(locker, acked);
  } else if (!remove_snaps_cond.wait_for(locker, timeout, acked)) {
    derr << "No reply from the monitors; check that the client is allowed "
         << "'osd pool rmsnap'" << dendl;
    return -ETIMEDOUT;
  }
  return 0;
}


int TableTool::main(std::vector<const char*> &argv)
{
  int r;

  dout(10) << __func__ << dendl;

  // RADOS init
  // ==========
  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "RADOS unavailable, cannot scan filesystem journal" << dendl;
    return r;
  }

  dout(4) << "connecting to RADOS..." << dendl;
  r = rados.connect();
  if (r < 0) {
    derr << "couldn't connect to cluster: " << cpp_strerror(r) << dendl;
    return r;
  }

  // Require at least 3 args <rank> <mode> <arg> [args...]
  if (argv.size() < 3) {
    cerr << "missing required 3 arguments" << std::endl;
    return -EINVAL;
  }

  const std::string role_str = std::string(argv[0]);
  const std::string mode = std::string(argv[1]);
  const std::string table = std::string(argv[2]);

  r = role_selector.parse(*fsmap, role_str);
  if (r < 0) {
    derr << "Bad rank selection: " << role_str << "'" << dendl;
    return r;
  }

  auto& fs = fsmap->get_filesystem(role_selector.get_ns());
  int64_t const pool_id = fs.get_mds_map().get_metadata_pool();
  dout(4) << "resolving pool " << pool_id << dendl;
  std::string pool_name;
  r = rados.pool_reverse_lookup(pool_id, &pool_name);
  if (r < 0) {
    derr << "Pool " << pool_id << " identified in MDS map not found in RADOS!"
         << dendl;
    return r;
  }

  dout(4) << "creating IoCtx.." << dendl;
  r = rados.ioctx_create(pool_name.c_str(), io);
  if (r != 0) {
    return r;
  }

  JSONFormatter jf(true);
  if (mode == "reset") {
    const std::string table = std::string(argv[2]);
    if (table == "session") {
      r = apply_role_fn([this](mds_role_t rank, Formatter *f) -> int {
            return TableHandlerOmap<SessionMapStore>(rank, "sessionmap", false).reset(&io);
      }, &jf);
    } else if (table == "inode") {
      r = apply_role_fn([this](mds_role_t rank, Formatter *f) -> int {
            return TableHandler<InoTable>(rank, "inotable", true).reset(&io);
      }, &jf);
    } else if (table == "snap") {
      r = TableHandler<SnapServer>(mds_role_t(), "snaptable", true).reset(&io);
      jf.open_object_section("reset_snap_status");
      jf.dump_int("result", r);
      jf.close_section();
    } else {
      cerr << "Invalid table '" << table << "'" << std::endl;
      return -EINVAL;
    }
  } else if (mode == "show") {
    const std::string table = std::string(argv[2]);
    if (table == "session") {
      r = apply_role_fn([this](mds_role_t rank, Formatter *f) -> int {
        return TableHandlerOmap<SessionMapStore>(rank, "sessionmap", false).load_and_dump(&io, f);
      }, &jf);
    } else if (table == "inode") {
      r = apply_role_fn([this](mds_role_t rank, Formatter *f) -> int {
        return TableHandler<InoTable>(rank, "inotable", true).load_and_dump(&io, f);;
      }, &jf);
    } else if (table == "snap") {
      jf.open_object_section("show_snap_table");
      {
        r = TableHandler<SnapServer>(
            mds_role_t(), "snaptable", true).load_and_dump(&io, &jf);
        jf.dump_int("result", r);
      }
      jf.close_section();
    } else {
      cerr << "Invalid table '" << table << "'" << std::endl;
      return -EINVAL;
    }
  } else if (mode == "take_inos") {
    const std::string ino_str = std::string(argv[2]);
    std::string ino_err;
    inodeno_t ino = strict_strtoll(ino_str.c_str(), 10, &ino_err);
    if (!ino_err.empty()) {
      derr << "Bad ino '" << ino_str << "'" << dendl;
      return -EINVAL;
    }
    r = apply_role_fn([this, ino](mds_role_t rank, Formatter *f) -> int {
      return InoTableHandler(rank).take_inos(&io, ino, f);
    }, &jf);
  } else if (mode == "purge_snaps") {
    bool really = argv.size() == 5 &&
      std::string(argv[4]) == "--yes-i-really-mean-it";
    if (argv.size() < 4 || (argv.size() > 4 && !really)) {
      cerr << "Usage: purge_snaps <pool> <snapids> [--yes-i-really-mean-it]"
           << std::endl;
      return -EINVAL;
    }
    jf.open_object_section("purge_snaps");
    r = purge_snaps(argv[2], argv[3], really, &jf);
    jf.dump_int("result", r);
    jf.close_section();
  } else {
    cerr << "Invalid mode '" << mode << "'" << std::endl;
    return -EINVAL;
  }

  // Subcommand should have written to formatter, flush it
  jf.flush(std::cout);
  std::cout << std::endl;
  return r;
}

