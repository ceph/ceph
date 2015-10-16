// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
#include "common/errno.h"

#include "mds/SessionMap.h"
#include "mds/InoTable.h"
#include "mds/SnapServer.h"

#include "TableTool.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": "

void TableTool::usage()
{
  std::cout << "Usage: \n"
    << "  cephfs-table-tool <all|[mds rank]> <reset|show> <session|snap|inode>"
    << std::endl;

  generic_client_usage();
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
  rados.connect();
 
  int const pool_id = mdsmap->get_metadata_pool();
  dout(4) << "resolving pool " << pool_id << dendl;
  std::string pool_name;
  r = rados.pool_reverse_lookup(pool_id, &pool_name);
  if (r < 0) {
    derr << "Pool " << pool_id << " identified in MDS map not found in RADOS!" << dendl;
    return r;
  }

  dout(4) << "creating IoCtx.." << dendl;
  r = rados.ioctx_create(pool_name.c_str(), io);
  assert(r == 0);

  // Require at least 3 args <action> <table> <rank>
  if (argv.size() < 3) {
    usage();
    return -EINVAL;
  }

  const std::string rank_str = std::string(argv[0]);
  const std::string mode = std::string(argv[1]);
  const std::string table = std::string(argv[2]);

  if (rank_str == "all") {
    rank = MDS_RANK_NONE;
  } else {
    std::string rank_err;
    rank = strict_strtol(rank_str.c_str(), 10, &rank_err);
    if (!rank_err.empty()) {
      derr << "Bad rank '" << rank_str << "'" << dendl;
      usage();
    }
  }

  JSONFormatter jf(true);
  if (mode == "reset") {
    if (table == "session") {
      r = apply_rank_fn(&TableTool::_reset_session_table, &jf);
    } else if (table == "inode") {
      r = apply_rank_fn(&TableTool::_reset_ino_table, &jf);
    } else if (table == "snap") {
      r = _reset_snap_table(&jf);
    } else {
      derr << "Invalid table '" << table << "'" << dendl;
      usage();
      return -EINVAL;
    }
  } else if (mode == "show") {
    if (table == "session") {
      r = apply_rank_fn(&TableTool::_show_session_table, &jf);
    } else if (table == "inode") {
      r = apply_rank_fn(&TableTool::_show_ino_table, &jf);
    } else if (table == "snap") {
      r = _show_snap_table(&jf);
    } else {
      derr << "Invalid table '" << table << "'" << dendl;
      usage();
      return -EINVAL;
    }
  } else {
    derr << "Invalid mode '" << mode << "'" << dendl;
    usage();
    return -EINVAL;
  }

  // Subcommand should have written to formatter, flush it
  jf.flush(std::cout);
  std::cout << std::endl;
  return r;
}






/**
 * For a function that takes an MDS rank as an argument and
 * returns an error code, execute it either on all ranks (if
 * this->rank is MDS_RANK_NONE), or on the rank specified
 * by this->rank.
 */
int TableTool::apply_rank_fn(int (TableTool::*fptr) (mds_rank_t, Formatter*), Formatter *f)
{
  assert(f != NULL);

  int r = 0;
  std::set<mds_rank_t> apply_to_ranks;
  if (rank == MDS_RANK_NONE) {
    mdsmap->get_mds_set(apply_to_ranks);
  } else {
    apply_to_ranks.insert(rank);
  }

  f->open_object_section("ranks");

  for (std::set<mds_rank_t>::iterator rank_i = apply_to_ranks.begin();
      rank_i != apply_to_ranks.end(); ++rank_i) {
    std::ostringstream rank_str;
    rank_str << *rank_i;
    f->open_object_section(rank_str.str().c_str());

    f->open_object_section("data");
    int rank_r = (this->*fptr)(*rank_i, f);
    f->close_section();
    r = r ? r : rank_r;

    f->dump_int("result", rank_r);
    f->close_section();
  }

  f->close_section();

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
private:
  // The RADOS object ID for the table
  std::string object_name;

  // The rank in question (may be NONE)
  mds_rank_t rank;

  // Whether this is an MDSTable subclass (i.e. has leading version field to decode)
  bool mds_table;

public:
  TableHandler(mds_rank_t r, std::string const &name, bool mds_table_)
    : rank(r), mds_table(mds_table_)
  {
    // Compose object name of the table we will dump
    std::ostringstream oss;
    oss << "mds";
    if (rank != MDS_RANK_NONE) {
      oss << rank;
    }
    oss << "_" << name;
    object_name = oss.str();
  }

  int load_and_dump(librados::IoCtx *io, Formatter *f)
  {
    assert(io != NULL);
    assert(f != NULL);

    // Attempt read
    bufferlist table_bl;
    int read_r = io->read(object_name, table_bl, 0, 0);
    if (read_r >= 0) {
      bufferlist::iterator q = table_bl.begin();
      try {
        if (mds_table) {
          version_t version;
          ::decode(version, q);
          f->dump_int("version", version);
        }
        A table_inst;
        table_inst.set_rank(rank);
        table_inst.decode(q);
        table_inst.dump(f);

        return 0;
      } catch (buffer::error &e) {
        derr << "table " << object_name << " is corrupt" << dendl;
        return -EIO;
      }
    } else {
      derr << "error reading table object " << object_name
        << ": " << cpp_strerror(read_r) << dendl;
      return read_r;
    }
  }

  int reset(librados::IoCtx *io)
  {
    A table_inst;
    table_inst.set_rank(rank);
    table_inst.reset_state();
    
    // Compose new (blank) table
    bufferlist new_bl;
    if (mds_table) {
      version_t version = 1;
      ::encode(version, new_bl);
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

  // The rank in question (may be NONE)
  mds_rank_t rank;

  // Whether this is an MDSTable subclass (i.e. has leading version field to decode)
  bool mds_table;

public:
  TableHandlerOmap(mds_rank_t r, std::string const &name, bool mds_table_)
    : rank(r), mds_table(mds_table_)
  {
    // Compose object name of the table we will dump
    std::ostringstream oss;
    oss << "mds";
    if (rank != MDS_RANK_NONE) {
      oss << rank;
    }
    oss << "_" << name;
    object_name = oss.str();
  }

  int load_and_dump(librados::IoCtx *io, Formatter *f)
  {
    assert(io != NULL);
    assert(f != NULL);

    // Read in the header
    bufferlist header_bl;
    int r = io->omap_get_header(object_name, &header_bl);
    if (r != 0) {
      derr << "error reading header: " << cpp_strerror(r) << dendl;
      return r;
    }

    // Decode the header
    A table_inst;
    table_inst.set_rank(rank);
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
          g_conf->mds_sessionmap_keys_per_op, &values);

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
    table_inst.set_rank(rank);
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

int TableTool::_show_session_table(mds_rank_t rank, Formatter *f)
{
  return TableHandlerOmap<SessionMapStore>(rank, "sessionmap", false).load_and_dump(&io, f);
}

int TableTool::_reset_session_table(mds_rank_t rank, Formatter *f)
{
  return TableHandlerOmap<SessionMapStore>(rank, "sessionmap", false).reset(&io);
}

int TableTool::_show_ino_table(mds_rank_t rank, Formatter *f)
{
  return TableHandler<InoTable>(rank, "inotable", true).load_and_dump(&io, f);;
}

int TableTool::_reset_ino_table(mds_rank_t rank, Formatter *f)
{
  return TableHandler<InoTable>(rank, "inotable", true).reset(&io);
}

int TableTool::_show_snap_table(Formatter *f)
{
  int r;

  f->open_object_section("show_snap_table");
  {
    r = TableHandler<SnapServer>(MDS_RANK_NONE, "snaptable", true).load_and_dump(&io, f);
    f->dump_int("result", r);
  }
  f->close_section();

  return r;
}

int TableTool::_reset_snap_table(Formatter *f)
{
  int r = TableHandler<SnapServer>(MDS_RANK_NONE, "snaptable", true).reset(&io);
  f->open_object_section("reset_snap_status");
  f->dump_int("result", r);
  f->close_section();
  return r;
}

