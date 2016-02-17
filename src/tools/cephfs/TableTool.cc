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
    << "  cephfs-table-tool <all|[mds rank]> <take_inos> <max_ino>"
    << std::endl;

  generic_client_usage();
}


/**
 * For a function that takes an MDS role as an argument and
 * returns an error code, execute it on the roles specified
 * by `role_selector`.
 */
int TableTool::apply_role_fn(std::function<int(mds_role_t, Formatter *)> fptr, Formatter *f)
{
  assert(f != NULL);

  int r = 0;

  f->open_object_section("ranks");

  for (auto role : role_selector.get_roles()) {
    std::ostringstream rank_str;
    rank_str << role.rank;
    f->open_object_section(rank_str.str().c_str());

    f->open_object_section("data");
    int rank_r = fptr(role, f);
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
        table_inst.set_rank(role.rank);
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
    assert(io != NULL);
    assert(f != NULL);

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
 


  // Require at least 3 args <rank> <mode> <arg> [args...]
  if (argv.size() < 3) {
    usage();
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

  auto fs =  fsmap->get_filesystem(role_selector.get_ns());
  assert(fs != nullptr);
  int const pool_id = fs->mds_map.get_metadata_pool();
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
      return r;
    } else {
      derr << "Invalid table '" << table << "'" << dendl;
      usage();
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
      derr << "Invalid table '" << table << "'" << dendl;
      usage();
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

