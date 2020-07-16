// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * ceph - scalable distributed file system
 *
 * copyright (c) 2014 john spray <john.spray@inktank.com>
 *
 * this is free software; you can redistribute it and/or
 * modify it under the terms of the gnu lesser general public
 * license version 2.1, as published by the free software
 * foundation.  see file copying.
 */


#include <sstream>

#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "osdc/Journaler.h"
#include "mds/mdstypes.h"
#include "mds/LogEvent.h"
#include "mds/InoTable.h"

#include "mds/events/ENoOp.h"
#include "mds/events/EUpdate.h"

#include "JournalScanner.h"
#include "EventOutput.h"
#include "Dumper.h"
#include "Resetter.h"

#include "JournalTool.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": "



void JournalTool::usage()
{
  std::cout << "Usage: \n"
    << "  cephfs-journal-tool [options] journal <command>\n"
    << "    <command>:\n"
    << "      inspect\n"
    << "      import <path> [--force]\n"
    << "      export <path>\n"
    << "      reset [--force]\n"
    << "  cephfs-journal-tool [options] header <get|set> <field> <value>\n"
    << "    <field>: [trimmed_pos|expire_pos|write_pos|pool_id]\n"
    << "  cephfs-journal-tool [options] event <effect> <selector> <output> [special options]\n"
    << "    <selector>:\n"
    << "      --range=<start>..<end>\n"
    << "      --path=<substring>\n"
    << "      --inode=<integer>\n"
    << "      --type=<UPDATE|OPEN|SESSION...><\n"
    << "      --frag=<ino>.<frag> [--dname=<dentry string>]\n"
    << "      --client=<session id integer>\n"
    << "    <effect>: [get|recover_dentries|splice]\n"
    << "    <output>: [summary|list|binary|json] [--path <path>]\n"
    << "\n"
    << "General options:\n"
    << "  --rank=filesystem:mds-rank|all Journal rank (mandatory)\n"
    << "  --journal=<mdlog|purge_queue>  Journal type (purge_queue means\n"
    << "                                 this journal is used to queue for purge operation,\n"
    << "                                 default is mdlog, and only mdlog support event mode)\n"
    << "\n"
    << "Special options\n"
    << "  --alternate-pool <name>     Alternative metadata pool to target\n"
    << "                              when using recover_dentries.\n";

  generic_client_usage();
}


/**
 * Handle arguments and hand off to journal/header/event mode
 */
int JournalTool::main(std::vector<const char*> &argv)
{
  int r;

  dout(10) << "JournalTool::main " << dendl;
  // Common arg parsing
  // ==================
  if (argv.empty()) {
    cerr << "missing positional argument" << std::endl;
    return -EINVAL;
  }

  std::vector<const char*>::iterator arg = argv.begin();

  std::string rank_str;
  if (!ceph_argparse_witharg(argv, arg, &rank_str, "--rank", (char*)NULL)) {
    derr << "missing mandatory \"--rank\" argument" << dendl;
    return -EINVAL;
  }

  if (!ceph_argparse_witharg(argv, arg, &type, "--journal", (char*)NULL)) {
    // Default is mdlog
    type = "mdlog";
  }
  
  r = validate_type(type);
  if (r != 0) {
    derr << "journal type is not correct." << dendl;
    return r;
  }

  r = role_selector.parse(*fsmap, rank_str, false);
  if (r != 0) {
    derr << "Couldn't determine MDS rank." << dendl;
    return r;
  }

  std::string mode;
  if (arg == argv.end()) {
    derr << "Missing mode [journal|header|event]" << dendl;
    return -EINVAL;
  }
  mode = std::string(*arg);
  arg = argv.erase(arg);

  // RADOS init
  // ==========
  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "RADOS unavailable, cannot scan filesystem journal" << dendl;
    return r;
  }

  dout(4) << "JournalTool: connecting to RADOS..." << dendl;
  r = rados.connect();
  if (r < 0) {
    derr << "couldn't connect to cluster: " << cpp_strerror(r) << dendl;
    return r;
  }
 
  auto fs = fsmap->get_filesystem(role_selector.get_ns());
  ceph_assert(fs != nullptr);
  int64_t const pool_id = fs->mds_map.get_metadata_pool();
  dout(4) << "JournalTool: resolving pool " << pool_id << dendl;
  std::string pool_name;
  r = rados.pool_reverse_lookup(pool_id, &pool_name);
  if (r < 0) {
    derr << "Pool " << pool_id << " named in MDS map not found in RADOS!" << dendl;
    return r;
  }

  dout(4) << "JournalTool: creating IoCtx.." << dendl;
  r = rados.ioctx_create(pool_name.c_str(), input);
  ceph_assert(r == 0);
  output.dup(input);

  // Execution
  // =========
  // journal and header are general journal mode
  // event mode is only specific for mdlog
  auto roles = role_selector.get_roles();
  if (roles.size() > 1) {
    const std::string &command = argv[0];
    bool allowed = can_execute_for_all_ranks(mode, command);
    if (!allowed) {
      derr << "operation not allowed for all ranks" << dendl;
      return -EINVAL;
    }

    all_ranks = true;
  }
  for (auto role : roles) {
    rank = role.rank;
    std::vector<const char *> rank_argv(argv);
    dout(4) << "Executing for rank " << rank << dendl;
    if (mode == std::string("journal")) {
      r = main_journal(rank_argv);
    } else if (mode == std::string("header")) {
      r = main_header(rank_argv);
    } else if (mode == std::string("event")) {
      r = main_event(rank_argv);
    } else {
      cerr << "Bad command '" << mode << "'" << std::endl;
      return -EINVAL;
    }

    if (r != 0) {
      return r;
    }
  }

  return r;
}

int JournalTool::validate_type(const std::string &type)
{
  if (type == "mdlog" || type == "purge_queue") {
    return 0;
  }
  return -1;
}

std::string JournalTool::gen_dump_file_path(const std::string &prefix) {
  if (!all_ranks) {
    return prefix;
  }

  return prefix + "." + std::to_string(rank);
}

bool JournalTool::can_execute_for_all_ranks(const std::string &mode,
                                            const std::string &command) {
  if (mode == "journal" && command == "import") {
    return false;
  }

  return true;
}

/**
 * Handle arguments for 'journal' mode
 *
 * This is for operations that act on the journal as a whole.
 */
int JournalTool::main_journal(std::vector<const char*> &argv)
{
  if (argv.empty()) {
    derr << "Missing journal command, please see help" << dendl;
    return -EINVAL;
  }

  std::string command = argv[0];
  if (command == "inspect") {
    return journal_inspect();
  } else if (command == "export" || command == "import") {
    bool force = false;
    if (argv.size() >= 2) {
      std::string const path = argv[1];
      if (argv.size() == 3) {
        if (std::string(argv[2]) == "--force") {
          force = true;
        } else {
          std::cerr << "Unknown argument " << argv[1] << std::endl;
          return -EINVAL;
        }
      }
      return journal_export(path, command == "import", force);
    } else {
      derr << "Missing path" << dendl;
      return -EINVAL;
    }
  } else if (command == "reset") {
    bool force = false;
    if (argv.size() == 2) {
      if (std::string(argv[1]) == "--force") {
        force = true;
      } else {
        std::cerr << "Unknown argument " << argv[1] << std::endl;
        return -EINVAL;
      }
    } else if (argv.size() > 2) {
      std::cerr << "Too many arguments!" << std::endl;
      return -EINVAL;
    }
    return journal_reset(force);
  } else {
    derr << "Bad journal command '" << command << "'" << dendl;
    return -EINVAL;
  }
}


/**
 * Parse arguments and execute for 'header' mode
 *
 * This is for operations that act on the header only.
 */
int JournalTool::main_header(std::vector<const char*> &argv)
{
  JournalFilter filter(type);
  JournalScanner js(input, rank, type, filter);
  int r = js.scan(false);
  if (r < 0) {
    std::cerr << "Unable to scan journal" << std::endl;
    return r;
  }

  if (!js.header_present) {
    std::cerr << "Header object not found!" << std::endl;
    return -ENOENT;
  } else if (!js.header_valid && js.header == NULL) {
    // Can't do a read or a single-field write without a copy of the original
    derr << "Header could not be read!" << dendl;
    return -ENOENT;
  } else {
    ceph_assert(js.header != NULL);
  }

  if (argv.empty()) {
    derr << "Missing header command, must be [get|set]" << dendl;
    return -EINVAL;
  }
  std::vector<const char *>::iterator arg = argv.begin();
  std::string const command = *arg;
  arg = argv.erase(arg);

  if (command == std::string("get")) {
    // Write JSON journal dump to stdout
    JSONFormatter jf(true);
    js.header->dump(&jf);
    jf.flush(std::cout);
    std::cout << std::endl;
  } else if (command == std::string("set")) {
    // Need two more args <key> <val>
    if (argv.size() != 2) {
      derr << "'set' requires two arguments <trimmed_pos|expire_pos|write_pos> <value>" << dendl;
      return -EINVAL;
    }

    std::string const field_name = *arg;
    arg = argv.erase(arg);

    std::string const value_str = *arg;
    arg = argv.erase(arg);
    ceph_assert(argv.empty());

    std::string parse_err;
    uint64_t new_val = strict_strtoll(value_str.c_str(), 0, &parse_err);
    if (!parse_err.empty()) {
      derr << "Invalid value '" << value_str << "': " << parse_err << dendl;
      return -EINVAL;
    }

    uint64_t *field = NULL;
    if (field_name == "trimmed_pos") {
      field = &(js.header->trimmed_pos);
    } else if (field_name == "expire_pos") {
      field = &(js.header->expire_pos);
    } else if (field_name == "write_pos") {
      field = &(js.header->write_pos);
    } else if (field_name == "pool_id") {
      field = (uint64_t*)(&(js.header->layout.pool_id));
    } else {
      derr << "Invalid field '" << field_name << "'" << dendl;
      return -EINVAL;
    }

    std::cout << "Updating " << field_name << std::hex << " 0x" << *field << " -> 0x" << new_val << std::dec << std::endl;
    *field = new_val;

    dout(4) << "Writing object..." << dendl;
    bufferlist header_bl;
    encode(*(js.header), header_bl);
    output.write_full(js.obj_name(0), header_bl);
    dout(4) << "Write complete." << dendl;
    std::cout << "Successfully updated header." << std::endl;
  } else {
    derr << "Bad header command '" << command << "'" << dendl;
    return -EINVAL;
  }

  return 0;
}


/**
 * Parse arguments and execute for 'event' mode
 *
 * This is for operations that act on LogEvents within the log
 */
int JournalTool::main_event(std::vector<const char*> &argv)
{
  int r;

  if (argv.empty()) {
    derr << "Missing event command, please see help" << dendl;
    return -EINVAL;
  }

  std::vector<const char*>::iterator arg = argv.begin();
  bool dry_run = false;

  std::string command = *(arg++);
  if (command != "get" && command != "splice" && command != "recover_dentries") {
    derr << "Unknown argument '" << command << "'" << dendl;
    return -EINVAL;
  }

  if (command == "recover_dentries") {
    if (type != "mdlog") {
      derr << "journaler for " << type << " can't do \"recover_dentries\"." << dendl;
      return -EINVAL;
    } else {
      if (arg != argv.end() && ceph_argparse_flag(argv, arg, "--dry_run", (char*)NULL)) {
        dry_run = true;
      }
    }
  }

  if (arg == argv.end()) {
    derr << "Incomplete command line" << dendl;
    return -EINVAL;
  }

  // Parse filter options
  // ====================
  JournalFilter filter(type);
  r = filter.parse_args(argv, arg);
  if (r) {
    return r;
  }

  // Parse output options
  // ====================
  if (arg == argv.end()) {
    cerr << "Missing output command" << std::endl;
    return -EINVAL;
  }
  std::string output_style = *(arg++);
  if (output_style != "binary" && output_style != "json" &&
      output_style != "summary" && output_style != "list") {
    cerr << "Unknown argument: '" << output_style << "'" << std::endl;
    return -EINVAL;
  }

  std::string output_path = "dump";
  while(arg != argv.end()) {
    std::string arg_str;
    if (ceph_argparse_witharg(argv, arg, &arg_str, "--path", (char*)NULL)) {
      output_path = arg_str;
    } else if (ceph_argparse_witharg(argv, arg, &arg_str, "--alternate-pool",
				     nullptr)) {
      dout(1) << "Using alternate pool " << arg_str << dendl;
      int r = rados.ioctx_create(arg_str.c_str(), output);
      ceph_assert(r == 0);
      other_pool = true;
    } else {
      cerr << "Unknown argument: '" << *arg << "'" << std::endl;
      return -EINVAL;
    }
  }

  const std::string dump_path = gen_dump_file_path(output_path);

  // Execute command
  // ===============
  JournalScanner js(input, rank, type, filter);
  if (command == "get") {
    r = js.scan();
    if (r) {
      derr << "Failed to scan journal (" << cpp_strerror(r) << ")" << dendl;
      return r;
    }
  } else if (command == "recover_dentries") {
    r = js.scan();
    if (r) {
      derr << "Failed to scan journal (" << cpp_strerror(r) << ")" << dendl;
      return r;
    }

    /**
     * Iterate over log entries, attempting to scavenge from each one
     */
    std::set<inodeno_t> consumed_inos;
    for (JournalScanner::EventMap::iterator i = js.events.begin();
         i != js.events.end(); ++i) {
      auto& le = i->second.log_event;
      EMetaBlob const *mb = le->get_metablob();
      if (mb) {
        int scav_r = recover_dentries(*mb, dry_run, &consumed_inos);
        if (scav_r) {
          dout(1) << "Error processing event 0x" << std::hex << i->first << std::dec
                  << ": " << cpp_strerror(scav_r) << ", continuing..." << dendl;
          if (r == 0) {
            r = scav_r;
          }
          // Our goal is to read all we can, so don't stop on errors, but
          // do record them for possible later output
          js.errors.insert(std::make_pair(i->first,
                JournalScanner::EventError(scav_r, cpp_strerror(r))));
        }
      }
    }

    /**
     * Update InoTable to reflect any inode numbers consumed during scavenge
     */
    dout(4) << "consumed " << consumed_inos.size() << " inodes" << dendl;
    if (consumed_inos.size() && !dry_run) {
      int consume_r = consume_inos(consumed_inos);
      if (consume_r) {
        dout(1) << "Error updating InoTable for " << consumed_inos.size()
                << " consume inos: " << cpp_strerror(consume_r) << dendl;
        if (r == 0) {
          r = consume_r;
        }
      }
    }

    // Remove consumed dentries from lost+found.
    if (other_pool && !dry_run) {
      std::set<std::string> found;

      for (auto i : consumed_inos) {
	char s[20];

	snprintf(s, sizeof(s), "%llx_head", (unsigned long long) i);
	dout(20) << "removing " << s << dendl;
	found.insert(std::string(s));
      }

      object_t frag_oid;
      frag_oid = InodeStore::get_object_name(CEPH_INO_LOST_AND_FOUND,
					     frag_t(), "");
      output.omap_rm_keys(frag_oid.name, found);
    }
  } else if (command == "splice") {
    r = js.scan();
    if (r) {
      derr << "Failed to scan journal (" << cpp_strerror(r) << ")" << dendl;
      return r;
    }

    uint64_t start, end;
    if (filter.get_range(start, end)) {
      // Special case for range filter: erase a numeric range in the log
      uint64_t range = end - start;
      int r = erase_region(js, start, range);
      if (r) {
        derr << "Failed to erase region 0x" << std::hex << start << "~0x" << range << std::dec
             << ": " << cpp_strerror(r) << dendl;
        return r;
      }
    } else {
      // General case: erase a collection of individual entries in the log
      for (JournalScanner::EventMap::iterator i = js.events.begin(); i != js.events.end(); ++i) {
        dout(4) << "Erasing offset 0x" << std::hex << i->first << std::dec << dendl;

        int r = erase_region(js, i->first, i->second.raw_size);
        if (r) {
          derr << "Failed to erase event 0x" << std::hex << i->first << std::dec
               << ": " << cpp_strerror(r) << dendl;
          return r;
        }
      }
    }


  } else {
    cerr << "Unknown argument '" << command << "'" << std::endl;
    return -EINVAL;
  }

  // Generate output
  // ===============
  EventOutput output(js, dump_path);
  int output_result = 0;
  if (output_style == "binary") {
      output_result = output.binary();
  } else if (output_style == "json") {
      output_result = output.json();
  } else if (output_style == "summary") {
      output.summary();
  } else if (output_style == "list") {
      output.list();
  } else {
    std::cerr << "Bad output command '" << output_style << "'" << std::endl;
    return -EINVAL;
  }

  if (output_result != 0) {
    std::cerr << "Error writing output: " << cpp_strerror(output_result) << std::endl;
  }

  return output_result;
}

/**
 * Provide the user with information about the condition of the journal,
 * especially indicating what range of log events is available and where
 * any gaps or corruptions in the journal are.
 */
int JournalTool::journal_inspect()
{
  int r;

  JournalFilter filter(type);
  JournalScanner js(input, rank, type, filter);
  r = js.scan();
  if (r) {
    std::cerr << "Failed to scan journal (" << cpp_strerror(r) << ")" << std::endl;
    return r;
  }

  js.report(std::cout);

  return 0;
}


/**
 * Attempt to export a binary dump of the journal.
 *
 * This is allowed to fail if the header is malformed or there are
 * objects inaccessible, in which case the user would have to fall
 * back to manually listing RADOS objects and extracting them, which
 * they can do with the ``rados`` CLI.
 */
int JournalTool::journal_export(std::string const &path, bool import, bool force)
{
  int r = 0;
  JournalScanner js(input, rank, type);

  if (!import) {
    /*
     * If doing an export, first check that the header is valid and
     * no objects are missing before trying to dump
     */
    r = js.scan();
    if (r < 0) {
      derr << "Unable to scan journal, assuming badly damaged" << dendl;
      return r;
    }
    if (!js.is_readable()) {
      derr << "Journal not readable, attempt object-by-object dump with `rados`" << dendl;
      return -EIO;
    }
  }

  /*
   * Assuming we can cleanly read the journal data, dump it out to a file
   */
  {
    Dumper dumper;
    r = dumper.init(mds_role_t(role_selector.get_ns(), rank), type);
    if (r < 0) {
      derr << "dumper::init failed: " << cpp_strerror(r) << dendl;
      return r;
    }
    if (import) {
      r = dumper.undump(path.c_str(), force);
    } else {
      const std::string ex_path = gen_dump_file_path(path);
      r = dumper.dump(ex_path.c_str());
    }
  }

  return r;
}


/**
 * Truncate journal and insert EResetJournal
 */
int JournalTool::journal_reset(bool hard)
{
  int r = 0;
  Resetter resetter;
  r = resetter.init(mds_role_t(role_selector.get_ns(), rank), type, hard);
  if (r < 0) {
    derr << "resetter::init failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (hard) {
    r = resetter.reset_hard();
  } else {
    r = resetter.reset();
  }

  return r;
}


/**
 * Selective offline replay which only reads out dentries and writes
 * them to the backing store iff their version is > what is currently
 * in the backing store.
 *
 * In order to write dentries to the backing store, we may create the
 * required enclosing dirfrag objects.
 *
 * Test this by running scavenge on an unflushed journal, then nuking
 * it offline, then starting an MDS and seeing that the dentries are
 * visible.
 *
 * @param metablob an EMetaBlob retrieved from the journal
 * @param dry_run if true, do no writes to RADOS
 * @param consumed_inos output, populated with any inos inserted
 * @returns 0 on success, else negative error code
 */
int JournalTool::recover_dentries(
    EMetaBlob const &metablob,
    bool const dry_run,
    std::set<inodeno_t> *consumed_inos)
{
  ceph_assert(consumed_inos != NULL);

  int r = 0;

  // Replay fullbits (dentry+inode)
  for (const auto& frag : metablob.lump_order) {
    EMetaBlob::dirlump const &lump = metablob.lump_map.find(frag)->second;
    lump._decode_bits();
    object_t frag_oid = InodeStore::get_object_name(frag.ino, frag.frag, "");

    dout(4) << "inspecting lump " << frag_oid.name << dendl;


    // We will record old fnode version for use in hard link handling
    // If we don't read an old fnode, take version as zero and write in
    // all hardlinks we find.
    version_t old_fnode_version = 0;

    // Update fnode in omap header of dirfrag object
    bool write_fnode = false;
    bufferlist old_fnode_bl;
    r = input.omap_get_header(frag_oid.name, &old_fnode_bl);
    if (r == -ENOENT) {
      // Creating dirfrag from scratch
      dout(4) << "failed to read OMAP header from directory fragment "
        << frag_oid.name << " " << cpp_strerror(r) << dendl;
      write_fnode = true;
      // Note: creating the dirfrag *without* a backtrace, relying on
      // MDS to regenerate backtraces on read or in FSCK
    } else if (r == 0) {
      // Conditionally update existing omap header
      fnode_t old_fnode;
      auto old_fnode_iter = old_fnode_bl.cbegin();
      try {
        old_fnode.decode(old_fnode_iter);
        dout(4) << "frag " << frag_oid.name << " fnode old v" <<
          old_fnode.version << " vs new v" << lump.fnode.version << dendl;
        old_fnode_version = old_fnode.version;
        write_fnode = old_fnode_version < lump.fnode.version;
      } catch (const buffer::error &err) {
        dout(1) << "frag " << frag_oid.name
                << " is corrupt, overwriting" << dendl;
        write_fnode = true;
      }
    } else {
      // Unexpected error
      dout(4) << "failed to read OMAP header from directory fragment "
        << frag_oid.name << " " << cpp_strerror(r) << dendl;
      return r;
    }

    if ((other_pool || write_fnode) && !dry_run) {
      dout(4) << "writing fnode to omap header" << dendl;
      bufferlist fnode_bl;
      lump.fnode.encode(fnode_bl);
      if (!other_pool || frag.ino >= MDS_INO_SYSTEM_BASE) {
	r = output.omap_set_header(frag_oid.name, fnode_bl);
      }
      if (r != 0) {
        derr << "Failed to write fnode for frag object "
             << frag_oid.name << dendl;
        return r;
      }
    }

    std::set<std::string> read_keys;

    // Compose list of potentially-existing dentries we would like to fetch
    for (const auto& fb : lump.get_dfull()) {
      // Get a key like "foobar_head"
      std::string key;
      dentry_key_t dn_key(fb.dnlast, fb.dn.c_str());
      dn_key.encode(key);
      read_keys.insert(key);
    }

    for(const auto& rb : lump.get_dremote()) {
      // Get a key like "foobar_head"
      std::string key;
      dentry_key_t dn_key(rb.dnlast, rb.dn.c_str());
      dn_key.encode(key);
      read_keys.insert(key);
    }

    for (const auto& nb : lump.get_dnull()) {
      // Get a key like "foobar_head"
      std::string key;
      dentry_key_t dn_key(nb.dnlast, nb.dn.c_str());
      dn_key.encode(key);
      read_keys.insert(key);
    }

    // Perform bulk read of existing dentries
    std::map<std::string, bufferlist> read_vals;
    r = input.omap_get_vals_by_keys(frag_oid.name, read_keys, &read_vals);
    if (r == -ENOENT && other_pool) {
      r = output.omap_get_vals_by_keys(frag_oid.name, read_keys, &read_vals);
    }
    if (r != 0) {
      derr << "unexpected error reading fragment object "
           << frag_oid.name << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    // Compose list of dentries we will write back
    std::map<std::string, bufferlist> write_vals;
    for (const auto& fb : lump.get_dfull()) {
      // Get a key like "foobar_head"
      std::string key;
      dentry_key_t dn_key(fb.dnlast, fb.dn.c_str());
      dn_key.encode(key);

      dout(4) << "inspecting fullbit " << frag_oid.name << "/" << fb.dn
        << dendl;
      bool write_dentry = false;
      if (read_vals.find(key) == read_vals.end()) {
        dout(4) << "dentry did not already exist, will create" << dendl;
        write_dentry = true;
      } else {
        dout(4) << "dentry " << key << " existed already" << dendl;
        dout(4) << "dentry exists, checking versions..." << dendl;
        bufferlist &old_dentry = read_vals[key];
        // Decode dentry+inode
        auto q = old_dentry.cbegin();

        snapid_t dnfirst;
        decode(dnfirst, q);
        char dentry_type;
        decode(dentry_type, q);

        if (dentry_type == 'L') {
          // leave write_dentry false, we have no version to
          // compare with in a hardlink, so it's not safe to
          // squash over it with what's in this fullbit
          dout(10) << "Existing remote inode in slot to be (maybe) written "
               << "by a full inode from the journal dn '" << fb.dn.c_str()
               << "' with lump fnode version " << lump.fnode.version
               << "vs existing fnode version " << old_fnode_version << dendl;
          write_dentry = old_fnode_version < lump.fnode.version;
        } else if (dentry_type == 'I') {
          // Read out inode version to compare with backing store
          InodeStore inode;
          inode.decode_bare(q);
          dout(4) << "decoded embedded inode version "
            << inode.inode->version << " vs fullbit version "
            << fb.inode->version << dendl;
          if (inode.inode->version < fb.inode->version) {
            write_dentry = true;
          }
        } else {
          dout(4) << "corrupt dentry in backing store, overwriting from "
            "journal" << dendl;
          write_dentry = true;
        }
      }

      if ((other_pool || write_dentry) && !dry_run) {
        dout(4) << "writing I dentry " << key << " into frag "
          << frag_oid.name << dendl;

        // Compose: Dentry format is dnfirst, [I|L], InodeStore(bare=true)
        bufferlist dentry_bl;
        encode(fb.dnfirst, dentry_bl);
        encode('I', dentry_bl);
        encode_fullbit_as_inode(fb, true, &dentry_bl);

        // Record for writing to RADOS
        write_vals[key] = dentry_bl;
        consumed_inos->insert(fb.inode->ino);
      }
    }

    for(const auto& rb : lump.get_dremote()) {
      // Get a key like "foobar_head"
      std::string key;
      dentry_key_t dn_key(rb.dnlast, rb.dn.c_str());
      dn_key.encode(key);

      dout(4) << "inspecting remotebit " << frag_oid.name << "/" << rb.dn
        << dendl;
      bool write_dentry = false;
      if (read_vals.find(key) == read_vals.end()) {
        dout(4) << "dentry did not already exist, will create" << dendl;
        write_dentry = true;
      } else {
        dout(4) << "dentry " << key << " existed already" << dendl;
        dout(4) << "dentry exists, checking versions..." << dendl;
        bufferlist &old_dentry = read_vals[key];
        // Decode dentry+inode
        auto q = old_dentry.cbegin();

        snapid_t dnfirst;
        decode(dnfirst, q);
        char dentry_type;
        decode(dentry_type, q);

        if (dentry_type == 'L') {
          dout(10) << "Existing hardlink inode in slot to be (maybe) written "
               << "by a remote inode from the journal dn '" << rb.dn.c_str()
               << "' with lump fnode version " << lump.fnode.version
               << "vs existing fnode version " << old_fnode_version << dendl;
          write_dentry = old_fnode_version < lump.fnode.version;
        } else if (dentry_type == 'I') {
          dout(10) << "Existing full inode in slot to be (maybe) written "
               << "by a remote inode from the journal dn '" << rb.dn.c_str()
               << "' with lump fnode version " << lump.fnode.version
               << "vs existing fnode version " << old_fnode_version << dendl;
          write_dentry = old_fnode_version < lump.fnode.version;
        } else {
          dout(4) << "corrupt dentry in backing store, overwriting from "
            "journal" << dendl;
          write_dentry = true;
        }
      }

      if ((other_pool || write_dentry) && !dry_run) {
        dout(4) << "writing L dentry " << key << " into frag "
          << frag_oid.name << dendl;

        // Compose: Dentry format is dnfirst, [I|L], InodeStore(bare=true)
        bufferlist dentry_bl;
        encode(rb.dnfirst, dentry_bl);
        encode('L', dentry_bl);
        encode(rb.ino, dentry_bl);
        encode(rb.d_type, dentry_bl);

        // Record for writing to RADOS
        write_vals[key] = dentry_bl;
        consumed_inos->insert(rb.ino);
      }
    }

    std::set<std::string> null_vals;
    for (const auto& nb : lump.get_dnull()) {
      std::string key;
      dentry_key_t dn_key(nb.dnlast, nb.dn.c_str());
      dn_key.encode(key);

      dout(4) << "inspecting nullbit " << frag_oid.name << "/" << nb.dn
	<< dendl;

      auto it = read_vals.find(key);
      if (it != read_vals.end()) {
	dout(4) << "dentry exists, will remove" << dendl;

	auto q = it->second.cbegin();
	snapid_t dnfirst;
	decode(dnfirst, q);
	char dentry_type;
	decode(dentry_type, q);

	bool remove_dentry = false;
	if (dentry_type == 'L') {
	  dout(10) << "Existing hardlink inode in slot to be (maybe) removed "
	    << "by null journal dn '" << nb.dn.c_str()
	    << "' with lump fnode version " << lump.fnode.version
	    << "vs existing fnode version " << old_fnode_version << dendl;
	  remove_dentry = old_fnode_version < lump.fnode.version;
	} else if (dentry_type == 'I') {
	  dout(10) << "Existing full inode in slot to be (maybe) removed "
	    << "by null journal dn '" << nb.dn.c_str()
	    << "' with lump fnode version " << lump.fnode.version
	    << "vs existing fnode version " << old_fnode_version << dendl;
	  remove_dentry = old_fnode_version < lump.fnode.version;
	} else {
	  dout(4) << "corrupt dentry in backing store, will remove" << dendl;
	  remove_dentry = true;
	}

	if (remove_dentry)
	  null_vals.insert(key);
      }
    }

    // Write back any new/changed dentries
    if (!write_vals.empty()) {
      r = output.omap_set(frag_oid.name, write_vals);
      if (r != 0) {
	derr << "error writing dentries to " << frag_oid.name
	     << ": " << cpp_strerror(r) << dendl;
	return r;
      }
    }

    // remove any null dentries
    if (!null_vals.empty()) {
      r = output.omap_rm_keys(frag_oid.name, null_vals);
      if (r != 0) {
	derr << "error removing dentries from " << frag_oid.name
	  << ": " << cpp_strerror(r) << dendl;
	return r;
      }
    }
  }

  /* Now that we've looked at the dirlumps, we finally pay attention to
   * the roots (i.e. inodes without ancestry).  This is necessary in order
   * to pick up dirstat updates on ROOT_INO.  dirstat updates are functionally
   * important because clients use them to infer completeness
   * of directories
   */
  for (const auto& fb : metablob.roots) {
    inodeno_t ino = fb.inode->ino;
    dout(4) << "updating root 0x" << std::hex << ino << std::dec << dendl;

    object_t root_oid = InodeStore::get_object_name(ino, frag_t(), ".inode");
    dout(4) << "object id " << root_oid.name << dendl;

    bool write_root_ino = false;
    bufferlist old_root_ino_bl;
    r = input.read(root_oid.name, old_root_ino_bl, (1<<22), 0);
    if (r == -ENOENT) {
      dout(4) << "root does not exist, will create" << dendl;
      write_root_ino = true;
    } else if (r >= 0) {
      r = 0;
      InodeStore old_inode;
      dout(4) << "root exists, will modify (" << old_root_ino_bl.length()
        << ")" << dendl;
      auto inode_bl_iter = old_root_ino_bl.cbegin(); 
      std::string magic;
      decode(magic, inode_bl_iter);
      if (magic == CEPH_FS_ONDISK_MAGIC) {
        dout(4) << "magic ok" << dendl;
        old_inode.decode(inode_bl_iter);

        if (old_inode.inode->version < fb.inode->version) {
          write_root_ino = true;
        }
      } else {
        dout(4) << "magic bad: '" << magic << "'" << dendl;
        write_root_ino = true;
      }
    } else {
      derr << "error reading root inode object " << root_oid.name
            << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    if (write_root_ino && !dry_run) {
      dout(4) << "writing root ino " << root_oid.name
               << " version " << fb.inode->version << dendl;

      // Compose: root ino format is magic,InodeStore(bare=false)
      bufferlist new_root_ino_bl;
      encode(std::string(CEPH_FS_ONDISK_MAGIC), new_root_ino_bl);
      encode_fullbit_as_inode(fb, false, &new_root_ino_bl);

      // Write to RADOS
      r = output.write_full(root_oid.name, new_root_ino_bl);
      if (r != 0) {
        derr << "error writing inode object " << root_oid.name
              << ": " << cpp_strerror(r) << dendl;
        return r;
      }
    }
  }

  return r;
}


/**
 * Erase a region of the log by overwriting it with ENoOp
 *
 */
int JournalTool::erase_region(JournalScanner const &js, uint64_t const pos, uint64_t const length)
{
  // To erase this region, we use our preamble, the encoding overhead
  // of an ENoOp, and our trailing start ptr.  Calculate how much padding
  // is needed inside the ENoOp to make up the difference.
  bufferlist tmp;
  if (type == "mdlog") {
    ENoOp enoop(0);
    enoop.encode_with_header(tmp, CEPH_FEATURES_SUPPORTED_DEFAULT);
  } else if (type == "purge_queue") {
    PurgeItem pi;
    pi.encode(tmp);
  }

  dout(4) << "erase_region " << pos << " len=" << length << dendl;

  // FIXME: get the preamble/postamble length via JournalStream
  int32_t padding = length - tmp.length() - sizeof(uint32_t) - sizeof(uint64_t) - sizeof(uint64_t);
  dout(4) << "erase_region padding=0x" << std::hex << padding << std::dec << dendl;

  if (padding < 0) {
    derr << "Erase region " << length << " too short" << dendl;
    return -EINVAL;
  }

  bufferlist entry;
  if (type == "mdlog") {
    // Serialize an ENoOp with the correct amount of padding
    ENoOp enoop(padding);
    enoop.encode_with_header(entry, CEPH_FEATURES_SUPPORTED_DEFAULT);
  } else if (type == "purge_queue") {
    PurgeItem pi;
    pi.pad_size = padding;
    pi.encode(entry);
  }
  JournalStream stream(JOURNAL_FORMAT_RESILIENT);
  // Serialize region of log stream
  bufferlist log_data;
  stream.write(entry, &log_data, pos);

  dout(4) << "erase_region data length " << log_data.length() << dendl;
  ceph_assert(log_data.length() == length);

  // Write log stream region to RADOS
  // FIXME: get object size somewhere common to scan_events
  uint32_t object_size = g_conf()->mds_log_segment_size;
  if (object_size == 0) {
    // Default layout object size
    object_size = file_layout_t::get_default().object_size;
  }

  uint64_t write_offset = pos;
  uint64_t obj_offset = (pos / object_size);
  int r = 0;
  while(log_data.length()) {
    std::string const oid = js.obj_name(obj_offset);
    uint32_t offset_in_obj = write_offset % object_size;
    uint32_t write_len = min(log_data.length(), object_size - offset_in_obj);

    r = output.write(oid, log_data, write_len, offset_in_obj);
    if (r < 0) {
      return r;
    } else {
      dout(4) << "Wrote " << write_len << " bytes to " << oid << dendl;
      r = 0;
    }
     
    log_data.splice(0, write_len);
    write_offset += write_len;
    obj_offset++;
  }

  return r;
}

/**
 * Given an EMetaBlob::fullbit containing an inode, write out
 * the encoded inode in the format used by InodeStore (i.e. the
 * backing store format)
 *
 * This is a distant cousin of EMetaBlob::fullbit::update_inode, but for use
 * on an offline InodeStore instance.  It's way simpler, because we are just
 * uncritically hauling the data between structs.
 *
 * @param fb a fullbit extracted from a journal entry
 * @param bare if true, leave out [EN|DE]CODE_START decoration
 * @param out_bl output, write serialized inode to this bufferlist
 */
void JournalTool::encode_fullbit_as_inode(
  const EMetaBlob::fullbit &fb,
  const bool bare,
  bufferlist *out_bl)
{
  ceph_assert(out_bl != NULL);

  // Compose InodeStore
  InodeStore new_inode;
  new_inode.inode = fb.inode;
  new_inode.xattrs = fb.xattrs;
  new_inode.dirfragtree = fb.dirfragtree;
  new_inode.snap_blob = fb.snapbl;
  new_inode.symlink = fb.symlink;
  new_inode.old_inodes = fb.old_inodes;

  // Serialize InodeStore
  if (bare) {
    new_inode.encode_bare(*out_bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
  } else {
    new_inode.encode(*out_bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
  }
}

/**
 * Given a list of inode numbers known to be in use by
 * inodes in the backing store, ensure that none of these
 * numbers are listed as free in the InoTables in the
 * backing store.
 *
 * Used after injecting inodes into the backing store, to
 * ensure that the same inode numbers are not subsequently
 * used for new files during ordinary operation.
 *
 * @param inos list of inode numbers to be removed from
 *             free lists in InoTables
 * @returns 0 on success, else negative error code
 */
int JournalTool::consume_inos(const std::set<inodeno_t> &inos)
{
  int r = 0;

  // InoTable is a per-MDS structure, so iterate over assigned ranks
  auto fs = fsmap->get_filesystem(role_selector.get_ns());
  std::set<mds_rank_t> in_ranks;
  fs->mds_map.get_mds_set(in_ranks);

  for (std::set<mds_rank_t>::iterator rank_i = in_ranks.begin();
      rank_i != in_ranks.end(); ++rank_i)
  {
    // Compose object name
    std::ostringstream oss;
    oss << "mds" << *rank_i << "_inotable";
    object_t inotable_oid = object_t(oss.str());

    // Read object
    bufferlist inotable_bl;
    int read_r = input.read(inotable_oid.name, inotable_bl, (1<<22), 0);
    if (read_r < 0) {
      // Things are really bad if we can't read inotable.  Beyond our powers.
      derr << "unable to read inotable '" << inotable_oid.name << "': "
        << cpp_strerror(read_r) << dendl;
      r = r ? r : read_r;
      continue;
    }

    // Deserialize InoTable
    version_t inotable_ver;
    auto q = inotable_bl.cbegin();
    decode(inotable_ver, q);
    InoTable ino_table(NULL);
    ino_table.decode(q);
    
    // Update InoTable in memory
    bool inotable_modified = false;
    for (std::set<inodeno_t>::iterator i = inos.begin();
        i != inos.end(); ++i)
    {
      const inodeno_t ino = *i;
      if (ino_table.force_consume(ino)) {
        dout(4) << "Used ino 0x" << std::hex << ino << std::dec
          << " requires inotable update" << dendl;
        inotable_modified = true;
      }
    }

    // Serialize and write InoTable
    if (inotable_modified) {
      inotable_ver += 1;
      dout(4) << "writing modified inotable version " << inotable_ver << dendl;
      bufferlist inotable_new_bl;
      encode(inotable_ver, inotable_new_bl);
      ino_table.encode_state(inotable_new_bl);
      int write_r = output.write_full(inotable_oid.name, inotable_new_bl);
      if (write_r != 0) {
        derr << "error writing modified inotable " << inotable_oid.name
          << ": " << cpp_strerror(write_r) << dendl;
        r = r ? r : read_r;
        continue;
      }
    }
  }

  return r;
}

