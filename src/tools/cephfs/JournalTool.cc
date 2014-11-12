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

#include "mds/events/ENoOp.h"
#include "mds/events/EUpdate.h"

#include "JournalScanner.h"
#include "EventOutput.h"
#include "Dumper.h"
#include "Resetter.h"

#include "JournalTool.h"


#define dout_subsys ceph_subsys_mds



void JournalTool::usage()
{
  std::cout << "Usage: \n"
    << "  cephfs-journal-tool [options] journal [inspect|import|export|reset]\n"
    << "  cephfs-journal-tool [options] header <get|set <field> <value>\n"
    << "  cephfs-journal-tool [options] event <effect> <selector> <output>\n"
    << "    <selector>:\n"
    << "      --range=<start>..<end>\n"
    << "      --path=<substring>\n"
    << "      --inode=<integer>\n"
    << "      --type=<UPDATE|OPEN|SESSION...><\n"
    << "      --frag=<ino>.<frag> [--dname=<dentry string>]\n"
    << "      --client=<session id integer>\n"
    << "    <effect>: [get|apply|splice]\n"
    << "    <output>: [summary|binary|json] [--path <path>]\n"
    << "\n"
    << "Options:\n"
    << "  --rank=<int>  Journal rank (default 0)\n";

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
    usage();
    return -EINVAL;
  }

  std::vector<const char*>::iterator arg = argv.begin();
  std::string rank_str;
  if(ceph_argparse_witharg(argv, arg, &rank_str, "--rank", (char*)NULL)) {
    std::string rank_err;
    rank = strict_strtol(rank_str.c_str(), 10, &rank_err);
    if (!rank_err.empty()) {
        derr << "Bad rank '" << rank_str << "'" << dendl;
        usage();
    }
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
  rados.connect();
 
  int const pool_id = mdsmap->get_metadata_pool();
  dout(4) << "JournalTool: resolving pool " << pool_id << dendl;
  std::string pool_name;
  r = rados.pool_reverse_lookup(pool_id, &pool_name);
  if (r < 0) {
    derr << "Pool " << pool_id << " named in MDS map not found in RADOS!" << dendl;
    return r;
  }

  dout(4) << "JournalTool: creating IoCtx.." << dendl;
  r = rados.ioctx_create(pool_name.c_str(), io);
  assert(r == 0);

  // Execution
  // =========
  dout(4) << "Executing for rank " << rank << dendl;
  if (mode == std::string("journal")) {
    return main_journal(argv);
  } else if (mode == std::string("header")) {
    return main_header(argv);
  } else if (mode == std::string("event")) {
    return main_event(argv);
  } else {
    derr << "Bad command '" << mode << "'" << dendl;
    usage();
    return -EINVAL;
  }
}


/**
 * Handle arguments for 'journal' mode
 *
 * This is for operations that act on the journal as a whole.
 */
int JournalTool::main_journal(std::vector<const char*> &argv)
{
  std::string command = argv[0];
  if (command == "inspect") {
    return journal_inspect();
  } else if (command == "export" || command == "import") {
    if (argv.size() >= 2) {
      std::string const path = argv[1];
      return journal_export(path, command == "import");
    } else {
      derr << "Missing path" << dendl;
      return -EINVAL;
    }
  } else if (command == "reset") {
      return journal_reset();
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
  JournalFilter filter;
  JournalScanner js(io, rank, filter);
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
    assert(js.header != NULL);
  }

  if (argv.size() == 0) {
    derr << "Invalid header command, must be [get|set]" << dendl;
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
    assert(argv.empty());

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
    } else {
      derr << "Invalid field '" << field_name << "'" << dendl;
      return -EINVAL;
    }

    std::cout << "Updating " << field_name << std::hex << " 0x" << *field << " -> 0x" << new_val << std::dec << std::endl;
    *field = new_val;

    dout(4) << "Writing object..." << dendl;
    bufferlist header_bl;
    ::encode(*(js.header), header_bl);
    io.write_full(js.obj_name(0), header_bl);
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

  std::vector<const char*>::iterator arg = argv.begin();

  std::string command = *(arg++);
  if (command != "get" && command != "apply" && command != "splice") {
    derr << "Unknown argument '" << command << "'" << dendl;
    usage();
    return -EINVAL;
  }

  if (arg == argv.end()) {
    derr << "Incomplete command line" << dendl;
    usage();
    return -EINVAL;
  }

  // Parse filter options
  // ====================
  JournalFilter filter;
  r = filter.parse_args(argv, arg);
  if (r) {
    return r;
  }

  // Parse output options
  // ====================
  if (arg == argv.end()) {
    derr << "Missing output command" << dendl;
    usage();
  }
  std::string output_style = *(arg++);
  if (output_style != "binary" && output_style != "json" &&
      output_style != "summary" && output_style != "list") {
      derr << "Unknown argument: '" << output_style << "'" << dendl;
      usage();
      return -EINVAL;
  }

  std::string output_path = "dump";
  while(arg != argv.end()) {
    std::string arg_str;
    if (ceph_argparse_witharg(argv, arg, &arg_str, "--path", (char*)NULL)) {
      output_path = arg_str;
    } else {
      derr << "Unknown argument: '" << *arg << "'" << dendl;
      usage();
      return -EINVAL;
    }
  }

  // Execute command
  // ===============
  JournalScanner js(io, rank, filter);
  if (command == "get") {
    r = js.scan();
    if (r) {
      derr << "Failed to scan journal (" << cpp_strerror(r) << ")" << dendl;
      return r;
    }
  } else if (command == "apply") {
    r = js.scan();
    if (r) {
      derr << "Failed to scan journal (" << cpp_strerror(r) << ")" << dendl;
      return r;
    }

    bool dry_run = false;
    if (arg != argv.end() && ceph_argparse_flag(argv, arg, "--dry_run", (char*)NULL)) {
      dry_run = true;
    }

    for (JournalScanner::EventMap::iterator i = js.events.begin(); i != js.events.end(); ++i) {
      LogEvent *le = i->second.log_event;
      EMetaBlob const *mb = le->get_metablob();
      if (mb) {
        replay_offline(*mb, dry_run);
      }
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
    derr << "Unknown argument '" << command << "'" << dendl;
    usage();
    return -EINVAL;
  }

  // Generate output
  // ===============
  EventOutput output(js, output_path);
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

  JournalFilter filter;
  JournalScanner js(io, rank, filter);
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
int JournalTool::journal_export(std::string const &path, bool import)
{
  int r = 0;
  JournalScanner js(io, rank);

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
    r = dumper.init(rank);
    if (r < 0) {
      derr << "dumper::init failed: " << cpp_strerror(r) << dendl;
      return r;
    }
    if (import) {
      r = dumper.undump(path.c_str());
    } else {
      r = dumper.dump(path.c_str());
    }
    dumper.shutdown();
  }

  return r;
}


/**
 * Truncate journal and insert EResetJournal
 */
int JournalTool::journal_reset()
{
  int r = 0;
  Resetter resetter;
  r = resetter.init();
  if (r < 0) {
    derr << "resetter::init failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  resetter.reset(rank);
  resetter.shutdown();

  return r;
}


int JournalTool::replay_offline(EMetaBlob const &metablob, bool const dry_run)
{
  int r;

  // Replay roots
  for (list<ceph::shared_ptr<EMetaBlob::fullbit> >::const_iterator p = metablob.roots.begin(); p != metablob.roots.end(); ++p) {
    EMetaBlob::fullbit const &fb = *(*p);
    inodeno_t ino = fb.inode.ino;
    dout(4) << __func__ << ": updating root 0x" << std::hex << ino << std::dec << dendl;

    object_t root_oid = InodeStore::get_object_name(ino, frag_t(), ".inode");
    dout(4) << __func__ << ": object id " << root_oid.name << dendl;

    bufferlist inode_bl;
    r = io.read(root_oid.name, inode_bl, (1<<22), 0);
    InodeStore inode;
    if (r == -ENOENT) {
      dout(4) << __func__ << ": root does not exist, will create" << dendl;
    } else {
      dout(4) << __func__ << ": root exists, will modify (" << inode_bl.length() << ")" << dendl;
      // TODO: add some kind of force option so that we can overwrite bad inodes
      // from the journal if needed
      bufferlist::iterator inode_bl_iter = inode_bl.begin(); 
      std::string magic;
      ::decode(magic, inode_bl_iter);
      if (magic == CEPH_FS_ONDISK_MAGIC) {
        dout(4) << "magic ok" << dendl;
      } else {
        dout(4) << "magic bad: '" << magic << "'" << dendl;
      }
      inode.decode(inode_bl_iter);
    }

    // This is a distant cousin of EMetaBlob::fullbit::update_inode, but for use
    // on an offline InodeStore instance.  It's way simpler, because we are just
    // uncritically hauling the data between structs.
    inode.inode = fb.inode;
    inode.xattrs = fb.xattrs;
    inode.dirfragtree = fb.dirfragtree;
    inode.snap_blob = fb.snapbl;
    inode.symlink = fb.symlink;
    inode.old_inodes = fb.old_inodes;

    inode_bl.clear();
    std::string magic = CEPH_FS_ONDISK_MAGIC;
    ::encode(magic, inode_bl);
    inode.encode(inode_bl);

    if (!dry_run) {
      r = io.write_full(root_oid.name, inode_bl);
      assert(r == 0);
    }
  }

  // TODO: respect metablob.renamed_dirino (cues us as to which dirlumps
  // indicate renamed directories)

  // Replay fullbits (dentry+inode)
  for (list<dirfrag_t>::const_iterator lp = metablob.lump_order.begin(); lp != metablob.lump_order.end(); ++lp) {
    dirfrag_t const &frag = *lp;
    EMetaBlob::dirlump const &lump = metablob.lump_map.find(frag)->second;
    lump._decode_bits();
    object_t frag_object_id = InodeStore::get_object_name(frag.ino, frag.frag, "");

    // Check for presence of dirfrag object
    uint64_t psize;
    time_t pmtime;
    r = io.stat(frag_object_id.name, &psize, &pmtime);
    if (r == -ENOENT) {
      dout(4) << "Frag object " << frag_object_id.name << " did not exist, will create" << dendl;
    } else if (r != 0) {
      // FIXME: what else can happen here?  can I deal?
      assert(r == 0);
    } else {
      dout(4) << "Frag object " << frag_object_id.name << " exists, will modify" << dendl;
    }

    // Write fnode to omap header of dirfrag object
    bufferlist fnode_bl;
    lump.fnode.encode(fnode_bl);
    if (!dry_run) {
      r = io.omap_set_header(frag_object_id.name, fnode_bl);
      if (r != 0) {
        derr << "Failed to write fnode for frag object " << frag_object_id.name << dendl;
        return r;
      }
    }

    // Try to get the existing dentry
    list<ceph::shared_ptr<EMetaBlob::fullbit> > const &fb_list = lump.get_dfull();
    for (list<ceph::shared_ptr<EMetaBlob::fullbit> >::const_iterator fbi = fb_list.begin(); fbi != fb_list.end(); ++fbi) {
      EMetaBlob::fullbit const &fb = *(*fbi);

      // Get a key like "foobar_head"
      std::string key;
      dentry_key_t dn_key(fb.dnlast, fb.dn.c_str());
      dn_key.encode(key);

      // See if the dentry is present
      std::set<std::string> keys;
      keys.insert(key);
      std::map<std::string, bufferlist> vals;
      r = io.omap_get_vals_by_keys(frag_object_id.name, keys, &vals);
      assert (r == 0);  // I assume success because I checked object existed and absence of 
                        // dentry gives me empty map instead of failure
                        // FIXME handle failures so we can replay other events
                        // if this one is causing some unexpected issue
    
      if (vals.find(key) == vals.end()) {
        dout(4) << "dentry " << key << " does not exist, will be created" << dendl;
      } else {
        dout(4) << "dentry " << key << " existed already" << dendl;
        // TODO: read out existing dentry before adding new one so that
        // we can print a bit of info about what we're overwriting
      }
    
      bufferlist dentry_bl;
      ::encode(fb.dnfirst, dentry_bl);
      ::encode('I', dentry_bl);

      InodeStore inode;
      inode.inode = fb.inode;
      inode.xattrs = fb.xattrs;
      inode.dirfragtree = fb.dirfragtree;
      inode.snap_blob = fb.snapbl;
      inode.symlink = fb.symlink;
      inode.old_inodes = fb.old_inodes;
      inode.encode_bare(dentry_bl);
      
      vals[key] = dentry_bl;
      if (!dry_run) {
        r = io.omap_set(frag_object_id.name, vals);
        assert(r == 0);  // FIXME handle failures
      }
    }

    list<EMetaBlob::nullbit> const &nb_list = lump.get_dnull();
    for (list<EMetaBlob::nullbit>::const_iterator
	iter = nb_list.begin(); iter != nb_list.end(); ++iter) {
      EMetaBlob::nullbit const &nb = *iter;

      // Get a key like "foobar_head"
      std::string key;
      dentry_key_t dn_key(nb.dnlast, nb.dn.c_str());
      dn_key.encode(key);

      // Remove it from the dirfrag
      dout(4) << "Removing dentry " << key << dendl;
      std::set<std::string> keys;
      keys.insert(key);
      if (!dry_run) {
        r = io.omap_rm_keys(frag_object_id.name, keys);
        assert(r == 0);
      }
    }
  }

  for (std::vector<inodeno_t>::const_iterator i = metablob.destroyed_inodes.begin();
       i != metablob.destroyed_inodes.end(); ++i) {
    dout(4) << "Destroyed inode: " << *i << dendl;
    // TODO: if it was a dir, then delete its dirfrag objects
  }

  return 0;
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
  ENoOp enoop(0);
  enoop.encode_with_header(tmp);

  dout(4) << "erase_region " << pos << " len=" << length << dendl;

  // FIXME: get the preamble/postamble length via JournalStream
  int32_t padding = length - tmp.length() - sizeof(uint32_t) - sizeof(uint64_t) - sizeof(uint64_t);
  dout(4) << "erase_region padding=0x" << std::hex << padding << std::dec << dendl;

  if (padding < 0) {
    derr << "Erase region " << length << " too short" << dendl;
    return -EINVAL;
  }

  // Serialize an ENoOp with the correct amount of padding
  enoop = ENoOp(padding);
  bufferlist entry;
  enoop.encode_with_header(entry);
  JournalStream stream(JOURNAL_FORMAT_RESILIENT);

  // Serialize region of log stream
  bufferlist log_data;
  stream.write(entry, &log_data, pos);

  dout(4) << "erase_region data length " << log_data.length() << dendl;
  assert(log_data.length() == length);

  // Write log stream region to RADOS
  // FIXME: get object size somewhere common to scan_events
  uint32_t object_size = g_conf->mds_log_segment_size;
  if (object_size == 0) {
    // Default layout object size
    object_size = g_default_file_layout.fl_object_size;
  }

  uint64_t write_offset = pos;
  uint64_t obj_offset = (pos / object_size);
  int r = 0;
  while(log_data.length()) {
    std::string const oid = js.obj_name(obj_offset);
    uint32_t offset_in_obj = write_offset % object_size;
    uint32_t write_len = min(log_data.length(), object_size - offset_in_obj);

    r = io.write(oid, log_data, write_len, offset_in_obj);
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

