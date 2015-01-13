// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <stdlib.h>
#include <string>

#include "common/Formatter.h"
#include "common/errno.h"

#include "global/global_init.h"
#include "include/stringify.h"
#include "mon/MonitorDBStore.h"
#include "mon/Paxos.h"

namespace po = boost::program_options;
using namespace std;

class TraceIter {
  int fd;
  unsigned idx;
  MonitorDBStore::TransactionRef t;
public:
  TraceIter(string fname) : fd(-1), idx(-1) {
    fd = ::open(fname.c_str(), O_RDONLY);
    t.reset(new MonitorDBStore::Transaction);
  }
  bool valid() {
    return fd != -1;
  }
  MonitorDBStore::TransactionRef cur() {
    assert(valid());
    return t;
  }
  unsigned num() { return idx; }
  void next() {
    ++idx;
    bufferlist bl;
    int r = bl.read_fd(fd, 6);
    if (r < 0) {
      std::cerr << "Got error: " << cpp_strerror(r) << " on read_fd"
		<< std::endl;
      ::close(fd);
      fd = -1;
      return;
    } else if ((unsigned)r < 6) {
      std::cerr << "short read" << std::endl;
      ::close(fd);
      fd = -1;
      return;
    }
    bufferlist::iterator bliter = bl.begin();
    uint8_t ver, ver2;
    ::decode(ver, bliter);
    ::decode(ver2, bliter);
    uint32_t len;
    ::decode(len, bliter);
    r = bl.read_fd(fd, len);
    if (r < 0) {
      std::cerr << "Got error: " << cpp_strerror(r) << " on read_fd"
		<< std::endl;
      ::close(fd);
      fd = -1;
      return;
    } else if ((unsigned)r < len) {
      std::cerr << "short read" << std::endl;
      ::close(fd);
      fd = -1;
      return;
    }
    bliter = bl.begin();
    t.reset(new MonitorDBStore::Transaction);
    t->decode(bliter);
  }
  void init() {
    next();
  }
  ~TraceIter() {
    if (fd != -1) {
      ::close(fd);
      fd = -1;
    }
  }
};


int parse_cmd_args(
    po::options_description *desc, /// < visible options description
    po::options_description *hidden_desc, /// < hidden options description
    po::positional_options_description *positional, /// < positional args
    vector<string> &cmd_args, /// < arguments to be parsed
    po::variables_map *vm /// > post-parsing variable map
    )
{
  // desc_all will aggregate all visible and hidden options for parsing.
  //
  // From boost's program_options point of view, there is absolutely no
  // distinction between 'desc' and 'hidden_desc'.  This is a distinction
  // that is only useful to us:  'desc' is whatever we are willing to show
  // on 'usage()', whereas 'hidden_desc' refers to parameters we wish to
  // take advantage of but do not wish to show on 'usage()'.
  //
  // For example, consider that program_options matches positional arguments
  // (specified via 'positional') against the paramenters defined on a
  // given 'po::options_description' class.  This is performed below,
  // supplying both the description and the positional arguments to the
  // parser.  However, we do not want the parameters that are mapped to
  // positional arguments to be shown on usage, as that makes for ugly and
  // confusing usage messages.  Therefore we dissociate the options'
  // description that is to be used as an aid to the user from those options
  // that are nothing but useful for internal purposes (i.e., mapping options
  // to positional arguments).  We still need to aggregate them before parsing
  // and that's what 'desc_all' is all about.
  //

  assert(desc != NULL);

  po::options_description desc_all;
  desc_all.add(*desc);
  if (hidden_desc != NULL)
    desc_all.add(*hidden_desc);

  try {
    po::command_line_parser parser = po::command_line_parser(cmd_args).
      options(desc_all);

    if (positional) {
      parser = parser.positional(*positional);
    }

    po::parsed_options parsed = parser.run();
    po::store(parsed, *vm);
    po::notify(*vm);
  } catch (po::error &e) {
    std::cerr << "error: " << e.what() << std::endl;
    return -EINVAL;
  }
  return 0;
}


/**
 * usage: ceph-monstore-tool <store-path> <command> [options]
 *
 * commands:
 *
 *  store-copy < --out arg >
 *  dump-keys
 *  compact
 *  getmonmap < --out arg [ --version arg ] >
 *  getosdmap < --out arg [ --version arg ] >
 *  dump-paxos <--dump-start VER> <--dump-end VER>
 *  dump-trace < --trace-file arg >
 *  replay-trace
 *  random-gen
 *
 * wanted syntax:
 *
 * ceph-monstore-tool PATH CMD [options]
 *
 * ceph-monstore-tool PATH store-copy <PATH2 | -o PATH2>
 * ceph-monstore-tool PATH dump-keys
 * ceph-monstore-tool PATH compact
 * ceph-monstore-tool PATH get monmap [VER]
 * ceph-monstore-tool PATH get osdmap [VER]
 * ceph-monstore-tool PATH dump-paxos STARTVER ENDVER
 *
 *
 */
void usage(const char *n, po::options_description &d)
{
  std::cerr <<
     "usage: " << n << " <store-path> <cmd> [args|options]\n"
  << "\n"
  << "Commands:\n"
  << "  store-copy PATH                 copies store to PATH\n"
  << "  compact                         compacts the store\n"
  << "  get monmap [-- options]         get monmap (version VER if specified)\n"
  << "                                  (default: last committed)\n"
  << "  get osdmap [-- options]         get osdmap (version VER if specified)\n"
  << "                                  (default: last committed)\n"
  << "  get mdsmap [-- options]         get mdsmap (version VER if specified)\n"
  << "                                  (default: last committed)\n"
  << "  dump-keys                       dumps store keys to FILE\n"
  << "                                  (default: stdout)\n"
  << "  dump-paxos [-- options]         dump paxos transactions\n"
  << "                                  (dump-paxos -- --help for more info)\n"
  << "  dump-trace FILE [-- options]    dump contents of trace file FILE\n"
  << "                                  (dump-trace -- --help for more info)\n"
  << "  replay-trace FILE [-- options]  replay trace from FILE\n"
  << "                                  (replay-trace -- --help for more info)\n"
  << "  random-gen [-- options]         add randomly generated ops to the store\n"
  << "                                  (random-gen -- --help for more info)\n"
  << std::endl;
  std::cerr << d << std::endl;
  std::cerr
    << "\nPlease Note:\n"
    << "* Ceph-specific options should be in the format --option-name=VAL\n"
    << "  (specifically, do not forget the '='!!)\n"
    << "* Command-specific options need to be passed after a '--'\n"
    << "  e.g., 'get monmap -- --version 10 --out /tmp/foo'"
    << std::endl;
}

int main(int argc, char **argv) {
  int err = 0;
  po::options_description desc("Allowed options");
  string store_path, cmd;
  vector<string> subcmds;
  desc.add_options()
    ("help,h", "produce help message")
    ;

  /* Dear Future Developer:
   *
   * for further improvement, should you need to pass specific options to
   * a command (e.g., get osdmap VER --hex), you can expand the current
   * format by creating additional 'po::option_description' and passing
   * 'subcmds' to 'po::command_line_parser', much like what is currently
   * done by default.  However, beware: in order to differentiate a
   * command-specific option from the generic/global options, you will need
   * to pass '--' in the command line (so that the first parser, the one
   * below, assumes it has reached the end of all options); e.g.,
   * 'get osdmap VER -- --hex'.  Not pretty; far from intuitive; it was as
   * far as I got with this library.  Improvements on this format will be
   * left as an excercise for the reader. -Joao
   */
  po::options_description positional_desc("Positional argument options");
  positional_desc.add_options()
    ("store-path", po::value<string>(&store_path),
     "path to monitor's store")
    ("command", po::value<string>(&cmd),
     "Command")
    ("subcmd", po::value<vector<string> >(&subcmds),
     "Command arguments/Sub-Commands")
    ;
  po::positional_options_description positional;
  positional.add("store-path", 1);
  positional.add("command", 1);
  positional.add("subcmd", -1);

  po::options_description all_desc("All options");
  all_desc.add(desc).add(positional_desc);

  vector<string> ceph_option_strings;
  po::variables_map vm;
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).
        options(all_desc).
        positional(positional).
        allow_unregistered().run();

    po::store(
	      parsed,
	      vm);
    po::notify(vm);

    // Specifying po::include_positional would have our positional arguments
    // being collected (thus being part of ceph_option_strings and eventually
    // passed on to global_init() below).
    // Instead we specify po::exclude_positional, which has the upside of
    // completely avoid this, but the downside of having to specify ceph
    // options as --VAR=VAL (note the '='); otherwise we will capture the
    // positional 'VAL' as belonging to us, never being collected.
    ceph_option_strings = po::collect_unrecognized(parsed.options,
						   po::exclude_positional);

  } catch(po::error &e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  // parse command structure before calling global_init() and friends.

  if (vm.empty() || vm.count("help") ||
      store_path.empty() || cmd.empty() ||
      *cmd.begin() == '-') {
    usage(argv[0], desc);
    return 1;
  }

  vector<const char *> ceph_options, def_args;
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_MON,
    CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);
  g_conf = g_ceph_context->_conf;

  // this is where we'll write *whatever*, on a per-command basis.
  // not all commands require some place to write their things.
  MonitorDBStore st(store_path);
  if (store_path.size()) {
    stringstream ss;
    int r = st.open(ss);
    if (r < 0) {
      std::cerr << ss.str() << std::endl;
      return EINVAL;
    }
  }

  if (cmd == "dump-keys") {
    KeyValueDB::WholeSpaceIterator iter = st.get_iterator();
    while (iter->valid()) {
      pair<string,string> key(iter->raw_key());
      cout << key.first << " / " << key.second << std::endl;
      iter->next();
    }
  } else if (cmd == "compact") {
    st.compact();
  } else if (cmd == "get") {
    unsigned v = 0;
    string outpath;
    string map_type;
    // visible options for this command
    po::options_description op_desc("Allowed 'get' options");
    op_desc.add_options()
      ("help,h", "produce this help message")
      ("out,o", po::value<string>(&outpath),
       "output file (default: stdout)")
      ("version,v", po::value<unsigned>(&v),
       "map version to obtain")
      ;
    // this is going to be a positional argument; we don't want to show
    // it as an option during --help, but we do want to have it captured
    // when parsing.
    po::options_description hidden_op_desc("Hidden 'get' options");
    hidden_op_desc.add_options()
      ("map-type", po::value<string>(&map_type),
       "map-type")
      ;
    po::positional_options_description op_positional;
    op_positional.add("map-type", 1);

    po::variables_map op_vm;
    int r = parse_cmd_args(&op_desc, &hidden_op_desc, &op_positional,
                           subcmds, &op_vm);
    if (r < 0) {
      err = -r;
      goto done;
    }

    if (op_vm.count("help") || map_type.empty()) {
      usage(argv[0], op_desc);
      err = 0;
      goto done;
    }

    if (v == 0) {
      v = st.get(map_type, "last_committed");
    }

    int fd = STDOUT_FILENO;
    if (!outpath.empty()){
      fd = ::open(outpath.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
      if (fd < 0) {
        std::cerr << "error opening output file: "
          << cpp_strerror(errno) << std::endl;
        err = EINVAL;
        goto done;
      }
    }

    bufferlist bl;
    r = 0;
    if (map_type == "osdmap") {
      r = st.get(map_type, st.combine_strings("full", v), bl);
    } else {
      r = st.get(map_type, v, bl);
    }
    if (r < 0) {
      std::cerr << "Error getting map: " << cpp_strerror(r) << std::endl;
      err = EINVAL;
      ::close(fd);
      goto done;
    }
    bl.write_fd(fd);

    if (!outpath.empty()) {
      std::cout << "wrote " << map_type
                << " version " << v << " to " << outpath
                << std::endl;
    }
  } else if (cmd == "dump-paxos") {
    unsigned dstart = 0;
    unsigned dstop = ~0;
    po::options_description op_desc("Allowed 'dump-paxos' options");
    op_desc.add_options()
      ("help,h", "produce this help message")
      ("start,s", po::value<unsigned>(&dstart),
       "starting version (default: 0)")
      ("end,e", po::value<unsigned>(&dstop),
       "finish version (default: ~0)")
      ;

    po::variables_map op_vm;
    int r = parse_cmd_args(&op_desc, NULL, NULL,
                           subcmds, &op_vm);
    if (r < 0) {
      err = -r;
      goto done;
    }

    if (op_vm.count("help")) {
      usage(argv[0], op_desc);
      err = 0;
      goto done;
    }

    if (dstart > dstop) {
      std::cerr << "error: 'start' version (value: " << dstart << ") "
                << " is greater than 'end' version (value: " << dstop << ")"
                << std::endl;
      err = EINVAL;
      goto done;
    }

    version_t v = dstart;
    for (; v <= dstop; ++v) {
      bufferlist bl;
      st.get("paxos", v, bl);
      if (bl.length() == 0)
	break;
      cout << "\n--- " << v << " ---" << std::endl;
      MonitorDBStore::TransactionRef tx(new MonitorDBStore::Transaction);
      Paxos::decode_append_transaction(tx, bl);
      JSONFormatter f(true);
      tx->dump(&f);
      f.flush(cout);
    }

    std::cout << "dumped " << v << " paxos versions" << std::endl;

  } else if (cmd == "dump-trace") {
    unsigned dstart = 0;
    unsigned dstop = ~0;
    string outpath;

    // visible options for this command
    po::options_description op_desc("Allowed 'dump-trace' options");
    op_desc.add_options()
      ("help,h", "produce this help message")
      ("start,s", po::value<unsigned>(&dstart),
       "starting version (default: 0)")
      ("end,e", po::value<unsigned>(&dstop),
       "finish version (default: ~0)")
      ;
    // this is going to be a positional argument; we don't want to show
    // it as an option during --help, but we do want to have it captured
    // when parsing.
    po::options_description hidden_op_desc("Hidden 'dump-trace' options");
    hidden_op_desc.add_options()
      ("out,o", po::value<string>(&outpath),
       "file to write the dump to")
      ;
    po::positional_options_description op_positional;
    op_positional.add("out", 1);

    po::variables_map op_vm;
    int r = parse_cmd_args(&op_desc, &hidden_op_desc, &op_positional,
                           subcmds, &op_vm);
    if (r < 0) {
      err = -r;
      goto done;
    }

    if (op_vm.count("help")) {
      usage(argv[0], op_desc);
      err = 0;
      goto done;
    }

    if (outpath.empty()) {
      usage(argv[0], op_desc);
      err = EINVAL;
      goto done;
    }

    if (dstart > dstop) {
      std::cerr << "error: 'start' version (value: " << dstart << ") "
                << " is greater than 'stop' version (value: " << dstop << ")"
                << std::endl;
      err = EINVAL;
      goto done;
    }

    TraceIter iter(outpath.c_str());
    iter.init();
    while (true) {
      if (!iter.valid())
	break;
      if (iter.num() >= dstop) {
	break;
      }
      if (iter.num() >= dstart) {
	JSONFormatter f(true);
	iter.cur()->dump(&f, false);
	f.flush(std::cout);
	std::cout << std::endl;
      }
      iter.next();
    }
    std::cerr << "Read up to transaction " << iter.num() << std::endl;
  } else if (cmd == "replay-trace") {
    string inpath;
    unsigned num_replays = 1;
    // visible options for this command
    po::options_description op_desc("Allowed 'replay-trace' options");
    op_desc.add_options()
      ("help,h", "produce this help message")
      ("num-replays,n", po::value<unsigned>(&num_replays),
       "finish version (default: 1)")
      ;
    // this is going to be a positional argument; we don't want to show
    // it as an option during --help, but we do want to have it captured
    // when parsing.
    po::options_description hidden_op_desc("Hidden 'replay-trace' options");
    hidden_op_desc.add_options()
      ("in,i", po::value<string>(&inpath),
       "file to write the dump to")
      ;
    po::positional_options_description op_positional;
    op_positional.add("in", 1);

    // op_desc_all will aggregate all visible and hidden options for parsing.
    // when we call 'usage()' we just pass 'op_desc', as that's the description
    // holding the visible options.
    po::options_description op_desc_all;
    op_desc_all.add(op_desc).add(hidden_op_desc);

    po::variables_map op_vm;
    try {
      po::parsed_options op_parsed = po::command_line_parser(subcmds).
        options(op_desc_all).positional(op_positional).run();
      po::store(op_parsed, op_vm);
      po::notify(op_vm);
    } catch (po::error &e) {
      std::cerr << "error: " << e.what() << std::endl;
      err = EINVAL;
      goto done;
    }

    if (op_vm.count("help")) {
      usage(argv[0], op_desc);
      err = 0;
      goto done;
    }

    if (inpath.empty()) {
      usage(argv[0], op_desc);
      err = EINVAL;
      goto done;
    }

    unsigned num = 0;
    for (unsigned i = 0; i < num_replays; ++i) {
      TraceIter iter(inpath.c_str());
      iter.init();
      while (true) {
	if (!iter.valid())
	  break;
	std::cerr << "Replaying trans num " << num << std::endl;
	st.apply_transaction(iter.cur());
	iter.next();
	++num;
      }
      std::cerr << "Read up to transaction " << iter.num() << std::endl;
    }
  } else if (cmd == "random-gen") {
    unsigned tsize = 200;
    unsigned tvalsize = 1024;
    unsigned ntrans = 100;
    po::options_description op_desc("Allowed 'random-gen' options");
    op_desc.add_options()
      ("help,h", "produce this help message")
      ("num-keys,k", po::value<unsigned>(&tsize),
       "keys to write in each transaction (default: 200)")
      ("size,s", po::value<unsigned>(&tvalsize),
       "size (in bytes) of the value to write in each key (default: 1024)")
      ("ntrans,n", po::value<unsigned>(&ntrans),
       "number of transactions to run (default: 100)")
      ;

    po::variables_map op_vm;
    try {
      po::parsed_options op_parsed = po::command_line_parser(subcmds).
        options(op_desc).run();
      po::store(op_parsed, op_vm);
      po::notify(op_vm);
    } catch (po::error &e) {
      std::cerr << "error: " << e.what() << std::endl;
      err = EINVAL;
      goto done;
    }

    if (op_vm.count("help")) {
      usage(argv[0], op_desc);
      err = 0;
      goto done;
    }

    unsigned num = 0;
    for (unsigned i = 0; i < ntrans; ++i) {
      std::cerr << "Applying trans " << i << std::endl;
      MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
      string prefix;
      prefix.push_back((i%26)+'a');
      for (unsigned j = 0; j < tsize; ++j) {
	stringstream os;
	os << num;
	bufferlist bl;
	for (unsigned k = 0; k < tvalsize; ++k) bl.append(rand());
	t->put(prefix, os.str(), bl);
	++num;
      }
      t->compact_prefix(prefix);
      st.apply_transaction(t);
    }
  } else if (cmd == "store-copy") {
    if (subcmds.size() < 1 || subcmds[0].empty()) {
      usage(argv[0], desc);
      err = EINVAL;
      goto done;
    }

    string out_path = subcmds[0];

    MonitorDBStore out_store(out_path);
    {
      stringstream ss;
      int r = out_store.create_and_open(ss);
      if (r < 0) {
        std::cerr << ss.str() << std::endl;
        goto done;
      }
    }


    KeyValueDB::WholeSpaceIterator it = st.get_iterator();
    uint64_t total_keys = 0;
    uint64_t total_size = 0;
    uint64_t total_tx = 0;

    do {
      uint64_t num_keys = 0;

      MonitorDBStore::TransactionRef tx(new MonitorDBStore::Transaction);

      while (it->valid() && num_keys < 128) {
        pair<string,string> k = it->raw_key();
        bufferlist v = it->value();
        tx->put(k.first, k.second, v);

        num_keys ++;
        total_tx ++;
        total_size += v.length();

        it->next();
      }

      total_keys += num_keys;

      if (!tx->empty())
        out_store.apply_transaction(tx);

      std::cout << "copied " << total_keys << " keys so far ("
                << stringify(si_t(total_size)) << ")" << std::endl;

    } while (it->valid());

    std::cout << "summary: copied " << total_keys << " keys, using "
              << total_tx << " transactions, totalling "
              << stringify(si_t(total_size)) << std::endl;
    std::cout << "from '" << store_path << "' to '" << out_path << "'"
              << std::endl;
  } else {
    std::cerr << "Unrecognized command: " << cmd << std::endl;
    usage(argv[0], desc);
    goto done;
  }

  done:
  st.close();
  return err;
}
