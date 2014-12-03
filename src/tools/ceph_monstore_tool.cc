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

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  int version = -1;
  string store_path, cmd, out_path, tfile;
  unsigned dstart = 0;
  unsigned dstop = ~0;
  unsigned num_replays = 1;
  unsigned tsize = 200;
  unsigned tvalsize = 1024;
  unsigned ntrans = 100;
  desc.add_options()
    ("help", "produce help message")
    ("mon-store-path", po::value<string>(&store_path),
     "path to mon directory, mandatory")
    ("out", po::value<string>(&out_path),
     "out path")
    ("version", po::value<int>(&version),
     "version requested")
    ("trace-file", po::value<string>(&tfile),
     "trace file")
    ("dump-start", po::value<unsigned>(&dstart),
     "transaction num to start dumping at")
    ("dump-end", po::value<unsigned>(&dstop),
     "transaction num to stop dumping at")
    ("num-replays", po::value<unsigned>(&num_replays),
     "number of times to replay")
    ("trans-size", po::value<unsigned>(&tsize),
     "keys to write in each transaction")
    ("trans-val-size", po::value<unsigned>(&tvalsize),
     "val to write in each key")
    ("num-trans", po::value<unsigned>(&ntrans),
     "number of transactions to run")
    ("command", po::value<string>(&cmd),
     "command")
    ;
  po::positional_options_description p;
  p.add("command", 1);
  p.add("version", 1);

  vector<string> ceph_option_strings;
  po::variables_map vm;
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(desc).positional(p).allow_unregistered().run();
    po::store(
	      parsed,
	      vm);
    po::notify(vm);

    ceph_option_strings = po::collect_unrecognized(parsed.options,
						   po::include_positional);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
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

  if (vm.count("help")) {
    std::cerr << desc << std::endl;
    return 1;
  }

  int fd;
  if (vm.count("out")) {
    if ((fd = open(out_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666)) < 0) {
      int _err = errno;
      if (_err != EISDIR) {
        std::cerr << "Couldn't open " << out_path << ": " << cpp_strerror(_err) << std::endl; 
        return 1;
      }
    }
  } else {
    fd = STDOUT_FILENO;
  }

  if (fd < 0 && cmd != "store-copy") {
    std::cerr << "error: '" << out_path << "' is a directory!" << std::endl;
    return 1;
  }

  MonitorDBStore st(store_path);
  if (store_path.size()) {
    stringstream ss;
    int r = st.open(ss);
    if (r < 0) {
      std::cerr << ss.str() << std::endl;
      goto done;
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
  } else if (cmd == "getmonmap") {
    assert(fd >= 0);
    if (!store_path.size()) {
      std::cerr << "need mon store path" << std::endl;
      std::cerr << desc << std::endl;
      goto done;
    }
    version_t v;
    if (version <= 0) {
      v = st.get("monmap", "last_committed");
    } else {
      v = version;
    }

    bufferlist bl;
    /// XXX: this is not ok, osdmap and full should be abstracted somewhere
    int r = st.get("monmap", v, bl);
    if (r < 0) {
      std::cerr << "Error getting map: " << cpp_strerror(r) << std::endl;
      goto done;
    }
    bl.write_fd(fd);
  } else if (cmd == "getosdmap") {
    if (!store_path.size()) {
      std::cerr << "need mon store path" << std::endl;
      std::cerr << desc << std::endl;
      goto done;
    }
    version_t v;
    if (version == -1) {
      v = st.get("osdmap", "last_committed");
    } else {
      v = version;
    }

    bufferlist bl;
    /// XXX: this is not ok, osdmap and full should be abstracted somewhere
    int r = st.get("osdmap", st.combine_strings("full", v), bl);
    if (r < 0) {
      std::cerr << "Error getting map: " << cpp_strerror(r) << std::endl;
      goto done;
    }
    bl.write_fd(fd);
  } else if (cmd == "dump-paxos") {
    for (version_t v = dstart; v <= dstop; ++v) {
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
  } else if (cmd == "dump-trace") {
    if (tfile.empty()) {
      std::cerr << "Need trace_file" << std::endl;
      std::cerr << desc << std::endl;
      goto done;
    }
    TraceIter iter(tfile.c_str());
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
    if (!store_path.size()) {
      std::cerr << "need mon store path" << std::endl;
      std::cerr << desc << std::endl;
      goto done;
    }
    if (tfile.empty()) {
      std::cerr << "Need trace_file" << std::endl;
      std::cerr << desc << std::endl;
      goto done;
    }
    unsigned num = 0;
    for (unsigned i = 0; i < num_replays; ++i) {
      TraceIter iter(tfile.c_str());
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
    if (!store_path.size()) {
      std::cerr << "need mon store path" << std::endl;
      std::cerr << desc << std::endl;
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
    if (!store_path.size()) {
      std::cerr << "need mon store path to copy from" << std::endl;
      std::cerr << desc << std::endl;
      goto done;
    }
    if (!out_path.size()) {
      std::cerr << "need mon store path to copy to (--out <mon_data_dir>)"
                << std::endl;
      std::cerr << desc << std::endl;
      goto done;
    }
    if (fd > 0) {
      std::cerr << "supplied out path '" << out_path << "' is not a directory"
                << std::endl;
      goto done;
    }

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
    goto done;
  }

  done:
  st.close();
  if (vm.count("out") && fd > 0) {
    ::close(fd);
  }
  return 0;
}
