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
#include <boost/scope_exit.hpp>

#include <stdlib.h>
#include <string>

#include "common/Formatter.h"
#include "common/errno.h"

#include "auth/KeyRing.h"
#include "auth/cephx/CephxKeyServer.h"
#include "global/global_init.h"
#include "include/scope_guard.h"
#include "include/stringify.h"
#include "mgr/mgr_commands.h"
#include "mon/AuthMonitor.h"
#include "mon/MonitorDBStore.h"
#include "mon/Paxos.h"
#include "mon/MonMap.h"
#include "mds/FSMap.h"
#include "mon/MgrMap.h"
#include "osd/OSDMap.h"
#include "crush/CrushCompiler.h"
#include "mon/CreatingPGs.h"

namespace po = boost::program_options;

using namespace std;

class TraceIter {
  int fd;
  unsigned idx;
  MonitorDBStore::TransactionRef t;
public:
  explicit TraceIter(string fname) : fd(-1), idx(-1) {
    fd = ::open(fname.c_str(), O_RDONLY|O_BINARY);
    t.reset(new MonitorDBStore::Transaction);
  }
  bool valid() {
    return fd != -1;
  }
  MonitorDBStore::TransactionRef cur() {
    ceph_assert(valid());
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
    auto bliter = bl.cbegin();
    uint8_t ver, ver2;
    decode(ver, bliter);
    decode(ver2, bliter);
    uint32_t len;
    decode(len, bliter);
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
    bliter = bl.cbegin();
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

  ceph_assert(desc != NULL);

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
 *  rewrite-crush
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
  << "  get mgr [-- options]            get mgr map (version VER if specified)\n"
  << "                                  (default: last committed)\n"
  << "  get crushmap [-- options]       get crushmap (version VER if specified)\n"
  << "                                  (default: last committed)\n"
  << "  get-key PREFIX KEY [-- options] get key\n"
  << "  remove-key PREFIX KEY           remove key\n"
  << "  show-versions [-- options]      show the first&last committed version of map\n"
  << "                                  (show-versions -- --help for more info)\n"
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
  << "  rewrite-crush [-- options]      add a rewrite commit to the store\n"
  << "                                  (rewrite-crush -- --help for more info)\n"
  << "  rebuild                         rebuild store\n"
  << "                                  (rebuild -- --help for more info)\n"
  << std::endl;
  std::cerr << d << std::endl;
  std::cerr
    << "\nPlease Note:\n"
    << "* Ceph-specific options should be in the format --option-name=VAL\n"
    << "  (specifically, do not forget the '='!!)\n"
    << "  e.g., 'dump-keys --debug-rocksdb=0'\n"
    << "* Command-specific options need to be passed after a '--'\n"
    << "  e.g., 'get monmap -- --version 10 --out /tmp/foo'"
    << std::endl;
}

int update_osdmap(MonitorDBStore& store, version_t ver, bool copy,
		  std::shared_ptr<CrushWrapper> crush,
		  MonitorDBStore::Transaction* t) {
  const string prefix("osdmap");

  // full
  bufferlist bl;
  int r = 0;
  r = store.get(prefix, store.combine_strings("full", ver), bl);
  if (r) {
    std::cerr << "Error getting full map: " << cpp_strerror(r) << std::endl;
    return r;
  }
  OSDMap osdmap;
  osdmap.decode(bl);
  osdmap.crush = crush;
  if (copy) {
    osdmap.inc_epoch();
  }
  bl.clear();
  // be consistent with OSDMonitor::update_from_paxos()
  osdmap.encode(bl, CEPH_FEATURES_ALL|CEPH_FEATURE_RESERVED);
  t->put(prefix, store.combine_strings("full", osdmap.get_epoch()), bl);

  // incremental
  OSDMap::Incremental inc;
  if (copy) {
    inc.epoch = osdmap.get_epoch();
    inc.fsid = osdmap.get_fsid();
  } else {
    bl.clear();
    r = store.get(prefix, ver, bl);
    if (r) {
      std::cerr << "Error getting inc map: " << cpp_strerror(r) << std::endl;
      return r;
    }
    OSDMap::Incremental inc(bl);
    if (inc.crush.length()) {
      inc.crush.clear();
      crush->encode(inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
    }
    if (inc.fullmap.length()) {
      OSDMap fullmap;
      fullmap.decode(inc.fullmap);
      fullmap.crush = crush;
      inc.fullmap.clear();
      fullmap.encode(inc.fullmap);
    }
  }
  ceph_assert(osdmap.have_crc());
  inc.full_crc = osdmap.get_crc();
  bl.clear();
  // be consistent with OSDMonitor::update_from_paxos()
  inc.encode(bl, CEPH_FEATURES_ALL|CEPH_FEATURE_RESERVED);
  t->put(prefix, inc.epoch, bl);
  return 0;
}

int rewrite_transaction(MonitorDBStore& store, int version,
			const string& crush_file,
			MonitorDBStore::Transaction* t) {
  const string prefix("osdmap");

  // calc the known-good epoch
  version_t last_committed = store.get(prefix, "last_committed");
  version_t good_version = 0;
  if (version <= 0) {
    if (last_committed >= (unsigned)-version) {
      good_version = last_committed + version;
    } else {
      std::cerr << "osdmap-version is less than: -" << last_committed << std::endl;
      return EINVAL;
    }
  } else {
    good_version = version;
  }
  if (good_version >= last_committed) {
    std::cout << "good epoch is greater or equal to the last committed one: "
	      << good_version << " >= " << last_committed << std::endl;
    return 0;
  }

  // load/extract the crush map
  int r = 0;
  std::shared_ptr<CrushWrapper> crush(new CrushWrapper);
  if (crush_file.empty()) {
    bufferlist bl;
    r = store.get(prefix, store.combine_strings("full", good_version), bl);
    if (r) {
      std::cerr << "Error getting map: " << cpp_strerror(r) << std::endl;
      return r;
    }
    OSDMap osdmap;
    osdmap.decode(bl);
    crush = osdmap.crush;
  } else {
    string err;
    bufferlist bl;
    r = bl.read_file(crush_file.c_str(), &err);
    if (r) {
      std::cerr << err << ": " << cpp_strerror(r) << std::endl;
      return r;
    }
    auto p = bl.cbegin();
    crush->decode(p);
  }

  // prepare a transaction to rewrite the epochs
  // (good_version, last_committed]
  // with the good crush map.
  // XXX: may need to break this into several paxos versions?
  ceph_assert(good_version < last_committed);
  for (version_t v = good_version + 1; v <= last_committed; v++) {
    cout << "rewriting epoch #" << v << "/" << last_committed << std::endl;
    r = update_osdmap(store, v, false, crush, t);
    if (r)
      return r;
  }

  // add a new osdmap epoch to store, so monitors will update their current osdmap
  // in addition to the ones stored in epochs.
  //
  // This is needed due to the way the monitor updates from paxos and the
  // facilities we are leveraging to push this update to the rest of the
  // quorum.
  //
  // In a nutshell, we are generating a good version of the osdmap, with a
  // proper crush, and building a transaction that will replace the bad
  // osdmaps with good osdmaps. But this transaction needs to be applied on
  // all nodes, so that the monitors will have good osdmaps to share with
  // clients. We thus leverage Paxos, specifically the recovery mechanism, by
  // creating a pending value that will be committed once the monitors form an
  // initial quorum after being brought back to life.
  //
  // However, the way the monitor works has the paxos services, including the
  // OSDMonitor, updating their state from disk *prior* to the recovery phase
  // begins (so they have an up to date state in memory). This means the
  // OSDMonitor will see the old, broken map, before the new paxos version is
  // applied to disk, and the old version is cached. Even though we have the
  // good map now, and we share the good map with clients, we will still be
  // working on the old broken map. Instead of mucking around the monitor to
  // make this work, we instead opt for adding the same osdmap but with a
  // newer version, so that the OSDMonitor picks up on it when it updates from
  // paxos after the proposal has been committed. This is not elegant, but
  // avoids further unpleasantness that would arise from kludging around the
  // current behavior. Also, has the added benefit of making sure the clients
  // get an updated version of the map (because last_committed+1 >
  // last_committed) :)
  //
  cout << "adding a new epoch #" << last_committed+1 << std::endl;
  r = update_osdmap(store, last_committed++, true, crush, t);
  if (r)
    return r;
  t->put(prefix, store.combine_strings("full", "latest"), last_committed);
  t->put(prefix, "last_committed", last_committed);
  return 0;
}

/**
 * create a new paxos version which carries a proposal to rewrite all epochs
 * of incremental and full map of "osdmap" after a faulty crush map is injected.
 * so the leader will trigger a recovery and propagate this fix to its peons,
 * after the proposal is accepted, and the transaction in it is applied. all
 * monitors will rewrite the bad crush map with the good one, and have a new
 * osdmap epoch with the good crush map in it.
 */
int rewrite_crush(const char* progname,
		  vector<string>& subcmds,
		  MonitorDBStore& store) {
  po::options_description op_desc("Allowed 'rewrite-crush' options");
  int version = -1;
  string crush_file;
  op_desc.add_options()
    ("help,h", "produce this help message")
    ("crush", po::value<string>(&crush_file),
     ("path to the crush map file "
      "(default: will instead extract it from the known-good osdmap)"))
    ("good-epoch", po::value<int>(&version),
     "known-good epoch of osdmap, if a negative number '-N' is given, the "
     "$last_committed-N is used instead (default: -1). "
     "Please note, -1 is not necessarily a good epoch, because there are "
     "good chance that we have more epochs slipped into the monstore after "
     "the one where the crushmap is firstly injected.")
    ;
  po::variables_map op_vm;
  int r = parse_cmd_args(&op_desc, NULL, NULL, subcmds, &op_vm);
  if (r) {
    return -r;
  }
  if (op_vm.count("help")) {
    usage(progname, op_desc);
    return 0;
  }

  MonitorDBStore::Transaction rewrite_txn;
  r = rewrite_transaction(store, version, crush_file, &rewrite_txn);
  if (r) {
    return r;
  }

  // store the transaction into store as a proposal
  const string prefix("paxos");
  version_t pending_v = store.get(prefix, "last_committed") + 1;
  auto t(std::make_shared<MonitorDBStore::Transaction>());
  bufferlist bl;
  rewrite_txn.encode(bl);
  cout << "adding pending commit " << pending_v
       << " " << bl.length() << " bytes" << std::endl;
  t->put(prefix, pending_v, bl);
  t->put(prefix, "pending_v", pending_v);
  // a large enough yet unique proposal number will probably do the trick
  version_t pending_pn = (store.get(prefix, "accepted_pn") / 100 + 4) * 100 + 1;
  t->put(prefix, "pending_pn", pending_pn);
  store.apply_transaction(t);
  return 0;
}

static int update_auth(MonitorDBStore& st, const string& keyring_path)
{
  // import all keyrings stored in the keyring file
  KeyRing keyring;
  int r = keyring.load(g_ceph_context, keyring_path);
  if (r < 0) {
    cerr << "unable to load admin keyring: " << keyring_path << std::endl;
    return r;
  }

  bufferlist bl;
  __u8 v = 1;
  encode(v, bl);

  for (const auto& k : keyring.get_keys()) {
    KeyServerData::Incremental auth_inc;
    auth_inc.name = k.first;
    auth_inc.auth = k.second;
    if (auth_inc.auth.caps.empty()) {
      cerr << "no caps granted to: " << auth_inc.name << std::endl;
      return -EINVAL;
    }
    map<string,string> caps;
    std::transform(begin(auth_inc.auth.caps), end(auth_inc.auth.caps),
		   inserter(caps, end(caps)),
		   [](auto& cap) {
		     string c;
		     auto p = cap.second.cbegin();
		     decode(c, p);
		     return make_pair(cap.first, c);
		   });
    cout << "adding auth for '"
	 << auth_inc.name << "': " << auth_inc.auth
	 << " with caps(" << caps << ")" << std::endl;
    auth_inc.op = KeyServerData::AUTH_INC_ADD;

    AuthMonitor::Incremental inc;
    inc.inc_type = AuthMonitor::AUTH_DATA;
    encode(auth_inc, inc.auth_data);
    inc.auth_type = CEPH_AUTH_CEPHX;
    inc.encode(bl, CEPH_FEATURES_ALL);
  }

  // prime rotating secrets
  {
    KeyServer ks(g_ceph_context, nullptr);
    KeyServerData::Incremental auth_inc;
    auth_inc.op = KeyServerData::AUTH_INC_SET_ROTATING;
    bool r = ks.prepare_rotating_update(auth_inc.rotating_bl);
    ceph_assert(r);
    AuthMonitor::Incremental inc;
    inc.inc_type = AuthMonitor::AUTH_DATA;
    encode(auth_inc, inc.auth_data);
    inc.auth_type = CEPH_AUTH_CEPHX;
    inc.encode(bl, CEPH_FEATURES_ALL);
  }

  const string prefix("auth");
  auto last_committed = st.get(prefix, "last_committed") + 1;
  auto t = make_shared<MonitorDBStore::Transaction>();
  t->put(prefix, last_committed, bl);
  t->put(prefix, "last_committed", last_committed);
  auto first_committed = st.get(prefix, "first_committed");
  if (!first_committed) {
    t->put(prefix, "first_committed", last_committed);
  }
  st.apply_transaction(t);
  return 0;
}

static int update_mkfs(MonitorDBStore& st,
		       const string& monmap_path,
		       const vector<string>& mon_ids)
{
  MonMap monmap;
  if (!monmap_path.empty()) {
    cout << __func__ << " pulling initial monmap from " << monmap_path << std::endl;
    bufferlist bl;
    string err;
    int r = bl.read_file(monmap_path.c_str(), &err);
    if (r < 0) {
      cerr << "failed to read monmap from " << monmap_path << ": "
	   << cpp_strerror(r) << std::endl;
      return r;
    }
    monmap.decode(bl);
  } else {
    cout << __func__ << " generating seed initial monmap" << std::endl;
    int r = monmap.build_initial(g_ceph_context, true, cerr);
    if (r) {
      cerr << "no initial monitors" << std::endl;
      return -EINVAL;
    }
    vector<string> new_names;
    if (!mon_ids.empty()) {
      if (mon_ids.size() != monmap.size()) {
	cerr << "Please pass the same number of <mon-ids> to name the hosts "
	     << "listed in 'mon_host'. "
	     << mon_ids.size() << " mon-id(s) specified, "
	     << "while you have " << monmap.size() << " mon hosts." << std::endl;
	return -EINVAL;
      }
      new_names = mon_ids;
    } else {
      for (unsigned rank = 0; rank < monmap.size(); rank++) {
	string new_name{"a"};
	new_name[0] += rank;
	new_names.push_back(std::move(new_name));
      }
    }
    for (unsigned rank = 0; rank < monmap.size(); rank++) {
      auto name = monmap.get_name(rank);
      if (name.compare(0, 7, "noname-") == 0) {
	monmap.rename(name, new_names[rank]);
      }
    }
  }
  monmap.print(cout);
  bufferlist bl;
  monmap.encode(bl, CEPH_FEATURES_ALL);
  monmap.set_epoch(0);
  auto t = make_shared<MonitorDBStore::Transaction>();
  t->put("mkfs", "monmap", bl);
  st.apply_transaction(t);
  return 0;
}

static int update_monitor(MonitorDBStore& st)
{
  const string prefix("monitor");
  // a stripped-down Monitor::mkfs()
  bufferlist bl;
  bl.append(CEPH_MON_ONDISK_MAGIC "\n");
  auto t = make_shared<MonitorDBStore::Transaction>();
  t->put(prefix, "magic", bl);
  st.apply_transaction(t);
  return 0;
}

// rebuild
//  - creating_pgs
static int update_creating_pgs(MonitorDBStore& st)
{
  bufferlist bl;
  auto last_osdmap_epoch = st.get("osdmap", "last_committed");
  int r = st.get("osdmap", st.combine_strings("full", last_osdmap_epoch), bl);
  if (r < 0) {
    cerr << "unable to load osdmap e" << last_osdmap_epoch << std::endl;
    return r;
  }

  OSDMap osdmap;
  osdmap.decode(bl);
  creating_pgs_t creating;
  for (auto& i : osdmap.get_pools()) {
    creating.created_pools.insert(i.first);
  }
  creating.last_scan_epoch = last_osdmap_epoch;

  bufferlist newbl;
  encode(creating, newbl, CEPH_FEATURES_ALL);

  auto t = make_shared<MonitorDBStore::Transaction>();
  t->put("osd_pg_creating", "creating", newbl);
  st.apply_transaction(t);
  return 0;
}

// rebuild
//  - mgr
//  - mgr_command_desc
static int update_mgrmap(MonitorDBStore& st)
{
  auto t = make_shared<MonitorDBStore::Transaction>();

  {
    MgrMap map;
    // mgr expects epoch > 1
    map.epoch++;
    auto initial_modules =
      get_str_vec(g_ceph_context->_conf.get_val<string>("mgr_initial_modules"));
    copy(begin(initial_modules),
	 end(initial_modules),
	 inserter(map.modules, end(map.modules)));
    bufferlist bl;
    map.encode(bl, CEPH_FEATURES_ALL);
    t->put("mgr", map.epoch, bl);
    t->put("mgr", "last_committed", map.epoch);
  }
  {
    auto mgr_command_descs = mgr_commands;
    for (auto& c : mgr_command_descs) {
      c.set_flag(MonCommand::FLAG_MGR);
    }
    bufferlist bl;
    encode(mgr_command_descs, bl);
    t->put("mgr_command_descs", "", bl);
  }
  return st.apply_transaction(t);
}

static int update_paxos(MonitorDBStore& st)
{
  const string prefix("paxos");
  // a large enough version greater than the maximum possible `last_committed`
  // that could be replied by the peons when the leader is collecting paxos
  // transactions during recovery
  constexpr version_t first_committed = 0x42;
  constexpr version_t last_committed = first_committed;
  for (version_t v = first_committed; v < last_committed + 1; v++) {
    auto t = make_shared<MonitorDBStore::Transaction>();
    if (v == first_committed) {
      t->put(prefix, "first_committed", v);
    }
    bufferlist proposal;
    MonitorDBStore::Transaction empty_txn;
    empty_txn.encode(proposal);
    t->put(prefix, v, proposal);
    t->put(prefix, "last_committed", v);
    st.apply_transaction(t);
  }
  // build a pending paxos proposal from all non-permanent k/v pairs. once the
  // proposal is committed, it will gets applied. on the sync provider side, it
  // will be a no-op, but on its peers, the paxos commit will help to build up
  // the necessary epochs.
  bufferlist pending_proposal;
  {
    MonitorDBStore::Transaction t;
    vector<string> prefixes = {"auth", "osdmap",
			       "mgr", "mgr_command_desc"};
    for (const auto& prefix : prefixes) {
      for (auto i = st.get_iterator(prefix); i->valid(); i->next()) {
	auto key = i->raw_key();
	auto val = i->value();
	t.put(key.first, key.second, val);
      }
    }
    t.encode(pending_proposal);
  }
  auto pending_v = last_committed + 1;
  auto t = make_shared<MonitorDBStore::Transaction>();
  t->put(prefix, pending_v, pending_proposal);
  t->put(prefix, "pending_v", pending_v);
  t->put(prefix, "pending_pn", 400);
  st.apply_transaction(t);
  return 0;
}

int rebuild_monstore(const char* progname,
		     vector<string>& subcmds,
		     MonitorDBStore& st)
{
  po::options_description op_desc("Allowed 'rebuild' options");
  string keyring_path;
  string monmap_path;
  vector<string> mon_ids;
  op_desc.add_options()
    ("keyring", po::value<string>(&keyring_path),
     "path to the client.admin key")
    ("monmap", po::value<string>(&monmap_path),
     "path to the initial monmap")
    ("mon-ids", po::value<vector<string>>(&mon_ids)->multitoken(),
     "mon ids, use 'a', 'b', ... if not specified");
  po::positional_options_description pos_desc;
  pos_desc.add("mon-ids", -1);
  po::variables_map op_vm;
  int r = parse_cmd_args(&op_desc, nullptr, &pos_desc, subcmds, &op_vm);
  if (r) {
    return -r;
  }
  if (op_vm.count("help")) {
    usage(progname, op_desc);
    return 0;
  }
  if (!keyring_path.empty())
    update_auth(st, keyring_path);
  if ((r = update_creating_pgs(st))) {
    return r;
  }
  if ((r = update_mgrmap(st))) {
    return r;
  }
  if ((r = update_paxos(st))) {
    return r;
  }
  if ((r = update_mkfs(st, monmap_path, mon_ids))) {
    return r;
  }
  if ((r = update_monitor(st))) {
    return r;
  }
  return 0;
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

  vector<const char *> ceph_options;
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  auto cct = global_init(
    NULL, ceph_options, CEPH_ENTITY_TYPE_MON,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(g_ceph_context);
  cct->_conf.apply_changes(nullptr);

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

  auto close_store = make_scope_guard([&] {
    st.close();
  });

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
      ("readable,r", "print the map information in human readable format")
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
      return -r;
    }

    if (op_vm.count("help") || map_type.empty()) {
      usage(argv[0], op_desc);
      return 0;
    }

    if (v == 0) {
      if (map_type == "crushmap") {
        v = st.get("osdmap", "last_committed");
      } else {
        v = st.get(map_type, "last_committed");
      }
    }

    int fd = STDOUT_FILENO;
    if (!outpath.empty()){
      fd = ::open(outpath.c_str(), O_WRONLY|O_CREAT|O_TRUNC|O_BINARY, 0666);
      if (fd < 0) {
        std::cerr << "error opening output file: "
          << cpp_strerror(errno) << std::endl;
        return EINVAL;
      }
    }

    auto close_fd = make_scope_guard([&] {
      ::close(fd);
      if (r < 0 && fd != STDOUT_FILENO) {
        ::remove(outpath.c_str());
      }
    });

    bufferlist bl;
    r = 0;
    if (map_type == "osdmap") {
      r = st.get(map_type, st.combine_strings("full", v), bl);
    } else if (map_type == "crushmap") {
      bufferlist tmp;
      r = st.get("osdmap", st.combine_strings("full", v), tmp);
      if (r >= 0) {
        OSDMap osdmap;
        osdmap.decode(tmp);
        osdmap.crush->encode(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
      }
    } else {
      r = st.get(map_type, v, bl);
    }
    if (r < 0) {
      std::cerr << "Error getting map: " << cpp_strerror(r) << std::endl;
      return EINVAL;
    }

    if (op_vm.count("readable")) {
      stringstream ss;
      bufferlist out;
      try {
        if (map_type == "monmap") {
          MonMap monmap;
          monmap.decode(bl);
          monmap.print(ss);
        } else if (map_type == "osdmap") {
          OSDMap osdmap;
          osdmap.decode(bl);
          osdmap.print(cct.get(), ss);
        } else if (map_type == "mdsmap") {
          FSMap fs_map;
          fs_map.decode(bl);
          fs_map.print(ss);
        } else if (map_type == "mgr") {
          MgrMap mgr_map;
          auto p = bl.cbegin();
          mgr_map.decode(p);
          JSONFormatter f;
          f.dump_object("mgrmap", mgr_map);
          f.flush(ss);
        } else if (map_type == "crushmap") {
          CrushWrapper cw;
          auto it = bl.cbegin();
          cw.decode(it);
          CrushCompiler cc(cw, std::cerr, 0);
          cc.decompile(ss);
        } else {
          std::cerr << "This type of readable map does not exist: " << map_type
                    << std::endl << "You can only specify[osdmap|monmap|mdsmap"
                    "|crushmap|mgr]" << std::endl;
        }
      } catch (const buffer::error &err) {
        std::cerr << "Could not decode for human readable output (you may still"
	  " use non-readable mode).  Detail: " << err.what() << std::endl;
      }

      out.append(ss);
      out.write_fd(fd);
    } else {
      bl.write_fd(fd);
    }

    if (!outpath.empty()) {
      std::cout << "wrote " << map_type
                << " version " << v << " to " << outpath
                << std::endl;
    }
} else if (cmd == "get-key") {
    string outpath;
    string prefix;
    string key;

    // visible options for this command
    po::options_description op_desc("Allowed 'get-key' options");
    op_desc.add_options()
      ("help,h", "produce this help message")
      ("out,o", po::value<string>(&outpath),
       "output file (default: stdout)")
      ("readable,r", "print the map information in human readable format")
      ;
    // this is going to be a positional argument; we don't want to show
    // it as an option during --help, but we do want to have it captured
    // when parsing.
    po::options_description hidden_op_desc("Hidden 'get-key' options");
    hidden_op_desc.add_options()
      ("prefix", po::value<string>(&prefix),"prefix")
      ("key", po::value<string>(&key),"key")
      ;
    po::positional_options_description op_positional;
    op_positional.add("prefix", 1);
    op_positional.add("key", 1);


    po::variables_map op_vm;
    int r = parse_cmd_args(&op_desc, &hidden_op_desc, &op_positional,
                           subcmds, &op_vm);
    if (r < 0) {
      return -r;
    }

    if (op_vm.count("help") || prefix.empty()) {
      usage(argv[0], op_desc);
      return 0;
    }

    int fd = STDOUT_FILENO;
    if (!outpath.empty()){
      fd = ::open(outpath.c_str(), O_WRONLY|O_CREAT|O_TRUNC|O_BINARY, 0666);
      if (fd < 0) {
        std::cerr << "error opening output file: "
          << cpp_strerror(errno) << std::endl;
        return EINVAL;
      }
    }

    auto close_fd = make_scope_guard([&] {
      ::close(fd);
      if (r < 0 && fd != STDOUT_FILENO) {
        ::remove(outpath.c_str());
      }
    });
    bufferlist bl;
    r = 0;
    std::cout << prefix << " " << key << std::endl;
    r = st.get(prefix, key, bl);
    if (r < 0) {
      std::cerr << "Error getting key: " << cpp_strerror(r) << std::endl;
      return EINVAL;
    }

    if (op_vm.count("readable")) {
      try {
        if (prefix == "osd_snap") {
          auto p = bl.cbegin();
          if (key.starts_with("purged_epoch_")) {
            map<int64_t,snap_interval_set_t> val;
            ceph::decode(val, p);
            std::cout << val << std::endl;
          } else if (key.starts_with("purged_snap_")) {
            snapid_t first_snap, end_snap;
            epoch_t epoch;
            ceph::decode(first_snap, p);
            ceph::decode(end_snap, p);
            ceph::decode(epoch, p);
            std::cout << "first_snap:" << first_snap
                      << " end_snap: " << end_snap
                      << " epoch: " << epoch
                      << std::endl;
          }
        } else {
          std::cerr << "This type of readable key does not exist: " << prefix
                    << std::endl << "You can only specify[osd_snap]" << std::endl;
        }
      } catch (const buffer::error &err) {
        std::cerr << "Could not decode for human readable output (you may still"
	  " use non-readable mode).  Detail: " << err.what() << std::endl;
      }
    }

    bl.write_fd(fd);

    if (!outpath.empty()) {
      std::cout << "wrote " << prefix << " " <<  key <<  " to " << outpath << std::endl;
    }
  } else if (cmd == "show-versions") {
    string map_type; //map type:osdmap,monmap...
    // visible options for this command
    po::options_description op_desc("Allowed 'show-versions' options");
    op_desc.add_options()
      ("help,h", "produce this help message")
      ("map-type", po::value<string>(&map_type), "map_type");

    po::positional_options_description op_positional;
    op_positional.add("map-type", 1);

    po::variables_map op_vm;
    int r = parse_cmd_args(&op_desc, NULL, &op_positional,
                           subcmds, &op_vm);
    if (r < 0) {
      return -r;
    }

    if (op_vm.count("help") || map_type.empty()) {
      usage(argv[0], op_desc);
      return  0;
    }

    unsigned int v_first = 0;
    unsigned int v_last = 0;
    v_first = st.get(map_type, "first_committed");
    v_last = st.get(map_type, "last_committed");

    std::cout << "first committed:\t" << v_first << "\n"
              << "last  committed:\t" << v_last << std::endl;
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
      return -r;
    }

    if (op_vm.count("help")) {
      usage(argv[0], op_desc);
      return 0;
    }

    if (dstart > dstop) {
      std::cerr << "error: 'start' version (value: " << dstart << ") "
                << " is greater than 'end' version (value: " << dstop << ")"
                << std::endl;
      return EINVAL;
    }

    version_t v = dstart;
    for (; v <= dstop; ++v) {
      bufferlist bl;
      st.get("paxos", v, bl);
      if (bl.length() == 0)
	break;
      cout << "\n--- " << v << " ---" << std::endl;
      auto tx(std::make_shared<MonitorDBStore::Transaction>());
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
      return -r;
    }

    if (op_vm.count("help")) {
      usage(argv[0], op_desc);
      return 0;
    }

    if (outpath.empty()) {
      usage(argv[0], op_desc);
      return EINVAL;
    }

    if (dstart > dstop) {
      std::cerr << "error: 'start' version (value: " << dstart << ") "
                << " is greater than 'stop' version (value: " << dstop << ")"
                << std::endl;
      return EINVAL;
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
      return EINVAL;
    }

    if (op_vm.count("help")) {
      usage(argv[0], op_desc);
      return 0;
    }

    if (inpath.empty()) {
      usage(argv[0], op_desc);
      return EINVAL;
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
      return EINVAL;
    }

    if (op_vm.count("help")) {
      usage(argv[0], op_desc);
      return 0;
    }

    unsigned num = 0;
    for (unsigned i = 0; i < ntrans; ++i) {
      std::cerr << "Applying trans " << i << std::endl;
      auto t(std::make_shared<MonitorDBStore::Transaction>());
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
      return EINVAL;
    }

    string out_path = subcmds[0];

    MonitorDBStore out_store(out_path);
    {
      stringstream ss;
      int r = out_store.create_and_open(ss);
      if (r < 0) {
        std::cerr << ss.str() << std::endl;
        return err;
      }
    }


    KeyValueDB::WholeSpaceIterator it = st.get_iterator();
    uint64_t total_keys = 0;
    uint64_t total_size = 0;
    uint64_t total_tx = 0;

    do {
      uint64_t num_keys = 0;

      auto tx(std::make_shared<MonitorDBStore::Transaction>());

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
                << stringify(byte_u_t(total_size)) << ")" << std::endl;

    } while (it->valid());
    out_store.close();
    std::cout << "summary: copied " << total_keys << " keys, using "
              << total_tx << " transactions, totalling "
              << stringify(byte_u_t(total_size)) << std::endl;
    std::cout << "from '" << store_path << "' to '" << out_path << "'"
              << std::endl;
  } else if (cmd == "rewrite-crush") {
    err = rewrite_crush(argv[0], subcmds, st);
  } else if (cmd == "rebuild") {
    err = rebuild_monstore(argv[0], subcmds, st);
  } else if (cmd == "remove-key") {
    string prefix, key;
    // No visible options for this command
    po::options_description op_desc("Allowed 'get' options");
    po::options_description hidden_op_desc("Hidden 'get' options");
    hidden_op_desc.add_options()
      ("prefix", po::value<string>(&prefix),"prefix")
      ("key", po::value<string>(&key),"key")
      ;
    po::positional_options_description op_positional;
    op_positional.add("prefix", 1);
    op_positional.add("key", 1);

    po::variables_map op_vm;
    int r = parse_cmd_args(&op_desc, &hidden_op_desc, &op_positional,
                           subcmds, &op_vm);
    if (r < 0) {
      return -r;
    }
    r = st.clear_key(prefix, key);
    if (r < 0) {
      std::cerr << "error removing ("
                << prefix << "," << key << ")"
                << std::endl;
      return r;
    }
  } else {
    std::cerr << "Unrecognized command: " << cmd << std::endl;
    usage(argv[0], desc);
    return err;
  }
}
