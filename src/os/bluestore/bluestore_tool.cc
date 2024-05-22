// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

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

#include "os/bluestore/BlueFS.h"
#include "os/bluestore/BlueStore.h"
#include "common/admin_socket.h"
#include "kv/RocksDBStore.h"

using namespace std;
namespace fs = std::filesystem;
namespace po = boost::program_options;

void usage(po::options_description &desc)
{
  cout << desc << std::endl;
}

void validate_path(CephContext *cct, const string& path, bool bluefs)
{
  BlueStore bluestore(cct, path);
  string type;
  int r = bluestore.read_meta("type", &type);
  if (r < 0) {
    cerr << "failed to load os-type: " << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }
  if (type != "bluestore") {
    cerr << "expected bluestore, but type is " << type << std::endl;
    exit(EXIT_FAILURE);
  }
  if (!bluefs) {
    return;
  }

  string kv_backend;
  r = bluestore.read_meta("kv_backend", &kv_backend);
  if (r < 0) {
    cerr << "failed to load kv_backend: " << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }
  if (kv_backend != "rocksdb") {
    cerr << "expect kv_backend to be rocksdb, but is " << kv_backend
         << std::endl;
    exit(EXIT_FAILURE);
  }
  string bluefs_enabled;
  r = bluestore.read_meta("bluefs", &bluefs_enabled);
  if (r < 0) {
    cerr << "failed to load do_bluefs: " << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }
  if (bluefs_enabled != "1") {
    cerr << "bluefs not enabled for rocksdb" << std::endl;
    exit(EXIT_FAILURE);
  }
}

const char* find_device_path(
  int id,
  CephContext *cct,
  const vector<string>& devs)
{
  for (auto& i : devs) {
    bluestore_bdev_label_t label;
    int r = BlueStore::_read_bdev_label(cct, i, &label);
    if (r < 0) {
      cerr << "unable to read label for " << i << ": "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
    if ((id == BlueFS::BDEV_SLOW && label.description == "main") ||
        (id == BlueFS::BDEV_DB && label.description == "bluefs db") ||
        (id == BlueFS::BDEV_WAL && label.description == "bluefs wal")) {
      return i.c_str();
    }
  }
  return nullptr;
}

void parse_devices(
  CephContext *cct,
  const vector<string>& devs,
  map<string, int>* got,
  bool* has_db,
  bool* has_wal)
{
  string main;
  bool was_db = false;
  if (has_wal) {
    *has_wal = false;
  }
  if (has_db) {
    *has_db = false;
  }
  for (auto& d : devs) {
    bluestore_bdev_label_t label;
    int r = BlueStore::_read_bdev_label(cct, d, &label);
    if (r < 0) {
      cerr << "unable to read label for " << d << ": "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
    int id = -1;
    if (label.description == "main")
      main = d;
    else if (label.description == "bluefs db") {
      id = BlueFS::BDEV_DB;
      was_db = true;
      if (has_db) {
	*has_db = true;
      }
    }
    else if (label.description == "bluefs wal") {
      id = BlueFS::BDEV_WAL;
      if (has_wal) {
	*has_wal = true;
      }
    }
    if (id >= 0) {
      got->emplace(d, id);
    }
  }
  if (main.length()) {
    int id = was_db ? BlueFS::BDEV_SLOW : BlueFS::BDEV_DB;
    got->emplace(main, id);
  }
}

void add_devices(
  BlueFS *fs,
  CephContext *cct,
  const vector<string>& devs)
{
  map<string, int> got;
  parse_devices(cct, devs, &got, nullptr, nullptr);
  for(auto e : got) {
    char target_path[PATH_MAX] = "";
    if(!e.first.empty()) {
      if (realpath(e.first.c_str(), target_path) == nullptr) {
	cerr << "failed to retrieve absolute path for " << e.first
	      << ": " << cpp_strerror(errno)
	      << std::endl;
      }
    }

    cout << " slot " << e.second << " " << e.first;
    if (target_path[0]) {
      cout << " -> " << target_path;
    }
    cout << std::endl;

    // We provide no shared allocator which prevents bluefs to operate in R/W mode.
    // Read-only mode isn't strictly enforced though
    int r = fs->add_block_device(e.second, e.first, false, 0); // 'reserved' is fake
    if (r < 0) {
      cerr << "unable to open " << e.first << ": " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
  }
}

BlueFS *open_bluefs_readonly(
  CephContext *cct,
  const string& path,
  const vector<string>& devs)
{
  validate_path(cct, path, true);
  BlueFS *fs = new BlueFS(cct);

  add_devices(fs, cct, devs);

  int r = fs->mount();
  if (r < 0) {
    cerr << "unable to mount bluefs: " << cpp_strerror(r)
	 << std::endl;
    exit(EXIT_FAILURE);
  }
  return fs;
}

void log_dump(
  CephContext *cct,
  const string& path,
  const vector<string>& devs)
{
  validate_path(cct, path, true);
  BlueFS *fs = new BlueFS(cct);

  add_devices(fs, cct, devs);
  int r = fs->log_dump();
  if (r < 0) {
    cerr << "log_dump failed" << ": "
         << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }

  delete fs;
}

void inferring_bluefs_devices(vector<string>& devs, std::string& path)
{
  cout << "inferring bluefs devices from bluestore path" << std::endl;
  for (auto fn : {"block", "block.wal", "block.db"}) {
    string p = path + "/" + fn;
    struct stat st;
    if (::stat(p.c_str(), &st) == 0) {
      devs.push_back(p);
    }
  }
}

static void bluefs_import(
  const string& input_file,
  const string& dest_file,
  CephContext *cct,
  const string& path,
  const vector<string>& devs)
{
  int r;
  std::ifstream f(input_file.c_str(), std::ifstream::binary);
  if (!f) {
    r = -errno;
    cerr << "open " << input_file.c_str() << " failed: " << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }
  BlueStore bluestore(cct, path);
  KeyValueDB *db_ptr;
  r = bluestore.open_db_environment(&db_ptr, false);
  if (r < 0) {
    cerr << "error preparing db environment: " << cpp_strerror(r) << std::endl;
    exit(EXIT_FAILURE);
  }
  BlueFS* bs = bluestore.get_bluefs();

  BlueFS::FileWriter *h;
  fs::path file_path(dest_file);
  const string dir = file_path.parent_path().native();
  const string file_name = file_path.filename().native();
  bs->open_for_write(dir, file_name, &h, false);
  uint64_t max_block = 4096;
  char buf[max_block];
  uint64_t left = fs::file_size(input_file.c_str());
  uint64_t size = 0;
  while (left) {
    size = std::min(max_block, left);
    f.read(buf, size);
    h->append(buf, size);
    left -= size;
  }
  f.close();
  bs->fsync(h);
  bs->close_writer(h);
  bluestore.close_db_environment();
  return;
}

int main(int argc, char **argv)
{
  string out_dir;
  string osd_instance;
  vector<string> devs;
  vector<string> devs_source;
  string dev_target;
  string path;
  string action;
  string log_file;
  string input_file;
  string dest_file;
  string key, value;
  vector<string> allocs_name;
  string empty_sharding(1, '\0');
  string new_sharding = empty_sharding;
  string resharding_ctrl;
  int log_level = 30;
  bool fsck_deep = false;
  po::options_description po_options("Options");
  po_options.add_options()
    ("help,h", "produce help message")
    (",i", po::value<string>(&osd_instance), "OSD instance. Requires access to monitor/ceph.conf")
    ("path", po::value<string>(&path), "bluestore path")
    ("out-dir", po::value<string>(&out_dir), "output directory")
    ("input-file", po::value<string>(&input_file), "import file")
    ("dest-file", po::value<string>(&dest_file), "destination file")
    ("log-file,l", po::value<string>(&log_file), "log file")
    ("log-level", po::value<int>(&log_level), "log level (30=most, 20=lots, 10=some, 1=little)")
    ("dev", po::value<vector<string>>(&devs), "device(s)")
    ("devs-source", po::value<vector<string>>(&devs_source), "bluefs-dev-migrate source device(s)")
    ("dev-target", po::value<string>(&dev_target), "target/resulting device")
    ("deep", po::value<bool>(&fsck_deep), "deep fsck (read all data)")
    ("key,k", po::value<string>(&key), "label metadata key name")
    ("value,v", po::value<string>(&value), "label metadata value")
    ("allocator", po::value<vector<string>>(&allocs_name), "allocator to inspect: 'block'/'bluefs-wal'/'bluefs-db'")
    ("sharding", po::value<string>(&new_sharding), "new sharding to apply")
    ("resharding-ctrl", po::value<string>(&resharding_ctrl), "gives control over resharding procedure details")
    ;
  po::options_description po_positional("Positional options");
  po_positional.add_options()
    ("command", po::value<string>(&action),
        "fsck, "
        "qfsck, "
        "allocmap, "
        "restore_cfb, "
        "repair, "
        "quick-fix, "
        "bluefs-export, "
        "bluefs-import, "
        "bluefs-bdev-sizes, "
        "bluefs-bdev-expand, "
        "bluefs-bdev-new-db, "
        "bluefs-bdev-new-wal, "
        "bluefs-bdev-migrate, "
        "show-label, "
        "set-label-key, "
        "rm-label-key, "
        "prime-osd-dir, "
        "bluefs-log-dump, "
        "free-dump, "
        "free-score, "
        "free-fragmentation, "
        "bluefs-stats, "
        "reshard, "
        "show-sharding")
    ;
  po::options_description po_all("All options");
  po_all.add(po_options).add(po_positional);

  vector<string> ceph_option_strings;
  po::variables_map vm;
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(po_all).allow_unregistered().run();
    po::store( parsed, vm);
    po::notify(vm);
    ceph_option_strings = po::collect_unrecognized(parsed.options,
						   po::include_positional);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    exit(EXIT_FAILURE);
  }
  // normalize path (remove ending '/' if any)
  if (path.size() > 1 && *(path.end() - 1) == '/') {
    path.resize(path.size() - 1);
  }
  if (vm.count("help")) {
    usage(po_all);
    exit(EXIT_SUCCESS);
  }

  vector<const char*> args;
  if (log_file.size()) {
    args.push_back("--log-file");
    args.push_back(log_file.c_str());
    static char ll[10];
    snprintf(ll, sizeof(ll), "%d", log_level);
    args.push_back("--debug-bdev");
    args.push_back(ll);
    args.push_back("--debug-bluestore");
    args.push_back(ll);
    args.push_back("--debug-bluefs");
    args.push_back(ll);
    args.push_back("--debug-rocksdb");
    args.push_back(ll);
  } else {
    // do not write to default-named log "osd.x.log" if --log-file is not provided
    if (!osd_instance.empty()) {
      args.push_back("--no-log-to-file");
    }
  }

  if (!osd_instance.empty()) {
    args.push_back("-i");
    args.push_back(osd_instance.c_str());
  }
  args.push_back("--no-log-to-stderr");
  args.push_back("--err-to-stderr");

  for (auto& i : ceph_option_strings) {
    args.push_back(i.c_str());
  }
  auto cct = global_init(NULL, args, osd_instance.empty() ? CEPH_ENTITY_TYPE_CLIENT : CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 osd_instance.empty() ? CINIT_FLAG_NO_DEFAULT_CONFIG_FILE : 0);

  common_init_finish(cct.get());
  if (action.empty()) {
    // if action ("command") is not yet defined try to use first param as action
    if (args.size() > 0) {
      if (args.size() == 1) {
	// treat first unparsed value as action
	action = args[0];
      } else {
	std::cerr << "Unknown options: " << args << std::endl;
	exit(EXIT_FAILURE);
      }
    }
  } else {
    if (args.size() != 0) {
      std::cerr << "Unknown options: " << args << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  if (action.empty()) {
    cerr << "must specify an action; --help for help" << std::endl;
    exit(EXIT_FAILURE);
  }

  if (!osd_instance.empty()) {
    // when "-i" is provided "osd data" can be used as path
    if (path.size() == 0) {
      path = cct->_conf.get_val<std::string>("osd_data");
    }
  }

  if (action == "fsck" || action == "repair" || action == "quick-fix" || action == "allocmap" || action == "qfsck" || action == "restore_cfb") {
    if (path.empty()) {
      cerr << "must specify bluestore path" << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  if (action == "prime-osd-dir") {
    if (devs.size() != 1) {
      cerr << "must specify the main bluestore device" << std::endl;
      exit(EXIT_FAILURE);
    }
    if (path.empty()) {
      cerr << "must specify osd dir to prime" << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  if (action == "set-label-key" ||
      action == "rm-label-key") {
    if (devs.size() != 1) {
      cerr << "must specify the main bluestore device" << std::endl;
      exit(EXIT_FAILURE);
    }
    if (key.size() == 0) {
      cerr << "must specify a key name with -k" << std::endl;
      exit(EXIT_FAILURE);
    }
    if (action == "set-label-key" && value.size() == 0) {
      cerr << "must specify a value with -v" << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  if (action == "show-label") {
    if (devs.empty() && path.empty()) {
      cerr << "must specify bluestore path *or* raw device(s)" << std::endl;
      exit(EXIT_FAILURE);
    }
    if (devs.empty())
      inferring_bluefs_devices(devs, path);
  }
  if (action == "bluefs-export" || 
      action == "bluefs-import" || 
      action == "bluefs-log-dump") {
    if (path.empty()) {
      cerr << "must specify bluestore path" << std::endl;
      exit(EXIT_FAILURE);
    }
    if ((action == "bluefs-export") && out_dir.empty()) {
      cerr << "must specify out-dir to export bluefs" << std::endl;
      exit(EXIT_FAILURE);
    }
    if (action == "bluefs-import" && input_file.empty()) {
      cerr << "must specify input_file to import bluefs" << std::endl;
      exit(EXIT_FAILURE);
    }
    if (action == "bluefs-import" && dest_file.empty()) {
      cerr << "must specify dest_file to import bluefs" << std::endl;
      exit(EXIT_FAILURE);
    }
    inferring_bluefs_devices(devs, path);
  }
  if (action == "bluefs-bdev-sizes" || action == "bluefs-bdev-expand") {
    if (path.empty()) {
      cerr << "must specify bluestore path" << std::endl;
      exit(EXIT_FAILURE);
    }
    inferring_bluefs_devices(devs, path);
  }
  if (action == "bluefs-bdev-new-db" || action == "bluefs-bdev-new-wal") {
    if (path.empty()) {
      cerr << "must specify bluestore path" << std::endl;
      exit(EXIT_FAILURE);
    }
    if (dev_target.empty()) {
      cout << "NOTICE: --dev-target option omitted, will allocate as a file" << std::endl;
    }
    inferring_bluefs_devices(devs, path);
  }
  if (action == "bluefs-bdev-migrate") {
    if (path.empty()) {
      cerr << "must specify bluestore path" << std::endl;
      exit(EXIT_FAILURE);
    }
    inferring_bluefs_devices(devs, path);
    if (devs_source.size() == 0) {
      cerr << "must specify source devices with --devs-source" << std::endl;
      exit(EXIT_FAILURE);
    }
    if (dev_target.empty()) {
      cerr << "must specify target device with --dev-target" << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  if (action == "free-score" || action == "free-dump" || action == "free-fragmentation") {
    if (path.empty()) {
      cerr << "must specify bluestore path" << std::endl;
      exit(EXIT_FAILURE);
    }
    for (auto name : allocs_name) {
      if (!name.empty() &&
          name != "block" &&
          name != "bluefs-db" &&
          name != "bluefs-wal") {
        cerr << "unknown allocator '" << name << "'" << std::endl;
        exit(EXIT_FAILURE);
      }
    }
    if (allocs_name.empty())
      allocs_name = vector<string>{"block", "bluefs-db", "bluefs-wal"};
  }
  if (action == "reshard") {
    if (path.empty()) {
      cerr << "must specify bluestore path" << std::endl;
      exit(EXIT_FAILURE);
    }
    if (new_sharding == empty_sharding) {
      cerr << "must provide reshard specification" << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  if (action == "restore_cfb") {
#ifndef CEPH_BLUESTORE_TOOL_RESTORE_ALLOCATION
    cerr << action << " bluestore.restore_cfb is not supported!!! " << std::endl;
    exit(EXIT_FAILURE);
#else
    cout << action << " bluestore.restore_cfb" << std::endl;
    validate_path(cct.get(), path, false);
    BlueStore bluestore(cct.get(), path);
    int r = bluestore.push_allocation_to_rocksdb();
    if (r < 0) {
      cerr << action << " failed: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    } else {
      cout << action << " success" << std::endl;
    }
#endif
  }
  else if (action == "allocmap") {
#ifdef CEPH_BLUESTORE_TOOL_DISABLE_ALLOCMAP
    cerr << action << " bluestore.allocmap is not supported!!! " << std::endl;
    exit(EXIT_FAILURE);
#else
    cout << action << " bluestore.allocmap" << std::endl;
    validate_path(cct.get(), path, false);
    BlueStore bluestore(cct.get(), path);
    int r = bluestore.read_allocation_from_drive_for_bluestore_tool();
    if (r < 0) {
      cerr << action << " failed: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    } else {
      cout << action << " success" << std::endl;
    }
#endif
  }
  else if( action == "qfsck" ) {
#ifndef CEPH_BLUESTORE_TOOL_RESTORE_ALLOCATION
    cerr << action << " bluestore.qfsck is not supported!!! " << std::endl;
    exit(EXIT_FAILURE);
#else
    cout << action << " bluestore.quick-fsck" << std::endl;
    validate_path(cct.get(), path, false);
    BlueStore bluestore(cct.get(), path);
    int r = bluestore.read_allocation_from_drive_for_bluestore_tool();
    if (r < 0) {
      cerr << action << " failed: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    } else {
      cout << action << " success" << std::endl;
    }
#endif
  }
  else if (action == "fsck" ||
      action == "repair" ||
      action == "quick-fix") {
    validate_path(cct.get(), path, false);
    BlueStore bluestore(cct.get(), path);
    int r;
    if (action == "fsck") {
      r = bluestore.fsck(fsck_deep);
    } else if (action == "repair") {
      r = bluestore.repair(fsck_deep);
    } else {
      r = bluestore.quick_fix();
    }
    if (r < 0) {
      cerr << action << " failed: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    } else if (r > 0) {
      cerr << action << " status: remaining " << r << " error(s) and warning(s)" << std::endl;
      exit(EXIT_FAILURE);
    } else {
      cout << action << " success" << std::endl;
    }
  }
  else if (action == "prime-osd-dir") {
    bluestore_bdev_label_t label;
    int r = BlueStore::_read_bdev_label(cct.get(), devs.front(), &label);
    if (r < 0) {
      cerr << "failed to read label for " << devs.front() << ": "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }

    // kludge some things into the map that we want to populate into
    // target dir
    label.meta["path_block"] = devs.front();
    label.meta["type"] = "bluestore";
    label.meta["fsid"] = stringify(label.osd_uuid);
    
    for (auto kk : {
	"whoami",
	  "osd_key",
	  "ceph_fsid",
	  "fsid",
	  "type",
	  "ready" }) {
      string k = kk;
      auto i = label.meta.find(k);
      if (i == label.meta.end()) {
	continue;
      }
      string p = path + "/" + k;
      string v = i->second;
      if (k == "osd_key") {
	p = path + "/keyring";
	v = "[osd.";
	v += label.meta["whoami"];
	v += "]\nkey = " + i->second;
      }
      v += "\n";
      int fd = ::open(p.c_str(), O_CREAT|O_TRUNC|O_WRONLY|O_CLOEXEC, 0600);
      if (fd < 0) {
	cerr << "error writing " << p << ": " << cpp_strerror(errno)
	     << std::endl;
	exit(EXIT_FAILURE);
      }
      int r = safe_write(fd, v.c_str(), v.size());
      if (r < 0) {
	cerr << "error writing to " << p << ": " << cpp_strerror(errno)
	     << std::endl;
	exit(EXIT_FAILURE);
      }
      ::close(fd);
    }
  }
  else if (action == "show-label") {
    JSONFormatter jf(true);
    jf.open_object_section("devices");
    for (auto& i : devs) {
      bluestore_bdev_label_t label;
      int r = BlueStore::_read_bdev_label(cct.get(), i, &label);
      if (r < 0) {
	cerr << "unable to read label for " << i << ": "
	     << cpp_strerror(r) << std::endl;
	exit(EXIT_FAILURE);
      }
      jf.open_object_section(i.c_str());
      label.dump(&jf);
      jf.close_section();
    }
    jf.close_section();
    jf.flush(cout);
  }
  else if (action == "set-label-key") {
    bluestore_bdev_label_t label;
    int r = BlueStore::_read_bdev_label(cct.get(), devs.front(), &label);
    if (r < 0) {
      cerr << "unable to read label for " << devs.front() << ": "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
    if (key == "size") {
      label.size = strtoull(value.c_str(), nullptr, 10);
    } else if (key =="osd_uuid") {
      label.osd_uuid.parse(value.c_str());
    } else if (key =="btime") {
      uint64_t epoch;
      uint64_t nsec;
      int r = utime_t::parse_date(value.c_str(), &epoch, &nsec);
      if (r == 0) {
	label.btime = utime_t(epoch, nsec);
      }
    } else if (key =="description") {
      label.description = value;
    } else {
      label.meta[key] = value;
    }
    r = BlueStore::_write_bdev_label(cct.get(), devs.front(), label);
    if (r < 0) {
      cerr << "unable to write label for " << devs.front() << ": "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  else if (action == "rm-label-key") {
    bluestore_bdev_label_t label;
    int r = BlueStore::_read_bdev_label(cct.get(), devs.front(), &label);
    if (r < 0) {
      cerr << "unable to read label for " << devs.front() << ": "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
    if (!label.meta.count(key)) {
      cerr << "key '" << key << "' not present" << std::endl;
      exit(EXIT_FAILURE);
    }
    label.meta.erase(key);
    r = BlueStore::_write_bdev_label(cct.get(), devs.front(), label);
    if (r < 0) {
      cerr << "unable to write label for " << devs.front() << ": "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  else if (action == "bluefs-bdev-sizes") {
    BlueStore bluestore(cct.get(), path);
    bluestore.dump_bluefs_sizes(cout);
  }
  else if (action == "bluefs-bdev-expand") {
    BlueStore bluestore(cct.get(), path);
    auto r = bluestore.expand_devices(cout);
    if (r <0) {
      cerr << "failed to expand bluestore devices: "
	   << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  else if (action == "bluefs-import") {
    bluefs_import(input_file, dest_file, cct.get(), path, devs);
  }
  else if (action == "bluefs-export") {
    BlueFS *fs = open_bluefs_readonly(cct.get(), path, devs);

    vector<string> dirs;
    int r = fs->readdir("", &dirs);
    if (r < 0) {
      cerr << "readdir in root failed: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }

    if (::access(out_dir.c_str(), F_OK)) {
      r = ::mkdir(out_dir.c_str(), 0755);
      if (r < 0) {
        r = -errno;
        cerr << "mkdir " << out_dir << " failed: " << cpp_strerror(r) << std::endl;
        exit(EXIT_FAILURE);
      }
    }

    for (auto& dir : dirs) {
      if (dir[0] == '.')
	continue;
      cout << dir << "/" << std::endl;
      vector<string> ls;
      r = fs->readdir(dir, &ls);
      if (r < 0) {
	cerr << "readdir " << dir << " failed: " << cpp_strerror(r) << std::endl;
	exit(EXIT_FAILURE);
      }
      string full = out_dir + "/" + dir;
      if (::access(full.c_str(), F_OK)) {
        r = ::mkdir(full.c_str(), 0755);
        if (r < 0) {
          r = -errno;
          cerr << "mkdir " << full << " failed: " << cpp_strerror(r) << std::endl;
          exit(EXIT_FAILURE);
        }
      }
      for (auto& file : ls) {
	if (file[0] == '.')
	  continue;
	cout << dir << "/" << file << std::endl;
	uint64_t size;
	utime_t mtime;
	r = fs->stat(dir, file, &size, &mtime);
	if (r < 0) {
	  cerr << "stat " << file << " failed: " << cpp_strerror(r) << std::endl;
	  exit(EXIT_FAILURE);
	}
	string path = out_dir + "/" + dir + "/" + file;
	int fd = ::open(path.c_str(), O_CREAT|O_WRONLY|O_TRUNC|O_CLOEXEC, 0644);
	if (fd < 0) {
	  r = -errno;
	  cerr << "open " << path << " failed: " << cpp_strerror(r) << std::endl;
	  exit(EXIT_FAILURE);
	}
	if (size > 0) {
	  BlueFS::FileReader *h;
	  r = fs->open_for_read(dir, file, &h, false);
	  if (r < 0) {
	    cerr << "open_for_read " << dir << "/" << file << " failed: "
		 << cpp_strerror(r) << std::endl;
	    exit(EXIT_FAILURE);
	  }
	  int pos = 0;
	  int left = size;
	  while (left) {
	    bufferlist bl;
	    r = fs->read(h, pos, left, &bl, NULL);
	    if (r <= 0) {
	      cerr << "read " << dir << "/" << file << " from " << pos
		   << " failed: " << cpp_strerror(r) << std::endl;
	      exit(EXIT_FAILURE);
	    }
	    int rc = bl.write_fd(fd);
	    if (rc < 0) {
	      cerr << "write to " << path << " failed: "
		   << cpp_strerror(r) << std::endl;
	      exit(EXIT_FAILURE);
	    }
	    pos += r;
	    left -= r;
	  }
	  delete h;
	}
	::close(fd);
      }
    }
    fs->umount();
    delete fs;
  } else if (action == "bluefs-log-dump") {
    log_dump(cct.get(), path, devs);
  } else if (action == "bluefs-bdev-new-db" || action == "bluefs-bdev-new-wal") {
    map<string, int> cur_devs_map;
    bool need_db = action == "bluefs-bdev-new-db";

    bool has_wal = false;
    bool has_db = false;

    parse_devices(cct.get(), devs, &cur_devs_map, &has_db, &has_wal);

    if (has_db && has_wal) {
      cerr << "can't allocate new device, both WAL and DB exist"
	    << std::endl;
      exit(EXIT_FAILURE);
    } else if (need_db && has_db) {
      cerr << "can't allocate new DB device, already exists"
	    << std::endl;
      exit(EXIT_FAILURE);
    } else if (!need_db && has_wal) {
      cerr << "can't allocate new WAL device, already exists"
	    << std::endl;
      exit(EXIT_FAILURE);
    }

    auto [target_path, has_size_spec] =
      [&dev_target]() -> std::pair<string, bool> {
      if (dev_target.empty()) {
	return {"", false};
      }
      std::error_code ec;
      fs::path target = fs::weakly_canonical(fs::path{dev_target}, ec);
      if (ec) {
	cerr << "failed to retrieve absolute path for " << dev_target
	     << ": " << ec.message()
	     << std::endl;
	exit(EXIT_FAILURE);
      }
      return {target.native(),
              fs::exists(target) &&
               (fs::is_block_file(target) ||
	         (fs::is_regular_file(target) && fs::file_size(target) > 0))};
    }();
    // Attach either DB or WAL volume, create if needed
    // check if we need additional size specification
    if (!has_size_spec) {
      if (need_db && cct->_conf->bluestore_block_db_size == 0) {
	cerr << "Might need DB size specification, "
		"please set Ceph bluestore-block-db-size config parameter "
	     << std::endl;
	return EXIT_FAILURE;
      } else if (!need_db && cct->_conf->bluestore_block_wal_size == 0) {
	cerr << "Might need WAL size specification, "
		"please set Ceph bluestore-block-wal-size config parameter "
	     << std::endl;
	return EXIT_FAILURE;
      }
    }
    BlueStore bluestore(cct.get(), path);
    int r = bluestore.add_new_bluefs_device(
      need_db ? BlueFS::BDEV_NEWDB : BlueFS::BDEV_NEWWAL,
      target_path);
    if (r == 0) {
      cout << (need_db ? "DB" : "WAL") << " device added " << target_path
	   << std::endl;
    } else {
      cerr << "failed to add " << (need_db ? "DB" : "WAL") << " device:"
	   << cpp_strerror(r)
	   << std::endl;
    }
    return r;
  } else if (action == "bluefs-bdev-migrate") {
    map<string, int> cur_devs_map;
    set<int> src_dev_ids;
    map<string, int> src_devs;

    parse_devices(cct.get(), devs, &cur_devs_map, nullptr, nullptr);
    for (auto& s :  devs_source) {
      auto i = cur_devs_map.find(s);
      if (i != cur_devs_map.end()) {
        if (s == dev_target) {
	  cerr << "Device " << dev_target
	       << " is present in both source and target lists, omitted."
	       << std::endl;
        } else {
	  src_devs.emplace(*i);
	  src_dev_ids.emplace(i->second);
	}
      } else {
	cerr << "can't migrate " << s << ", not a valid bluefs volume "
	      << std::endl;
	exit(EXIT_FAILURE);
      }
    }

    auto i = cur_devs_map.find(dev_target);

    if (i != cur_devs_map.end()) {
      // Migrate to an existing BlueFS volume

      auto dev_target_id = i->second;
      if (dev_target_id == BlueFS::BDEV_WAL) {
	// currently we're unable to migrate to WAL device since there is no space
	// reserved for superblock
	cerr << "Migrate to WAL device isn't supported." << std::endl;
	exit(EXIT_FAILURE);
      }

      BlueStore bluestore(cct.get(), path);
      int r = bluestore.migrate_to_existing_bluefs_device(
	src_dev_ids,
	dev_target_id);
      if (r == 0) {
	for(auto src : src_devs) {
	  if (src.second != BlueFS::BDEV_SLOW) {
	    cout << " device removed:" << src.second << " " << src.first
		 << std::endl;
	  }
	}
      } else {
        bool need_db = dev_target_id == BlueFS::BDEV_DB;
	cerr << "failed to migrate to existing BlueFS device: "
	     << (need_db ? BlueFS::BDEV_DB : BlueFS::BDEV_WAL)
	     << " " << dev_target
	     << cpp_strerror(r)
	     << std::endl;
      }
      return r;
    } else {
      // Migrate to a new BlueFS volume
      // via creating either DB or WAL volume
      char target_path[PATH_MAX] = "";
      int dev_target_id;
      if (src_dev_ids.count(BlueFS::BDEV_DB)) {
	// if we have DB device in the source list - we create DB device
	// (and may be remove WAL).
	dev_target_id = BlueFS::BDEV_NEWDB;
      } else if (src_dev_ids.count(BlueFS::BDEV_WAL)) {
	dev_target_id = BlueFS::BDEV_NEWWAL;
      } else {
        cerr << "Unable to migrate Slow volume to new location, "
	        "please allocate new DB or WAL with "
		"--bluefs-bdev-new-db(wal) command"
	     << std::endl;
	exit(EXIT_FAILURE);
      }
      if(!dev_target.empty() &&
	        realpath(dev_target.c_str(), target_path) == nullptr) {
	cerr << "failed to retrieve absolute path for " << dev_target
	     << ": " << cpp_strerror(errno)
	     << std::endl;
	exit(EXIT_FAILURE);
      }

      BlueStore bluestore(cct.get(), path);

      bool need_db = dev_target_id == BlueFS::BDEV_NEWDB;
      int r = bluestore.migrate_to_new_bluefs_device(
	src_dev_ids,
	dev_target_id,
	target_path);
      if (r == 0) {
	for(auto src : src_devs) {
	  if (src.second != BlueFS::BDEV_SLOW) {
	    cout << " device removed:" << src.second << " " << src.first
		 << std::endl;
	  }
	}
	cout << " device added: "
	     << (need_db ? BlueFS::BDEV_DB : BlueFS::BDEV_DB)
	     << " " << target_path
	     << std::endl;
      } else {
	cerr << "failed to migrate to new BlueFS device: "
	     << (need_db ? BlueFS::BDEV_DB : BlueFS::BDEV_DB)
	     << " " << target_path
	     << cpp_strerror(r)
	     << std::endl;
      }
      return r;
    }
  } else  if (action == "free-dump" || action == "free-score" || action == "fragmentation") {
    AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
    ceph_assert(admin_socket);
    std::string action_name = action == "free-dump" ? "dump" :
                              action == "free-score" ? "score" : "fragmentation";
    validate_path(cct.get(), path, false);
    BlueStore bluestore(cct.get(), path);
    int r = bluestore.cold_open();
    if (r < 0) {
      cerr << "error from cold_open: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }

    for (auto alloc_name : allocs_name) {
      ceph::bufferlist in, out;
      ostringstream err;
      int r = admin_socket->execute_command(
	{"{\"prefix\": \"bluestore allocator " + action_name + " " + alloc_name + "\"}"},
	in, err, &out);
      if (r != 0) {
        cerr << "failure querying '" << alloc_name << "'" << std::endl;
      } else {
        cout << alloc_name << ":" << std::endl;
        cout << std::string(out.c_str(),out.length()) << std::endl;
      }
    }

    bluestore.cold_close();
  } else  if (action == "bluefs-stats") {
    AdminSocket* admin_socket = g_ceph_context->get_admin_socket();
    ceph_assert(admin_socket);
    validate_path(cct.get(), path, false);

    // make sure we can adjust any config settings
    g_conf()._clear_safe_to_start_threads();
    g_conf().set_val_or_die("bluestore_volume_selection_policy",
                            "use_some_extra_enforced");
    BlueStore bluestore(cct.get(), path);
    int r = bluestore.cold_open();
    if (r < 0) {
      cerr << "error from cold_open: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }

    ceph::bufferlist in, out;
    ostringstream err;
    r = admin_socket->execute_command(
      { "{\"prefix\": \"bluefs stats\"}" },
      in, err, &out);
    if (r != 0) {
      cerr << "failure querying bluefs stats: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
    cout << std::string(out.c_str(), out.length()) << std::endl;
     bluestore.cold_close();
  } else if (action == "reshard") {
    auto get_ctrl = [&](size_t& val) {
      if (!resharding_ctrl.empty()) {
	size_t pos;
	std::string token;
	pos = resharding_ctrl.find('/');
	token = resharding_ctrl.substr(0, pos);
	if (pos != std::string::npos)
	  resharding_ctrl.erase(0, pos + 1);
	else
	  resharding_ctrl.erase();
	char* endptr;
	val = strtoll(token.c_str(), &endptr, 0);
	if (*endptr != '\0') {
	  cerr << "invalid --resharding-ctrl. '" << token << "' is not a number" << std::endl;
	  exit(EXIT_FAILURE);
	}
      }
    };
    BlueStore bluestore(cct.get(), path);
    KeyValueDB *db_ptr;
    RocksDBStore::resharding_ctrl ctrl;
    if (!resharding_ctrl.empty()) {
      get_ctrl(ctrl.bytes_per_iterator);
      get_ctrl(ctrl.keys_per_iterator);
      get_ctrl(ctrl.bytes_per_batch);
      get_ctrl(ctrl.keys_per_batch);
      if (!resharding_ctrl.empty()) {
	cerr << "extra chars in --resharding-ctrl" << std::endl;
	exit(EXIT_FAILURE);
      }
    }
    int r = bluestore.open_db_environment(&db_ptr, true);
    if (r < 0) {
      cerr << "error preparing db environment: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
    ceph_assert(db_ptr);
    RocksDBStore* rocks_db = dynamic_cast<RocksDBStore*>(db_ptr);
    ceph_assert(rocks_db);
    r = rocks_db->reshard(new_sharding, &ctrl);
    if (r < 0) {
      cerr << "error resharding: " << cpp_strerror(r) << std::endl;
    } else {
      cout << "reshard success" << std::endl;
    }
    bluestore.close_db_environment();
  } else if (action == "show-sharding") {
    BlueStore bluestore(cct.get(), path);
    KeyValueDB *db_ptr;
    int r = bluestore.open_db_environment(&db_ptr, false);
    if (r < 0) {
      cerr << "error preparing db environment: " << cpp_strerror(r) << std::endl;
      exit(EXIT_FAILURE);
    }
    ceph_assert(db_ptr);
    RocksDBStore* rocks_db = dynamic_cast<RocksDBStore*>(db_ptr);
    ceph_assert(rocks_db);
    std::string sharding;
    bool res = rocks_db->get_sharding(sharding);
    bluestore.close_db_environment();
    if (!res) {
      cerr << "failed to retrieve sharding def" << std::endl;
      exit(EXIT_FAILURE);
    }
    cout << sharding << std::endl;
  } else {
    cerr << "unrecognized action " << action << std::endl;
    return 1;
  }

  return 0;
}
