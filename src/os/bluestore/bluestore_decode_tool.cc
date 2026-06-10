// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "common/JSONFormatter.h"
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
    int r = BlueStore::read_bdev_label(cct, i, &label);
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
    int r = BlueStore::read_bdev_label(cct, d, &label);
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

void super_dump(
  CephContext *cct,
  const string& path,
  const vector<string>& devs)
{
  validate_path(cct, path, true);
  BlueFS *fs = new BlueFS(cct);

  add_devices(fs, cct, devs);
  int r = fs->super_dump();
  if (r < 0) {
    cerr << "super_dump failed" << ": "
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


const uint8_t bindata[626] = {
/*000*/0x02,0x16,0x02,0xc3,0x03,0x07,0x01,0xa6,
/*008*/0x3d,0x0d,0xda,0x01,0x07,0x04,0x04,0x0c,
/*010*/0x04,0x7a,0x35,0xaa,0x45,0x05,0x07,0x02,
/*018*/0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
/*020*/0xff,0x01,0x07,0x26,0x83,0x0a,0xda,0x01,
/*028*/0x07,0x04,0x04,0x0c,0x08,0x00,0x00,0x00,
/*030*/0x00,0x1b,0xd4,0x46,0x97,0x05,0x0b,0x02,
/*038*/0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
/*040*/0xff,0x01,0x0b,0xa8,0xc3,0x11,0xda,0x01,
/*048*/0x07,0x04,0x04,0x0c,0x0c,0x00,0x00,0x00,
/*050*/0x00,0x00,0x00,0x00,0x00,0x9a,0xe1,0x6d,
/*058*/0x45,0x05,0x0f,0x02,0xff,0xff,0xff,0xff,
/*060*/0xff,0xff,0xff,0xff,0xff,0x01,0x0f,0x44,
/*068*/0x9e,0x16,0xda,0x01,0x07,0x04,0x04,0x0c,
/*070*/0x10,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*078*/0x00,0x00,0x00,0x00,0x00,0x1e,0x36,0x2d,
/*080*/0x06,0x05,0x13,0x02,0xff,0xff,0xff,0xff,
/*088*/0xff,0xff,0xff,0xff,0xff,0x01,0x13,0x85,
/*090*/0x1d,0x40,0x1b,0x07,0x04,0x04,0x0c,0x14,
/*098*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*0a0*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*0a8*/0x07,0x27,0xdc,0x5c,0x05,0x17,0x02,0xff,
/*0b0*/0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
/*0b8*/0x01,0x17,0xba,0x47,0x34,0xda,0x01,0x07,
/*0c0*/0x04,0x04,0x0c,0x18,0x00,0x00,0x00,0x00,
/*0c8*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*0d0*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*0d8*/0x5e,0xa3,0x41,0xa1,0x07,0x01,0x9c,0xc6,
/*0e0*/0x06,0xda,0x01,0x07,0x04,0x04,0x0c,0x04,
/*0e8*/0xc9,0x92,0x4a,0x6f,0x07,0x01,0xb2,0x6d,
/*0f0*/0x0b,0xda,0x01,0x07,0x04,0x04,0x0c,0x04,
/*0f8*/0x34,0x66,0x85,0x0f,0x01,0x23,0x0b,0x01,
/*100*/0x86,0x05,0x09,0xda,0x01,0x07,0x06,0x43,
/*108*/0x8c,0x1b,0x04,0x0c,0x04,0xd4,0x13,0x80,
/*110*/0x3e,0x01,0x0f,0x07,0x02,0xff,0xff,0xff,
/*118*/0xff,0xff,0xff,0xff,0xff,0xff,0x01,0x0f,
/*120*/0x6a,0x89,0x11,0xda,0x01,0x07,0x04,0x04,
/*128*/0x0c,0x10,0x00,0x00,0x00,0x00,0x00,0x00,
/*130*/0x00,0x00,0x00,0x00,0x00,0x00,0xe1,0xca,
/*138*/0x33,0x21,0x05,0x13,0x02,0xff,0xff,0xff,
/*140*/0xff,0xff,0xff,0xff,0xff,0xff,0x01,0x13,
/*148*/0xa4,0x97,0x12,0xda,0x01,0x07,0x04,0x04,
/*150*/0x0c,0x14,0x00,0x00,0x00,0x00,0x00,0x00,
/*158*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*160*/0x00,0x00,0xdb,0x39,0x59,0xe4,0x91,0x01,
/*168*/0x33,0x0b,0x01,0x1f,0x07,0x02,0xff,0xff,
/*170*/0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x01,
/*178*/0x1f,0x56,0x2c,0x12,0xda,0x01,0x07,0x04,
/*180*/0x04,0x0c,0x20,0x00,0x00,0x00,0x00,0x00,
/*188*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*190*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*198*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0xdc,
/*1a0*/0x59,0x21,0xa4,0x05,0x27,0x02,0xff,0xff,
/*1a8*/0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x01,
/*1b0*/0x27,0xb8,0xd1,0x01,0x09,0x07,0x04,0x04,
/*1b8*/0x0c,0x28,0x00,0x00,0x00,0x00,0x00,0x00,
/*1c0*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*1c8*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*1d0*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*1d8*/0x00,0x00,0x00,0x00,0x00,0x00,0x7d,0x60,
/*1e0*/0x37,0xc7,0x07,0x01,0x34,0xdb,0x00,0x09,
/*1e8*/0x07,0x04,0x04,0x0c,0x04,0xb4,0xbb,0x8e,
/*1f0*/0xd4,0xcd,0x61,0x07,0x05,0x0b,0x02,0xff,
/*1f8*/0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
/*200*/0x01,0x0b,0x86,0xfb,0x33,0xda,0x01,0x07,
/*208*/0x04,0x04,0x0c,0x0c,0x00,0x00,0x00,0x00,
/*210*/0x00,0x00,0x00,0x00,0xab,0x00,0xa3,0xcb,
/*218*/0x03,0x0b,0x01,0x91,0x45,0x42,0x1b,0x07,
/*220*/0x06,0x0b,0xd4,0x04,0x04,0x0c,0x04,0x2a,
/*228*/0xea,0x4d,0x2e,0x01,0x17,0x07,0x02,0xff,
/*230*/0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
/*238*/0x01,0x17,0xa6,0xfa,0x2e,0xda,0x01,0x07,
/*240*/0x04,0x04,0x0c,0x18,0x00,0x00,0x00,0x00,
/*248*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*250*/0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
/*258*/0x14,0x79,0x3f,0x9d,0xcd,0xad,0x0a,0x1b,
/*260*/0x07,0x01,0x5a,0x04,0xd7,0x49,0x07,0x04,
/*268*/0x04,0x0c,0x04,0x8d,0xa9,0x51,0x49,0xcd,
/*270*/0x61,0x23
};





class Decoder_Visual : public BlueStore::ExtentMap::ExtentDecoder {
  using Extent = BlueStore::Extent;
  using BlobRef = BlueStore::BlobRef;

  Extent extent;

protected:
  void consume_blobid(Extent *, bool spanning, uint64_t blobid) override;
  void consume_blob(Extent *le, uint64_t extent_no, uint64_t sbid, BlobRef b) override;
  void consume_spanning_blob(uint64_t sbid, BlobRef b) override;
  Extent *get_next_extent() override {
    extent = Extent();
    return &extent;
  }
  void add_extent(Extent * e) override {
    cout << std::hex << "LO=  " << e->logical_offset << std::dec << std::endl;
    cout << std::hex << "BO=  " << e->blob_offset << std::dec << std::endl;
    cout << std::hex << "LEN= " << e->length << std::dec << std::endl;
  }

public:
  Decoder_Visual() {}
};


void Decoder_Visual::consume_blobid(
  Extent* le, bool spanning, uint64_t blobid)
{
  if (spanning) {
    cout << std::hex << "SPANNING BLOB=" << blobid << std::endl;
  } else {
    cout << std::hex << "         BLOB=" << blobid << std::endl;
  }
}

void Decoder_Visual::consume_blob(
  Extent* le, uint64_t extent_no, uint64_t sbid, BlobRef b)
{
  cout << std::hex << "SHARED BLOB=" << sbid << std::endl;
}

void Decoder_Visual::consume_spanning_blob(
  uint64_t sbid, BlobRef b)
{
  cout << std::hex << "SPANNING BLOB=" << sbid << std::endl;
}










int main(int argc, char **argv)
{
  string out_dir;
  string osd_instance;
  vector<string> devs;
  vector<string> devs_source;
  string dev_target;
  string path, path_aux;
  string action, action_aux;
  string log_file;
  string input_file;
  string dest_file;
  string key, value;
  vector<string> allocs_name;
  vector<string> bdev_type;
  string empty_sharding(1, '\0');
  string new_sharding = empty_sharding;
  string resharding_ctrl;
  int log_level = 30;
  bool fsck_deep = false;
  uint64_t disk_offset;
  po::options_description po_options("Options");
  po_options.add_options()
    ("help,h", "produce help message")
    (",i", po::value<string>(&osd_instance), "OSD instance. Requires access to monitor/ceph.conf")
    ("path", po::value<string>(&path), "bluestore path")
    ("data-path", po::value<string>(&path_aux),
      "--path alias, ignored if the latter is present")
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
    ("bdev-type", po::value<vector<string>>(&bdev_type), "bdev type to inspect: 'bdev-block'/'bdev-wal'/'bdev-db'")
    ("yes-i-really-really-mean-it", "additional confirmation for dangerous commands")
    ("sharding", po::value<string>(&new_sharding), "new sharding to apply")
    ("resharding-ctrl", po::value<string>(&resharding_ctrl), "gives control over resharding procedure details")
    ("offset", po::value<uint64_t>(&disk_offset), "disk location")
    ("op", po::value<string>(&action_aux),
      "--command alias, ignored if the latter is present")
    ;
  po::options_description po_positional("Positional options");
  po_positional.add_options()
    ("command", po::value<string>(&action),
        "show"
    );
  po::options_description po_all("All options");
  po_all.add(po_options).add(po_positional);

  vector<string> ceph_option_strings;
  po::variables_map vm;
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(po_all)
        .allow_unregistered()
        .style(po::command_line_style::default_style &
               ~po::command_line_style::allow_guessing)
        .run();
    po::store( parsed, vm);
    po::notify(vm);
    ceph_option_strings = po::collect_unrecognized(parsed.options,
						   po::include_positional);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    exit(EXIT_FAILURE);
  }
  if (action != action_aux && !action.empty() && !action_aux.empty()) {
    std::cerr
      << " Ambiguous --op and --command options, please provide a single one."
      << std::endl;
    exit(EXIT_FAILURE);
  }
  if (action.empty()) {
    action.swap(action_aux);
  }
  if (!path_aux.empty()) {
    if (path.empty()) {
      path.swap(path_aux);
    } else if (path != path_aux) {
      std::cerr
	<< " Ambiguous --data-path and --path options, please provide a single one."
	<< std::endl;
      exit(EXIT_FAILURE);
    }
  };

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

  if (action == "show") {
    //BlueStore b(cct.get(), "");
    cerr << "Running" << std::endl;
    //read_alloc_stats_t stats;
    //BlueStore::Onode o(cct.get());
    Decoder_Visual dv;
    bufferlist bl;
    bl.append((char*)&bindata[0],sizeof(bindata));
    auto p = bl.front().begin_deep();
    anote::annotator an(p);
    dv.decode_some_it(an, nullptr);
    std::cout << an.get_string() << std::endl;
  } else {
    cerr << "unrecognized action " << action << std::endl;
    return 1;
  }

  return 0;
}
