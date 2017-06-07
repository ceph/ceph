// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"

#include "os/bluestore/BlueFS.h"
#include "os/bluestore/BlueStore.h"

namespace po = boost::program_options;

void usage(po::options_description &desc)
{
  cout << desc << std::endl;
}

int main(int argc, char **argv)
{
  string out_dir;
  vector<string> devs;
  string path;
  string action;
  bool fsck_deep = false;
  po::options_description po_options("Options");
  po_options.add_options()
    ("help,h", "produce help message")
    ("path", po::value<string>(&path), "bluestore path")
    ("out-dir", po::value<string>(&out_dir), "output directory")
    ("dev", po::value<vector<string>>(&devs), "device(s)")
    ("deep", po::value<bool>(&fsck_deep), "deep fsck (read all data)")
    ;
  po::options_description po_positional("Positional options");
  po_positional.add_options()
    ("command", po::value<string>(&action), "fsck, bluefs-export, show-label")
    ;
  po::options_description po_all("All options");
  po_all.add(po_options).add(po_positional);
  po::positional_options_description pd;
  pd.add("command", 1);

  vector<string> ceph_option_strings;
  po::variables_map vm;
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(po_all).allow_unregistered().positional(pd).run();
    po::store( parsed, vm);
    po::notify(vm);
    ceph_option_strings = po::collect_unrecognized(parsed.options,
						   po::include_positional);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }

  if (vm.count("help")) {
    usage(po_all);
    return 1;
  }
  if (action.empty()) {
    cerr << "must specify an action; --help for help" << std::endl;
    return 1;
  }

  if (action == "fsck") {
    if (path.empty()) {
      cerr << "must specify bluestore path" << std::endl;
      exit(1);
    }
  }
  if (action == "bluefs-export" ||
      action == "show-label") {
    if (devs.empty() && path.empty()) {
      cerr << "must specify bluestore path *or* raw device(s)" << std::endl;
      exit(1);
    }
    cout << "infering bluefs devices from bluestore path" << std::endl;
    for (auto fn : {"block", "block.wal", "block.db"}) {
      string p = path + "/" + fn;
      struct stat st;
      if (::stat(p.c_str(), &st) == 0) {
	devs.push_back(p);
      }
    }
  }

  vector<const char*> args;
  for (auto& i : ceph_option_strings) {
    args.push_back(i.c_str());
  }
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  cout << "action " << action << std::endl;

  if (action == "fsck" ||
      action == "fsck-deep") {
    BlueStore bluestore(cct.get(), path);
    int r = bluestore.fsck(fsck_deep);
    if (r < 0) {
      cerr << "error from fsck: " << cpp_strerror(r) << std::endl;
      return 1;
    }
  }
  else if (action == "show-label") {
    JSONFormatter jf(true);
    jf.open_array_section("devices");
    for (auto& i : devs) {
      bluestore_bdev_label_t label;
      int r = BlueStore::_read_bdev_label(cct.get(), i, &label);
      if (r < 0) {
	cerr << "unable to read label for " << i << ": "
	     << cpp_strerror(r) << std::endl;
	exit(1);
      }
      jf.open_object_section(i.c_str());
      label.dump(&jf);
      jf.close_section();
    }
    jf.close_section();
    jf.flush(cout);
  }
  else if (action == "bluefs-export") {
    if (out_dir.empty()) {
      cerr << "must specify out-dir to export bluefs" << std::endl;
      exit(1);
    }
    BlueFS fs(&(*cct));
    string main;
    set<int> got;
    for (auto& i : devs) {
      bluestore_bdev_label_t label;
      int r = BlueStore::_read_bdev_label(cct.get(), i, &label);
      if (r < 0) {
	cerr << "unable to read label for " << i << ": "
	     << cpp_strerror(r) << std::endl;
	exit(1);
      }
      int id = -1;
      if (label.description == "main")
	main = i;
      else if (label.description == "bluefs db")
	id = BlueFS::BDEV_DB;
      else if (label.description == "bluefs wal")
	id = BlueFS::BDEV_WAL;
      if (id >= 0) {
	got.insert(id);
	cout << " slot " << id << " " << i << std::endl;
	int r = fs.add_block_device(id, i);
	if (r < 0) {
	  cerr << "unable to open " << i << ": " << cpp_strerror(r) << std::endl;
	  exit(1);
	}
      }
    }
    if (main.length()) {
      int id = BlueFS::BDEV_DB;
      if (got.count(BlueFS::BDEV_DB))
	id = BlueFS::BDEV_SLOW;
      cout << " slot " << id << " " << main << std::endl;
      int r = fs.add_block_device(id, main);
      if (r < 0) {
	cerr << "unable to open " << main << ": " << cpp_strerror(r)
	     << std::endl;
	exit(1);
      }
    }

    int r = fs.mount();
    if (r < 0) {
      cerr << "unable to mount bluefs: " << cpp_strerror(r)
	   << std::endl;
      exit(1);
    }

    vector<string> dirs;
    r = fs.readdir("", &dirs);
    if (r < 0) {
      cerr << "readdir in root failed: " << cpp_strerror(r) << std::endl;
      exit(1);
    }
    for (auto& dir : dirs) {
      if (dir[0] == '.')
	continue;
      cout << dir << "/" << std::endl;
      vector<string> ls;
      r = fs.readdir(dir, &ls);
      if (r < 0) {
	cerr << "readdir " << dir << " failed: " << cpp_strerror(r) << std::endl;
	exit(1);
      }
      string full = out_dir + "/" + dir;
      r = ::mkdir(full.c_str(), 0755);
      if (r < 0) {
	cerr << "mkdir " << full << " failed: " << cpp_strerror(r) << std::endl;
	exit(1);
      }
      for (auto& file : ls) {
	if (file[0] == '.')
	  continue;
	cout << dir << "/" << file << std::endl;
	uint64_t size;
	utime_t mtime;
	r = fs.stat(dir, file, &size, &mtime);
	if (r < 0) {
	  cerr << "stat " << file << " failed: " << cpp_strerror(r) << std::endl;
	  exit(1);
	}
	string path = out_dir + "/" + dir + "/" + file;
	int fd = ::open(path.c_str(), O_CREAT|O_WRONLY|O_TRUNC, 0644);
	if (fd < 0) {
	  r = -errno;
	  cerr << "open " << path << " failed: " << cpp_strerror(r) << std::endl;
	  exit(1);
	}
	assert(fd >= 0);
	if (size > 0) {
	  BlueFS::FileReader *h;
	  r = fs.open_for_read(dir, file, &h, false);
	  if (r < 0) {
	    cerr << "open_for_read " << dir << "/" << file << " failed: "
		 << cpp_strerror(r) << std::endl;
	    exit(1);
	  }
	  int pos = 0;
	  int left = size;
	  while (left) {
	    bufferlist bl;
	    r = fs.read(h, &h->buf, pos, left, &bl, NULL);
	    if (r <= 0) {
	      cerr << "read " << dir << "/" << file << " from " << pos
		   << " failed: " << cpp_strerror(r) << std::endl;
	      exit(1);
	    }
	    int rc = bl.write_fd(fd);
	    if (rc < 0) {
	      cerr << "write to " << path << " failed: "
		   << cpp_strerror(r) << std::endl;
	      exit(1);
	    }
	    pos += r;
	    left -= r;
	  }
	  delete h;
	}
	::close(fd);
      }
    }
    fs.umount();
  } else {
    cerr << "unrecognized action " << action << std::endl;
    return 1;
  }

  return 0;
}
