// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <errno.h>
#include "ceph_ver.h"
#include "include/types.h"
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "denc_registry.h"

#define MB(m) ((m) * 1024 * 1024)

void usage(ostream &out)
{
  out << "usage: ceph-dencoder [commands ...]" << std::endl;
  out << "\n";
  out << "  version             print version string (to stdout)\n";
  out << "\n";
  out << "  import <encfile>    read encoded data from encfile\n";
  out << "  export <outfile>    write encoded data to outfile\n";
  out << "\n";
  out << "  set_features <num>  set feature bits used for encoding\n";
  out << "  get_features        print feature bits (int) to stdout\n";
  out << "\n";
  out << "  list_types          list supported types\n";
  out << "  type <classname>    select in-memory type\n";
  out << "  skip <num>          skip <num> leading bytes before decoding\n";
  out << "  decode              decode into in-memory object\n";
  out << "  encode              encode in-memory object\n";
  out << "  dump_json           dump in-memory object as json (to stdout)\n";
  out << "  hexdump             print encoded data in hex\n";
  out << "  get_struct_v        print version of the encoded object\n";
  out << "  get_struct_compat   print the oldest version of decoder that can decode the encoded object\n";
  out << "\n";
  out << "  copy                copy object (via operator=)\n";
  out << "  copy_ctor           copy object (via copy ctor)\n";
  out << "\n";
  out << "  count_tests         print number of generated test objects (to stdout)\n";
  out << "  select_test <n>     select generated test object as in-memory object\n";
  out << "  is_deterministic    exit w/ success if type encodes deterministically\n";
}
  
int main(int argc, const char **argv)
{
  register_common_dencoders();
  register_mds_dencoders();
  register_osd_dencoders();
  register_rbd_dencoders();
  register_rgw_dencoders();

  auto& dencoders = DencoderRegistry::instance().get();

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  Dencoder *den = NULL;
  uint64_t features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  bufferlist encbl;
  uint64_t skip = 0;

  if (args.empty()) {
    cerr << "-h for help" << std::endl;
    exit(1);
  }
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ++i) {
    string err;

    if (*i == string("help") || *i == string("-h") || *i == string("--help")) {
      usage(cout);
      exit(0);
    } else if (*i == string("version")) {
      cout << CEPH_GIT_NICE_VER << std::endl;
    } else if (*i == string("list_types")) {
      for (auto& dencoder : dencoders)
	cout << dencoder.first << std::endl;
      exit(0);
    } else if (*i == string("type")) {
      ++i;
      if (i == args.end()) {
	cerr << "expecting type" << std::endl;
	exit(1);
      }
      string cname = *i;
      if (!dencoders.count(cname)) {
	cerr << "class '" << cname << "' unknown" << std::endl;
	exit(1);
      }
      den = dencoders[cname].get();
      den->generate();
    } else if (*i == string("skip")) {
      ++i;
      if (i == args.end()) {
	cerr << "expecting byte count" << std::endl;
	exit(1);
      }
      skip = atoi(*i);
    } else if (*i == string("get_features")) {
      cout << CEPH_FEATURES_SUPPORTED_DEFAULT << std::endl;
      exit(0);
    } else if (*i == string("set_features")) {
      ++i;
      if (i == args.end()) {
	cerr << "expecting features" << std::endl;
	exit(1);
      }
      features = atoll(*i);
    } else if (*i == string("encode")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	exit(1);
      }
      den->encode(encbl, features | CEPH_FEATURE_RESERVED); // hack for OSDMap
    } else if (*i == string("decode")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	exit(1);
      }
      err = den->decode(encbl, skip);
    } else if (*i == string("copy_ctor")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	exit(1);
      }
      den->copy_ctor();
    } else if (*i == string("copy")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	exit(1);
      }
      den->copy();
    } else if (*i == string("dump_json")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	exit(1);
      }
      JSONFormatter jf(true);
      jf.open_object_section("object");
      den->dump(&jf);
      jf.close_section();
      jf.flush(cout);
      cout << std::endl;

    } else if (*i == string("hexdump")) {
      encbl.hexdump(cout);
    } else if (*i == string("get_struct_v")) {
      std::cout << den->get_struct_v(encbl, 0) << std::endl;
    } else if (*i == string("get_struct_compat")) {
      std::cout << den->get_struct_v(encbl, sizeof(uint8_t)) << std::endl;
    } else if (*i == string("import")) {
      ++i;
      if (i == args.end()) {
	cerr << "expecting filename" << std::endl;
	exit(1);
      }
      int r;
      if (*i == string("-")) {
        *i = "stdin";
	// Read up to 1mb if stdin specified
	r = encbl.read_fd(STDIN_FILENO, MB(1));
      } else {
	r = encbl.read_file(*i, &err);
      }
      if (r < 0) {
        cerr << "error reading " << *i << ": " << err << std::endl;
        exit(1);
      }

    } else if (*i == string("export")) {
      ++i;
      if (i == args.end()) {
	cerr << "expecting filename" << std::endl;
	exit(1);
      }
      int fd = ::open(*i, O_WRONLY|O_CREAT|O_TRUNC, 0644);
      if (fd < 0) {
	cerr << "error opening " << *i << " for write: " << cpp_strerror(errno) << std::endl;
	exit(1);
      }
      int r = encbl.write_fd(fd);
      if (r < 0) {
	cerr << "error writing " << *i << ": " << cpp_strerror(errno) << std::endl;
	exit(1);
      }
      ::close(fd);

    } else if (*i == string("count_tests")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	exit(1);
      }
      cout << den->num_generated() << std::endl;
    } else if (*i == string("select_test")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	exit(1);
      }
      ++i;
      if (i == args.end()) {
	cerr << "expecting instance number" << std::endl;
	exit(1);
      }
      int n = atoi(*i);
      err = den->select_generated(n);
    } else if (*i == string("is_deterministic")) {
      if (!den) {
	cerr << "must first select type with 'type <name>'" << std::endl;
	exit(1);
      }
      if (den->is_deterministic())
	exit(0);
      else
	exit(1);
    } else {
      cerr << "unknown option '" << *i << "'" << std::endl;
      exit(1);
    }      
    if (err.length()) {
      cerr << "error: " << err << std::endl;
      exit(1);
    }
  }
  return 0;
}
