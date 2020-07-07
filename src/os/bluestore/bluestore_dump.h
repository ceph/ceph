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
#include "common/safe_io.h"

#include "os/bluestore/BlueFS.h"
#include "os/bluestore/BlueStore.h"
#include "common/admin_socket.h"
#include "common/url_escape.h"

class Dump {
public:
  Dump(KeyValueDB *db)
    :db(db) {};

  int list_objects(const std::string& format,
		   const std::string& selector,
		   const std::string& trimmer);
private:
  bool dump_extent(const std::string& key, const bufferlist& v);
  bool dump_object(const std::string& key, const bufferlist& v);
  bool dump_object(const bufferlist& v);
  unsigned decode_some(const bufferlist& bl);
  std::string blob_flags_to_string(uint8_t flags);
  int decode_spanning_blobs(std::map<int16_t, bluestore_blob_t>& spanning_blobs,
			    bufferptr::const_iterator& p);
  void dump(const bluestore_blob_use_tracker_t& t);
  void dump(bluestore_onode_t& t);
  void dump(const bluestore_blob_t& t);
  KeyValueDB* db;
  Formatter* f = nullptr;
};
