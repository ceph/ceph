// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <algorithm>
#include <boost/algorithm/string/trim.hpp>
#include <boost/process.hpp>
#include <boost/tokenizer.hpp>
#include <boost/uuid/uuid.hpp>             // uuid class
#include <boost/uuid/uuid_generators.hpp>  // generators
#include <boost/uuid/uuid_io.hpp>          // streaming operators etc.
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "lazy_omap_stats_test.h"

using namespace std;
namespace bp = boost::process;

void LazyOmapStatsTest::init(const int argc, const char** argv)
{
  int ret = rados.init("admin");
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to initialise rados! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }

  ret = rados.conf_parse_argv(argc, argv);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to parse command line config options! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }

  rados.conf_parse_env(NULL);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to parse environment! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }

  rados.conf_read_file(NULL);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to read config file! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }

  ret = rados.connect();
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to connect to running cluster! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }

  string command = R"(
    {
      "prefix": "osd pool create",
      "pool": ")" + conf.pool_name +
                   R"(",
      "pool_type": "replicated",
      "size": )" + to_string(conf.replica_count) +
                   R"(
    })";
  librados::bufferlist inbl;
  string output;
  ret = rados.mon_command(command, inbl, nullptr, &output);
  if (output.length()) cout << output << endl;
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to create pool! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }

  ret = rados.ioctx_create(conf.pool_name.c_str(), io_ctx);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to create ioctx! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }
}

void LazyOmapStatsTest::shutdown()
{
  rados.pool_delete(conf.pool_name.c_str());
  rados.shutdown();
}

void LazyOmapStatsTest::write_omap(const string& object_name)
{
  librados::bufferlist bl;
  int ret = io_ctx.write_full(object_name, bl);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to create object! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }
  ret = io_ctx.omap_set(object_name, payload);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to write omap payload! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }
  cout << "Wrote " << conf.keys << " omap keys of " << conf.payload_size
       << " bytes to "
       << "the " << object_name << " object" << endl;
}

const string LazyOmapStatsTest::get_name() const
{
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  return boost::uuids::to_string(uuid);
}

void LazyOmapStatsTest::write_many(uint how_many)
{
  for (uint i = 0; i < how_many; i++) {
    write_omap(get_name());
  }
}

void LazyOmapStatsTest::create_payload()
{
  librados::bufferlist Lorem;
  Lorem.append(
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
      "eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut "
      "enim ad minim veniam, quis nostrud exercitation ullamco laboris "
      "nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in "
      "reprehenderit in voluptate velit esse cillum dolore eu fugiat "
      "nulla pariatur. Excepteur sint occaecat cupidatat non proident, "
      "sunt in culpa qui officia deserunt mollit anim id est laborum.");
  conf.payload_size = Lorem.length();
  conf.total_bytes = conf.keys * conf.payload_size * conf.how_many;
  conf.total_keys = conf.keys * conf.how_many;
  uint i = 0;
  for (i = 1; i < conf.keys + 1; ++i) {
    payload[get_name()] = Lorem;
  }
  cout << "Created payload with " << conf.keys << " keys of "
       << conf.payload_size
       << " bytes each. Total size in bytes = " << conf.keys * conf.payload_size
       << endl;
}

void LazyOmapStatsTest::scrub() const
{
  // Use CLI because we need to block

  cout << "Scrubbing" << endl;
  error_code ec;
  bp::ipstream is;
  bp::system("ceph osd deep-scrub all --block", bp::std_out > is, ec);
  if (ec) {
    cout << "Deep scrub command failed! Error: " << ec.value() << " "
         << ec.message() << endl;
    exit(ec.value());
  }
  cout << is.rdbuf() << endl;
}

const int LazyOmapStatsTest::find_matches(string& output, regex& reg) const
{
  sregex_iterator cur(output.begin(), output.end(), reg);
  uint x = 0;
  for (auto end = std::sregex_iterator(); cur != end; ++cur) {
    cout << (*cur)[1].str() << endl;
    x++;
  }
  return x;
}

const string LazyOmapStatsTest::get_output(const string command,
                                           const bool silent)
{
  librados::bufferlist inbl, outbl;
  string output;
  int ret = rados.mgr_command(command, inbl, &outbl, &output);
  if (output.length() && !silent) {
    cout << output << endl;
  }
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to get " << command << "! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }
  return string(outbl.c_str(), outbl.length());
}

void LazyOmapStatsTest::check_one()
{
  string full_output = get_output();
  cout << full_output << endl;
  regex reg(
      "\n"
      R"((PG_STAT[\s\S]*)"
      "\n)OSD_STAT"); // Strip OSD_STAT table so we don't find matches there
  smatch match;
  regex_search(full_output, match, reg);
  auto truncated_output = match[1].str();
  cout << truncated_output << endl;
  reg = regex(
      "\n"
      R"(([0-9,s].*\s)" +
      to_string(conf.keys) +
      R"(\s.*))"
      "\n");

  cout << "Checking number of keys " << conf.keys << endl;
  cout << "Found the following lines" << endl;
  cout << "*************************" << endl;
  uint result = find_matches(truncated_output, reg);
  cout << "**********************" << endl;
  cout << "Found " << result << " matching line(s)" << endl;
  uint total = result;

  reg = regex(
      "\n"
      R"(([0-9,s].*\s)" +
      to_string(conf.payload_size * conf.keys) +
      R"(\s.*))"
      "\n");
  cout << "Checking number of bytes "
       << conf.payload_size * conf.keys << endl;
  cout << "Found the following lines" << endl;
  cout << "*************************" << endl;
  result = find_matches(truncated_output, reg);
  cout << "**********************" << endl;
  cout << "Found " << result << " matching line(s)" << endl;

  total += result;
  if (total != 6) {
    cout << "Error: Found " << total << " matches, expected 6! Exiting..."
         << endl;
    exit(22);  // EINVAL
  }
  cout << "check_one successful. Found " << total << " matches as expected"
       << endl;
}

const int LazyOmapStatsTest::find_index(string& haystack, regex& needle,
                                        string label) const
{
  smatch match;
  regex_search(haystack, match, needle);
  auto line = match[1].str();
  boost::algorithm::trim(line);
  boost::char_separator<char> sep{" "};
  boost::tokenizer<boost::char_separator<char>> tok(line, sep);
  vector<string> tokens(tok.begin(), tok.end());
  auto it = find(tokens.begin(), tokens.end(), label);
  if (it != tokens.end()) {
    return distance(tokens.begin(), it);
  }

  cerr << "find_index failed to find index for " << label << endl;
  exit(2);    // ENOENT
  return -1;  // Unreachable
}

const uint LazyOmapStatsTest::tally_column(const uint omap_bytes_index,
                                           const string& table,
                                           bool header) const
{
  istringstream buffer(table);
  string line;
  uint64_t total = 0;
  while (std::getline(buffer, line)) {
    if (header) {
      header = false;
      continue;
    }
    boost::char_separator<char> sep{" "};
    boost::tokenizer<boost::char_separator<char>> tok(line, sep);
    vector<string> tokens(tok.begin(), tok.end());
    total += stoi(tokens.at(omap_bytes_index));
  }

  return total;
}

void LazyOmapStatsTest::check_column(const int index, const string& table,
                                     const string& type, bool header) const
{
  uint expected;
  string errormsg;
  if (type.compare("bytes") == 0) {
    expected = conf.total_bytes;
    errormsg = "Error. Got unexpected byte count!";
  } else {
    expected = conf.total_keys;
    errormsg = "Error. Got unexpected key count!";
  }
  uint sum = tally_column(index, table, header);
  cout << "Got: " << sum << " Expected: " << expected << endl;
  if (sum != expected) {
    cout << errormsg << endl;
    exit(22);  // EINVAL
  }
}

index_t LazyOmapStatsTest::get_indexes(regex& reg, string& output) const
{
  index_t indexes;
  indexes.byte_index = find_index(output, reg, "OMAP_BYTES*");
  indexes.key_index = find_index(output, reg, "OMAP_KEYS*");

  return indexes;
}

const string LazyOmapStatsTest::get_pool_id(string& pool)
{
  cout << R"(Querying pool id)" << endl;

  string command = R"({"prefix": "osd pool ls", "detail": "detail"})";
  librados::bufferlist inbl, outbl;
  string output;
  int ret = rados.mon_command(command, inbl, &outbl, &output);
  if (output.length()) cout << output << endl;
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to get pool id! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }
  string dump_output(outbl.c_str(), outbl.length());
  cout << dump_output << endl;

  string poolregstring = R"(pool\s(\d+)\s')" + pool + "'";
  regex reg(poolregstring);
  smatch match;
  regex_search(dump_output, match, reg);
  auto pool_id = match[1].str();
  cout << "Found pool ID: " << pool_id << endl;

  return pool_id;
}

void LazyOmapStatsTest::check_pg_dump()
{
  cout << R"(Checking "pg dump" output)" << endl;

  string dump_output = get_output();
  cout << dump_output << endl;

  regex reg(
      "\n"
      R"((PG_STAT\s.*))"
      "\n");
  index_t indexes = get_indexes(reg, dump_output);

  reg =
      "\n"
      R"((PG_STAT[\s\S]*))"
      "\n +\n[0-9]";
  smatch match;
  regex_search(dump_output, match, reg);
  auto table = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, table, string("bytes"));

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, table, string("keys"));

  cout << endl;
}

void LazyOmapStatsTest::check_pg_dump_summary()
{
  cout << R"(Checking "pg dump summary" output)" << endl;

  string command = R"({"prefix": "pg dump", "dumpcontents": ["summary"]})";
  string dump_output = get_output(command);
  cout << dump_output << endl;

  regex reg(
      "\n"
      R"((PG_STAT\s.*))"
      "\n");
  index_t indexes = get_indexes(reg, dump_output);

  reg =
      "\n"
      R"((sum\s.*))"
      "\n";
  smatch match;
  regex_search(dump_output, match, reg);
  auto table = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, table, string("bytes"), false);

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, table, string("keys"), false);
  cout << endl;
}

void LazyOmapStatsTest::check_pg_dump_pgs()
{
  cout << R"(Checking "pg dump pgs" output)" << endl;

  string command = R"({"prefix": "pg dump", "dumpcontents": ["pgs"]})";
  string dump_output = get_output(command);
  cout << dump_output << endl;

  regex reg(R"(^(PG_STAT\s.*))"
            "\n");
  index_t indexes = get_indexes(reg, dump_output);

  reg = R"(^(PG_STAT[\s\S]*))"
        "\n\n";
  smatch match;
  regex_search(dump_output, match, reg);
  auto table = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, table, string("bytes"));

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, table, string("keys"));
  cout << endl;
}

void LazyOmapStatsTest::check_pg_dump_pools()
{
  cout << R"(Checking "pg dump pools" output)" << endl;

  string command = R"({"prefix": "pg dump", "dumpcontents": ["pools"]})";
  string dump_output = get_output(command);
  cout << dump_output << endl;

  regex reg(R"(^(POOLID\s.*))"
            "\n");
  index_t indexes = get_indexes(reg, dump_output);

  auto pool_id = get_pool_id(conf.pool_name);

  reg =
      "\n"
      R"(()" +
      pool_id +
      R"(\s.*))"
      "\n";
  smatch match;
  regex_search(dump_output, match, reg);
  auto line = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, line, string("bytes"), false);

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, line, string("keys"), false);
  cout << endl;
}

void LazyOmapStatsTest::check_pg_ls()
{
  cout << R"(Checking "pg ls" output)" << endl;

  string command = R"({"prefix": "pg ls"})";
  string dump_output = get_output(command);
  cout << dump_output << endl;

  regex reg(R"(^(PG\s.*))"
            "\n");
  index_t indexes = get_indexes(reg, dump_output);

  reg = R"(^(PG[\s\S]*))"
        "\n\n";
  smatch match;
  regex_search(dump_output, match, reg);
  auto table = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, table, string("bytes"));

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, table, string("keys"));
  cout << endl;
}

void LazyOmapStatsTest::wait_for_active_clean()
{
  cout << "Waiting for active+clean" << endl;

  int index = -1;
  regex reg(
      "\n"
      R"((PG_STAT[\s\S]*))"
      "\n +\n[0-9]");
  string command = R"({"prefix": "pg dump"})";
  int num_not_clean;
  do {
    string dump_output = get_output(command, true);
    if (index == -1) {
      regex ireg(
          "\n"
          R"((PG_STAT\s.*))"
          "\n");
      index = find_index(dump_output, ireg, "STATE");
    }
    smatch match;
    regex_search(dump_output, match, reg);
    istringstream buffer(match[1].str());
    string line;
    num_not_clean = 0;
    while (std::getline(buffer, line)) {
      if (line.compare(0, 1, "P") == 0) continue;
      boost::char_separator<char> sep{" "};
      boost::tokenizer<boost::char_separator<char>> tok(line, sep);
      vector<string> tokens(tok.begin(), tok.end());
      num_not_clean += tokens.at(index).compare("active+clean");
    }
    cout << "." << flush;
    this_thread::sleep_for(chrono::milliseconds(250));
  } while (num_not_clean);

  cout << endl;
}

const int LazyOmapStatsTest::run(const int argc, const char** argv)
{
  init(argc, argv);
  create_payload();
  wait_for_active_clean();
  write_omap(get_name());
  scrub();
  check_one();

  write_many(conf.how_many - 1);  // Since we already wrote one
  scrub();
  check_pg_dump();
  check_pg_dump_summary();
  check_pg_dump_pgs();
  check_pg_dump_pools();
  check_pg_ls();
  cout << "All tests passed. Success!" << endl;

  shutdown();

  return 0;
}
